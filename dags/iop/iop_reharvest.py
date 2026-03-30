import base64
import fnmatch
import logging
from datetime import date, datetime, timedelta

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task
from common.utils import parse_without_names_spaces
from iop.repository import IOPRepository

logger = logging.getLogger("airflow.task")
IOP_REPO = IOPRepository()


def _parse_date(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _normalize_dois(dois):
    if not dois:
        return set()
    return {doi.strip().lower() for doi in dois if isinstance(doi, str) and doi.strip()}


def _normalize_file_keys(file_keys):
    return [k.strip() for k in file_keys if isinstance(k, str) and k.strip()]


def _is_glob_pattern(value):
    return any(c in value for c in ("*", "?", "["))


def _resolve_file_keys(file_keys, all_xml_keys):
    """Resolve a mix of exact S3 keys and glob patterns.

    Patterns are matched against the path after the leading 'IOP_REPO.EXTRACTED_DIR' prefix,
    so '*' matches every key.
    """
    all_xml_keys_set = set(all_xml_keys)
    prefix = IOP_REPO.EXTRACTED_DIR

    exact_keys = [k for k in file_keys if not _is_glob_pattern(k)]
    glob_patterns = [k for k in file_keys if _is_glob_pattern(k)]

    missing = [k for k in exact_keys if k not in all_xml_keys_set]
    if missing:
        raise ValueError(f"Some requested file_keys do not exist: {missing}")

    resolved = list(exact_keys)

    for pattern in glob_patterns:
        matched = [
            key
            for key in all_xml_keys
            if fnmatch.fnmatch(
                key[len(prefix) :] if key.startswith(prefix) else key, pattern
            )
        ]
        if not matched:
            logger.warning("Pattern %r matched no extracted XML keys", pattern)
        resolved.extend(matched)

    seen = set()
    deduped = []
    for k in resolved:
        if k not in seen:
            seen.add(k)
            deduped.append(k)
    return deduped


def _extract_doi_from_xml(xml_bytes):
    try:
        if isinstance(xml_bytes, bytes):
            xml_text = xml_bytes.decode("utf-8")
        else:
            xml_text = xml_bytes
        xml = parse_without_names_spaces(xml_text)
        doi_element = xml.find("front/article-meta/article-id/[@pub-id-type='doi']")
        if doi_element is not None and doi_element.text:
            return doi_element.text.strip().lower()
    except Exception:
        pass
    return None


def _extract_open_access_pub_date_from_xml(xml_bytes):
    try:
        if isinstance(xml_bytes, bytes):
            xml_text = xml_bytes.decode("utf-8")
        else:
            xml_text = xml_bytes

        xml = parse_without_names_spaces(xml_text)
        date_element = xml.find("front/article-meta/pub-date/[@pub-type='open-access']")
        if date_element is None:
            return None

        day_text = date_element.findtext("day")
        month_text = date_element.findtext("month")
        year_text = date_element.findtext("year")

        if not (day_text and month_text and year_text):
            return None

        return date(int(year_text), int(month_text), int(day_text))
    except Exception:
        return None


def _enforce_limit(records, limit):
    if limit is None:
        return
    if len(records) > limit:
        raise ValueError(
            f"Matched {len(records)} records, which is above limit={limit}. "
            "Please reduce the date range or increase the limit."
        )


@dag(
    start_date=pendulum.today("UTC").add(days=-1),
    schedule=None,
    catchup=False,
    tags=["reharvest", "iop"],
    params={
        "date_from": Param(
            default=None,
            type=["string", "null"],
            description="Start open-access pub-date (from XML) in YYYY-MM-DD format",
            title="Date from",
        ),
        "date_to": Param(
            default=None,
            type=["string", "null"],
            description="End open-access pub-date (from XML) in YYYY-MM-DD format",
            title="Date to",
        ),
        "file_keys": Param(
            default=[],
            type=["array", "null"],
            description=(
                "Exact S3 extracted XML keys or glob patterns (matched relative "
                f"to '{IOP_REPO.EXTRACTED_DIR}'). For example, "
                f"'{IOP_REPO.EXTRACTED_DIR}6_1674-1137_50_4_044108_10__1088_1674-1137_ae2f4f/cpc_50_4_044108.xml' "
                "or '6_1674-1137_50_4_044108_10__1088_1674-1137_ae2f4f/*'"
            ),
            title="File keys",
        ),
        "dois": Param(
            default=[],
            type=["array", "null"],
            description=(
                "List of DOIs to process. Can be used with date_from/date_to or "
                "together with file_keys to filter the resolved key set. The maximum date range is 1 Year."
            ),
            title="DOIs",
        ),
        "limit": Param(
            1000,
            type=["integer"],
            description="Maximum number of records to process",
            title="Limit",
        ),
        "dry_run": Param(
            default=True,
            type=["boolean", "null"],
            description="Whether to perform a dry run. If true, no downstream DAGs will be triggered",
            title="Dry run",
        ),
    },
)
def iop_reharvest():
    @task()
    def collect_file_keys(repo=IOP_REPO, **kwargs):
        params = kwargs.get("params", {})

        date_from = _parse_date(params.get("date_from"))
        date_to = _parse_date(params.get("date_to"))
        file_keys = _normalize_file_keys(params.get("file_keys") or [])
        target_dois = _normalize_dois(params.get("dois") or [])
        limit = params.get("limit")

        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValueError("limit must be a positive integer when provided")

        if bool(file_keys) and (date_from or date_to):
            raise ValueError(
                "Invalid parameters: date_from/date_to cannot be used together with file_keys"
            )

        if (date_from and not date_to) or (date_to and not date_from):
            raise ValueError("Both date_from and date_to must be provided together")

        all_xml_records = []
        for obj in repo.s3.objects.filter(Prefix=repo.EXTRACTED_DIR).all():
            if not obj.key.endswith(".xml"):
                continue
            all_xml_records.append({"key": obj.key})

        all_xml_keys = [record["key"] for record in all_xml_records]
        record_by_key = {record["key"]: record for record in all_xml_records}
        logger.info(
            "Found %s total IOP extracted XML file(s) in storage", len(all_xml_keys)
        )

        if file_keys:
            resolved_keys = _resolve_file_keys(file_keys, all_xml_keys)
            selected_records = [record_by_key[key] for key in resolved_keys]
        else:
            if target_dois and not (date_from and date_to):
                date_to = date.today()
                date_from = date_to - timedelta(days=365)

            if not (date_from and date_to):
                raise ValueError(
                    "Invalid parameters: provide either date_from+date_to, file_keys, or dois"
                )

            if target_dois and (date_to - date_from).days > 366:
                raise ValueError(
                    "For DOI search the date range must not exceed one year. "
                    "Please use a smaller range."
                )

            logger.info(
                "Selecting IOP XML files by XML open-access pub-date range %s to %s",
                date_from,
                date_to,
            )
            selected_records = []
            for record in all_xml_records:
                file_obj = repo.get_by_id(record["key"])
                record_date = _extract_open_access_pub_date_from_xml(
                    file_obj.getvalue()
                )
                if record_date is None:
                    continue
                if date_from <= record_date <= date_to:
                    selected_records.append(record)

        selected_records = sorted(
            selected_records, key=lambda r: r["key"], reverse=True
        )

        if not target_dois:
            selected_keys = [record["key"] for record in selected_records]
            _enforce_limit(selected_keys, limit)
            logger.info("Selected %s IOP XML key(s)", len(selected_keys))
            return selected_keys

        found_dois = set()
        deduped_by_doi = {}

        for record in selected_records:
            key = record["key"]
            file_obj = repo.get_by_id(key)
            doi = _extract_doi_from_xml(file_obj.getvalue())

            if not doi:
                logger.warning("Could not extract DOI from file: %s", key)
                continue

            if doi not in target_dois:
                continue

            found_dois.add(doi)
            if doi not in deduped_by_doi:
                deduped_by_doi[doi] = key

        missing_dois = sorted(target_dois - found_dois)
        if missing_dois:
            logger.warning("Some requested DOIs were not found: %s", missing_dois)

        selected_keys = list(deduped_by_doi.values())
        _enforce_limit(selected_keys, limit)

        logger.info(
            "Selected %s IOP XML key(s) after DOI filtering", len(selected_keys)
        )
        return selected_keys

    @task()
    def prepare_trigger_conf(file_keys, repo=IOP_REPO, **kwargs):
        dry_run = bool(kwargs.get("params", {}).get("dry_run", False))
        if dry_run:
            logger.info(
                "Dry run enabled. %s record(s) matched. No downstream runs will be triggered.",
                len(file_keys),
            )
            return []

        confs = []
        for key in file_keys:
            file_obj = repo.get_by_id(key)
            encoded_xml = base64.b64encode(file_obj.getvalue()).decode("utf-8")
            confs.append({"file": encoded_xml, "file_name": key})

        logger.info("Prepared %s downstream trigger conf(s)", len(confs))
        return confs

    file_keys = collect_file_keys()
    trigger_confs = prepare_trigger_conf(file_keys)

    TriggerDagRunOperator.partial(
        task_id="iop_reharvest_trigger_file_processing",
        trigger_dag_id="iop_process_file",
        reset_dag_run=True,
    ).expand(conf=trigger_confs)


iop_reharvest_dag = iop_reharvest()
