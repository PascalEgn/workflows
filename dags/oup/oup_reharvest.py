import base64
import fnmatch
import logging
import re
from datetime import date, datetime, timedelta

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task
from common.utils import parse_without_names_spaces
from oup.repository import OUPRepository

logger = logging.getLogger("airflow.task")
OUP_REPO = OUPRepository()
SCOAP_FOLDER = "SCOAP-Article-Submission"


def _parse_date(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _normalize_dois(dois):
    if not dois:
        return set()
    return {doi.strip().lower() for doi in dois if isinstance(doi, str) and doi.strip()}


def _extract_date_from_key(key):
    dt_value = _extract_datetime_from_key(key)
    if dt_value == datetime.min:
        return None
    return dt_value.date()


def _extract_datetime_from_key(key):
    if not key:
        return datetime.min

    # 1) Full timestamp in key: YYYY-MM-DD_HH:MM:SS or YYYY-MM-DDTHH:MM:SS
    ts_match = re.search(r"(\d{4}-\d{2}-\d{2}[T_]\d{2}:\d{2}:\d{2})", key)
    if ts_match:
        timestamp = ts_match.group(1).replace("T", "_")
        try:
            return datetime.strptime(timestamp, "%Y-%m-%d_%H:%M:%S")
        except ValueError:
            pass

    # 2) Date as YYYY-MM-DD anywhere in key
    ymd_match = re.search(r"(\d{4}-\d{2}-\d{2})", key)
    if ymd_match:
        try:
            return datetime.strptime(ymd_match.group(1), "%Y-%m-%d")
        except ValueError:
            pass

    # 3) Date as DD-MM-YYYY anywhere in key
    dmy_match = re.search(r"(\d{2}-\d{2}-\d{4})", key)
    if dmy_match:
        try:
            return datetime.strptime(dmy_match.group(1), "%d-%m-%Y")
        except ValueError:
            pass

    return datetime.min


def _is_oup_extracted_xml_key(key):
    return (
        key.startswith("extracted/")
        and f"/{SCOAP_FOLDER}/" in key
        and key.endswith(".xml")
    )


def _extract_doi_from_xml(xml_bytes):
    """Extract DOI from OUP XML bytes without a full parser pass."""
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


def _is_glob_pattern(value):
    return any(c in value for c in ("*", "?", "["))


def _resolve_file_keys(file_keys, all_xml_keys):
    """Resolve a mix of exact S3 keys and glob patterns.

    Patterns are matched against the path after the leading 'extracted/' prefix,
    so '*' matches every key and 'SCOAP-Article-Submission/*' matches everything
    inside that folder.
    """
    all_xml_keys_set = set(all_xml_keys)
    _PREFIX = "extracted/"

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
                key[len(_PREFIX) :] if key.startswith(_PREFIX) else key, pattern
            )
        ]
        if not matched:
            logger.warning("Pattern %r matched no extracted XML keys", pattern)
        resolved.extend(matched)

    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for k in resolved:
        if k not in seen:
            seen.add(k)
            deduped.append(k)
    return deduped


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
    tags=["reharvest", "oup"],
    params={
        "date_from": Param(
            default=None,
            type=["string", "null"],
            description="Start date in YYYY-MM-DD format",
            title="Date from",
        ),
        "date_to": Param(
            default=None,
            type=["string", "null"],
            description="End date in YYYY-MM-DD format",
            title="Date to",
        ),
        "file_keys": Param(
            default=[],
            type=["array", "null"],
            description=(
                "Exact S3 extracted XML keys or glob patterns (matched relative to 'extracted/'). "
                "Examples: '*' for all, 'SCOAP-Article-Submission/*' for everything under that "
                "folder, or a full key like 'SCOAP-Article-Submission/August/15-08-2025/"
                "ptep_iss_2025_8_part1.xml/ptac108.xml'."
            ),
            title="File keys",
        ),
        "dois": Param(
            default=[],
            type=["array", "null"],
            description="List of DOIs to process",
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
def oup_reharvest():
    @task()
    def collect_records(repo=OUP_REPO, **kwargs):
        params = kwargs.get("params", {})

        date_from = _parse_date(params.get("date_from"))
        date_to = _parse_date(params.get("date_to"))
        file_keys = params.get("file_keys") or []
        dois = _normalize_dois(params.get("dois") or [])
        limit = params.get("limit")

        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValueError("limit must be a positive integer when provided")

        if bool(file_keys) and bool(dois):
            raise ValueError(
                "Invalid parameters: file_keys and dois cannot be used together"
            )

        if bool(file_keys) and (date_from or date_to):
            raise ValueError(
                "Invalid parameters: date_from/date_to cannot be used together with file_keys"
            )

        if (date_from and not date_to) or (date_to and not date_from):
            raise ValueError("Both date_from and date_to must be provided together")

        # List OUP extracted XML files from extracted/
        all_xml_keys = [
            obj.key
            for obj in repo.s3.objects.filter(Prefix=repo.EXTRACTED_DIR).all()
            if _is_oup_extracted_xml_key(obj.key)
        ]
        logger.info(
            "Found %s OUP extracted XML file(s) in %s",
            len(all_xml_keys),
            SCOAP_FOLDER,
        )

        if file_keys:
            resolved_keys = _resolve_file_keys(file_keys, all_xml_keys)
            _enforce_limit(resolved_keys, limit)
            logger.info(
                "Selected %s OUP XML key(s) from file_keys (with glob expansion)",
                len(resolved_keys),
            )
            return resolved_keys

        target_dois = set(dois)

        # Apply default date range for DOI search when none given
        if dois and not (date_from and date_to):
            date_to = date.today()
            date_from = date_to - timedelta(days=365)

        if not (date_from and date_to):
            raise ValueError(
                "Invalid parameters: provide either date_from+date_to, file_keys, or dois"
            )

        if dois and (date_to - date_from).days > 366:
            raise ValueError(
                "For DOI search the date range must not exceed one year. "
                "Please use a smaller range."
            )

        logger.info(
            "Selecting OUP extracted XML files in date range %s to %s",
            date_from,
            date_to,
        )

        keys_in_range = []
        for key in all_xml_keys:
            key_date = _extract_date_from_key(key)
            if key_date is None:
                continue
            if date_from <= key_date <= date_to:
                keys_in_range.append(key)

        keys_in_range.sort(key=_extract_datetime_from_key, reverse=True)

        logger.info(
            "Found %s OUP extracted XML file(s) in the requested date range",
            len(keys_in_range),
        )

        deduped = {}  # doi -> key (first seen = newest due to desc sort)
        keys_without_doi = []
        found_dois = set()

        for key in keys_in_range:
            file_obj = repo.get_by_id(key)
            doi = _extract_doi_from_xml(file_obj.getvalue())

            if not doi:
                logger.warning("Could not extract DOI from file: %s", key)
                if not target_dois:
                    keys_without_doi.append(key)
                continue

            if target_dois and doi not in target_dois:
                continue

            if target_dois:
                found_dois.add(doi)

            if doi not in deduped:
                deduped[doi] = key

        if target_dois:
            missing_dois = sorted(target_dois - found_dois)
            if missing_dois:
                logger.warning("Some requested DOIs were not found: %s", missing_dois)

        selected_keys = list(deduped.values()) + keys_without_doi

        _enforce_limit(selected_keys, limit)
        logger.info("Collected %s deduplicated OUP XML key(s)", len(selected_keys))
        logger.info("Selected OUP XML keys: %s", selected_keys)
        return selected_keys

    @task()
    def prepare_trigger_conf(file_keys, repo=OUP_REPO, **kwargs):
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
            xml_bytes = file_obj.getvalue()
            encoded_xml = base64.b64encode(xml_bytes).decode("utf-8")
            confs.append({"file": encoded_xml, "file_name": key})

        logger.info("Prepared %s downstream trigger conf(s)", len(confs))
        return confs

    file_keys = collect_records()
    trigger_confs = prepare_trigger_conf(file_keys)

    TriggerDagRunOperator.partial(
        task_id="oup_reharvest_trigger_file_processing",
        trigger_dag_id="oup_process_file",
        reset_dag_run=True,
    ).expand(conf=trigger_confs)


oup_reharvest_dag = oup_reharvest()
