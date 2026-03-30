import fnmatch
import logging
import re
from datetime import date, datetime, timedelta

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task
from common.notification_service import FailedDagNotifier
from hindawi.repository import HindawiRepository
from hindawi.utils import split_xmls

logger = logging.getLogger("airflow.task")
HINDAWI_REPO = HindawiRepository()


def _parse_date(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _normalize_dois(dois):
    if not dois:
        return set()
    return {doi.strip().lower() for doi in dois if isinstance(doi, str) and doi.strip()}


def _extract_key_date(s3_key):
    key_date = s3_key.split("/")[0]
    return _parse_date(key_date)


def _extract_key_datetime(s3_key):
    try:
        key_ts = s3_key.split("/")[1].removesuffix(".xml")
        return datetime.strptime(key_ts, "%Y-%m-%dT%H:%M")
    except (IndexError, ValueError):
        return datetime.min


def _extract_doi_from_record(record_xml):
    match = re.search(r"10\.1155/[^\s<]+", record_xml, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(0).strip().lower()


def _enforce_limit(records, limit):
    if limit is None:
        return
    if len(records) > limit:
        raise ValueError(
            f"Matched {len(records)} records, which is above limit={limit}. "
            "Please reduce the date range or increase the limit."
        )


def _is_glob_pattern(value):
    return any(c in value for c in ("*", "?", "["))


def _resolve_file_keys(file_keys, all_snapshot_keys):
    all_snapshot_keys_set = set(all_snapshot_keys)

    exact_keys = [k for k in file_keys if not _is_glob_pattern(k)]
    glob_patterns = [k for k in file_keys if _is_glob_pattern(k)]

    missing_keys = [k for k in exact_keys if k not in all_snapshot_keys_set]
    if missing_keys:
        raise ValueError(f"Some requested file_keys do not exist: {missing_keys}")

    resolved = list(exact_keys)

    for pattern in glob_patterns:
        matched = [key for key in all_snapshot_keys if fnmatch.fnmatch(key, pattern)]
        if not matched:
            logger.warning("Pattern %r matched no Hindawi snapshot keys", pattern)
        resolved.extend(matched)

    seen = set()
    deduped = []
    for key in resolved:
        if key not in seen:
            seen.add(key)
            deduped.append(key)
    return deduped


@dag(
    on_failure_callback=FailedDagNotifier(),
    start_date=pendulum.today("UTC").add(days=-1),
    schedule=None,
    catchup=False,
    tags=["reharvest", "hindawi"],
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
                "List of Hindawi snapshot keys to process. Supports glob patterns "
                "(e.g. '*' or '2022-09-30/*')."
            ),
            title="File keys",
        ),
        "dois": Param(
            default=[],
            type=["array", "null"],
            description="List of DOIs to process. This will only search for records from the last 3 years, if date_from/date_to are not provided.",
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
def hindawi_reharvest():
    @task()
    def collect_records(repo=HINDAWI_REPO, **kwargs):
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

        all_snapshot_keys = [
            obj.key
            for obj in repo.s3_bucket.objects.all()
            if obj.key.endswith(".xml")
            and "/" in obj.key
            and not obj.key.startswith("parsed/")
        ]
        logger.info(
            "Found %s total Hindawi snapshot file(s) in storage",
            len(all_snapshot_keys),
        )

        selected_keys = []
        target_dois = set(dois)

        if file_keys:
            logger.info(
                "Selecting Hindawi snapshot files from file_keys (with glob expansion)"
            )
            selected_keys = _resolve_file_keys(file_keys, all_snapshot_keys)

        elif dois:
            if not date_from and not date_to:
                date_to = date.today()
                date_from = date_to - timedelta(days=365 * 3)

            logger.info(
                "Selecting Hindawi snapshot files for DOI search in date range %s to %s",
                date_from,
                date_to,
            )
            keys_in_range = []
            for key in all_snapshot_keys:
                key_date = _extract_key_date(key)
                if not key_date:
                    continue
                if date_from <= key_date <= date_to:
                    keys_in_range.append(key)

            selected_keys = sorted(
                keys_in_range, key=_extract_key_datetime, reverse=True
            )

        elif date_from and date_to:
            logger.info(
                "Selecting Hindawi snapshot files in date range %s to %s",
                date_from,
                date_to,
            )
            for key in all_snapshot_keys:
                key_date = _extract_key_date(key)
                if not key_date:
                    continue
                if date_from <= key_date <= date_to:
                    selected_keys.append(key)

        else:
            raise ValueError(
                "Invalid parameters: provide either date_from+date_to, file_keys, or dois"
            )

        logger.info("Selected %s Hindawi snapshot key(s)", len(selected_keys))
        logger.info("Selected Hindawi snapshot keys: %s", selected_keys)

        deduped_records = {}
        found_dois = set()

        for key in selected_keys:
            snapshot_dt = _extract_key_datetime(key)
            for record in split_xmls(repo, key):
                identifier = _extract_doi_from_record(record)
                if not identifier:
                    logger.warning(
                        "Skipping Hindawi record with missing DOI in file %s", key
                    )
                    continue

                if target_dois and identifier not in target_dois:
                    continue

                if target_dois:
                    found_dois.add(identifier)

                existing = deduped_records.get(identifier)
                if not existing or snapshot_dt > existing["snapshot_dt"]:
                    deduped_records[identifier] = {
                        "record": record,
                        "snapshot_dt": snapshot_dt,
                    }

        records = [entry["record"] for entry in deduped_records.values()]

        if target_dois:
            missing_dois = sorted(target_dois - found_dois)
            if missing_dois:
                logger.warning("Some requested DOIs were not found: %s", missing_dois)

        _enforce_limit(records, limit)

        logger.info("Collected %s deduplicated Hindawi record(s)", len(records))
        return records

    @task()
    def prepare_trigger_conf(records, **kwargs):
        dry_run = bool(kwargs.get("params", {}).get("dry_run", False))
        if dry_run:
            logger.info(
                "Dry run enabled. %s record(s) matched. No downstream runs will be triggered.",
                len(records),
            )
            return []

        confs = [{"record": record} for record in records]
        logger.info("Prepared %s downstream trigger conf(s)", len(confs))
        return confs

    records = collect_records()
    trigger_confs = prepare_trigger_conf(records)

    TriggerDagRunOperator.partial(
        task_id="hindawi_reharvest_trigger_file_processing",
        trigger_dag_id="hindawi_file_processing",
        reset_dag_run=True,
    ).expand(conf=trigger_confs)


hindawi_reharvest_dag = hindawi_reharvest()
