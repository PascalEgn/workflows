import fnmatch
import json
import logging
from datetime import date, datetime, timedelta

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task
from aps.repository import APSRepository
from common.notification_service import FailedDagNotifier

logger = logging.getLogger("airflow.task")
APS_REPO = APSRepository()


def _parse_date(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_dt(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")


def _normalize_dois(dois):
    if not dois:
        return set()
    return {doi.strip().lower() for doi in dois if isinstance(doi, str) and doi.strip()}


def _extract_key_date(s3_key):
    key_date = s3_key.split("/")[0]
    return _parse_date(key_date)


def _extract_key_datetime(s3_key):
    try:
        key_ts = s3_key.split("/")[1].removesuffix(".json")
        return datetime.strptime(key_ts, "%Y-%m-%dT%H:%M")
    except (IndexError, ValueError):
        return datetime.min


def _get_identifier(article):
    return (
        ((article.get("identifiers") or {}).get("doi") or article.get("id") or "")
        .strip()
        .lower()
    )


def _get_article_recency(article):
    return _parse_dt(article.get("last_modified_at")) or _parse_dt(
        article.get("metadata_last_modified_at")
    )


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
    """Resolve a mix of exact S3 keys and glob patterns for APS snapshot keys."""
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
            logger.warning("Pattern %r matched no APS snapshot keys", pattern)
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
    tags=["reharvest", "aps"],
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
                "List of APS snapshot keys to process. Supports glob patterns "
                "(e.g. '*' or '2026-03-11/*')."
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
def aps_reharvest():
    @task()
    def collect_articles(repo=APS_REPO, **kwargs):
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
            if obj.key.endswith(".json")
            and "/" in obj.key
            and not obj.key.startswith("parsed/")
        ]
        logger.info(
            "Found %s total APS snapshot file(s) in storage", len(all_snapshot_keys)
        )
        selected_keys = []
        target_dois = set(dois)

        if file_keys:
            logger.info(
                "Selecting APS snapshot files from file_keys (with glob expansion)"
            )
            selected_keys = _resolve_file_keys(file_keys, all_snapshot_keys)

        elif dois:
            if not date_from and not date_to:
                date_to = date.today()
                date_from = date_to - timedelta(days=365 * 3)

            logger.info(
                "Selecting APS snapshot files for DOI search in date range %s to %s",
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
                "Selecting APS snapshot files in date range %s to %s",
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

        logger.info("Selected %s APS snapshot key(s)", len(selected_keys))
        logger.info("Selected APS snapshot keys: %s", selected_keys)

        deduped_records = {}
        found_dois = set()

        for key in selected_keys:
            file_obj = repo.get_by_id(key)
            payload = json.loads(file_obj.getvalue().decode("utf-8"))

            for article in payload.get("data", []):
                identifier = _get_identifier(article)
                if not identifier:
                    logger.warning(
                        "Skipping article with missing identifier in file %s: %s",
                        key,
                        article,
                    )
                    continue

                if target_dois and identifier not in target_dois:
                    continue

                if target_dois:
                    found_dois.add(identifier)

                existing = deduped_records.get(identifier)
                if not existing:
                    deduped_records[identifier] = article
                    continue

                existing_recency = _get_article_recency(existing)
                candidate_recency = _get_article_recency(article)
                if candidate_recency and (
                    not existing_recency or candidate_recency > existing_recency
                ):
                    deduped_records[identifier] = article

        records = list(deduped_records.values())

        if target_dois:
            missing_dois = sorted(target_dois - found_dois)
            if missing_dois:
                logger.warning("Some requested DOIs were not found: %s", missing_dois)

        _enforce_limit(records, limit)

        logger.info("Collected %s deduplicated APS record(s)", len(records))
        return records

    @task()
    def prepare_trigger_conf(articles, **kwargs):
        dry_run = bool(kwargs.get("params", {}).get("dry_run", False))
        if dry_run:
            logger.info(
                "Dry run enabled. %s record(s) matched. No downstream runs will be triggered.",
                len(articles),
            )
            return []

        confs = [{"article": json.dumps(article)} for article in articles]
        logger.info("Prepared %s downstream trigger conf(s)", len(confs))
        return confs

    articles = collect_articles()
    trigger_confs = prepare_trigger_conf(articles)

    TriggerDagRunOperator.partial(
        task_id="aps_reharvest_trigger_file_processing",
        trigger_dag_id="aps_process_file",
        reset_dag_run=True,
    ).expand(conf=trigger_confs)


aps_reharvest_dag = aps_reharvest()
