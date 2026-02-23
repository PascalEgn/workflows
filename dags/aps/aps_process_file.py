import json
import logging

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from aps.parser import APSParser, APSXMLParser
from aps.repository import APSRepository
from common.enhancer import Enhancer
from common.enricher import Enricher
from common.exceptions import EmptyOutputFromPreviousTask
from common.scoap3_s3 import Scoap3Repository
from common.utils import create_or_update_article, upload_json_to_s3
from inspire_utils.record import get_value

logger = logging.getLogger("airflow.task")


def parse_aps(data):
    parser = APSParser()
    parsed = parser.parse(data)
    return parsed


def enhance_aps(parsed_file):
    return Enhancer()("APS", parsed_file)


def enrich_aps(enhanced_file):
    return Enricher()(enhanced_file)


def replace_authors_with_xml_authors(
    parsed_json, parsed_xml, fail_if_author_count_not_equal=True
):
    if fail_if_author_count_not_equal and len(parsed_json["authors"]) != len(
        parsed_xml["authors"]
    ):
        raise ValueError("Number of authors in JSON and XML do not match")
    parsed_json["authors"] = parsed_xml["authors"]
    return parsed_json


def add_data_availability(parsed_json, parsed_xml):
    parsed_json["data_availability"] = parsed_xml["data_availability"]
    return parsed_json


@dag(
    schedule=None,
    start_date=pendulum.today("UTC").add(days=-1),
    tags=["process", "aps"],
    params={
        "fail_if_author_count_not_equal": Param(True, type="boolean"),
    },
)
def aps_process_file():
    s3_client = APSRepository()

    @task()
    def parse(**kwargs):
        if "params" in kwargs and "article" in kwargs["params"]:
            article = json.loads(kwargs["params"]["article"])
            return parse_aps(article)

    @task()
    def enhance(parsed_file):
        if not parsed_file:
            raise EmptyOutputFromPreviousTask("parse")
        return enhance_aps(parsed_file)

    @task()
    def enrich(enhanced_file):
        if not enhanced_file:
            raise EmptyOutputFromPreviousTask("enhance")
        return enrich_aps(enhanced_file)

    @task()
    def populate_files(parsed_file):
        if "dois" not in parsed_file:
            return parsed_file

        doi = get_value(parsed_file, "dois.value[0]")
        logger.info("Populating files for doi: %s", doi)

        files = {
            "pdf": f"http://harvest.aps.org/v2/journals/articles/{doi}",
            "xml": f"http://harvest.aps.org/v2/journals/articles/{doi}",
        }
        s3_scoap3_client = Scoap3Repository()
        downloaded_files = s3_scoap3_client.download_files_for_aps(files, prefix=doi)
        parsed_file["files"] = downloaded_files
        logger.info("Files populated: %s", parsed_file["files"])
        return parsed_file

    @task()
    def parse_xml(xml_file):
        parser = APSXMLParser()
        parsed = parser.parse(xml_file)
        return parsed

    @task()
    def replace_authors_with_xml_authors(parsed_json, parsed_xml):
        ctx = get_current_context()
        return replace_authors_with_xml_authors(
            parsed_json,
            parsed_xml,
            fail_if_author_count_not_equal=ctx["params"][
                "fail_if_author_count_not_equal"
            ],
        )

    @task()
    def add_data_availability(parsed_json, parsed_xml):
        parsed_json["data_availability"] = parsed_xml["data_availability"]
        return parsed_json

    @task()
    def save_to_s3(complete_file):
        upload_json_to_s3(json_record=complete_file, repo=s3_client)

    @task()
    def create_or_update(complete_file):
        create_or_update_article(complete_file)

    parsed_file = parse()
    enhanced_file = enhance(parsed_file)
    enhanced_file_with_files = populate_files(enhanced_file)
    enriched_file = enrich(enhanced_file_with_files)
    parsed_xml = parse_xml(enriched_file["files"]["xml"])
    replaced_authors_file = replace_authors_with_xml_authors(enriched_file, parsed_xml)
    complete_file = add_data_availability(replaced_authors_file, parsed_xml)
    save_to_s3(complete_file)
    create_or_update(complete_file)


dag_for_aps_files_processing = aps_process_file()
