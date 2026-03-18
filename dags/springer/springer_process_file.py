import base64
import importlib
import logging
import os
import xml.etree.ElementTree as ET
from io import BytesIO

import pendulum
import requests
from airflow.sdk import Param, dag, get_current_context, task
from common.cleanup import (
    clean_inline_expressions,
    clean_whitespace_characters,
    convert_html_italics_to_latex,
    convert_html_subscripts_to_latex,
    replace_cdata_format,
)
from common.enhancer import Enhancer
from common.enricher import Enricher
from common.exceptions import EmptyOutputFromPreviousTask
from common.scoap3_s3 import Scoap3Repository
from common.utils import (
    create_or_update_article,
    remove_xml_namespaces,
    upload_json_to_s3,
)
from inspire_utils.record import get_value
from jsonschema import validate
from springer.parser import SpringerParser, SpringerPDFParser
from springer.repository import SpringerRepository

logger = logging.getLogger("airflow.task")


def process_xml(input):
    input = remove_xml_namespaces(input)
    input = convert_html_subscripts_to_latex(input)
    input = convert_html_italics_to_latex(input)
    input = replace_cdata_format(input)
    input = clean_inline_expressions(input)
    input = input.replace("\n", "").replace("\r", "").lstrip().rstrip()
    input = clean_whitespace_characters(input.strip())
    return input


def extract_text_from_pdf(pdf_stream: BytesIO, filename: str) -> str:
    document_stream_module = importlib.import_module("docling.datamodel.base_models")
    document_converter_module = importlib.import_module("docling.document_converter")
    conversion_error_module = importlib.import_module("docling.exceptions")

    document_stream_class = document_stream_module.DocumentStream
    document_converter_class = document_converter_module.DocumentConverter
    conversion_error_class = conversion_error_module.ConversionError

    try:
        converter = document_converter_class()
        doc_stream = document_stream_class(
            stream=pdf_stream, name=filename, mime_type="application/pdf"
        )
        doc = converter.convert(doc_stream).document
        return doc.export_to_markdown(labels={"text"})
    except conversion_error_class as e:
        logger.error("There was an issue exporting the PDF %s", e)
        return None


def springer_parse_file(**kwargs):
    if "params" in kwargs and "file" in kwargs["params"]:
        encoded_xml = kwargs["params"]["file"]
        file_name = kwargs["params"]["file_name"]
        xml_bytes = base64.b64decode(encoded_xml)
        if isinstance(xml_bytes, bytes):
            xml_bytes = xml_bytes.decode("utf-8")
        xml_bytes = process_xml(xml_bytes)
        xml = ET.fromstring(xml_bytes)

        parser = SpringerParser(file_name)
        parsed = parser.parse(xml)

        return parsed
    raise Exception("There was no 'file' parameter. Exiting run.")


def springer_parse_pdf(text):
    parser = SpringerPDFParser()
    return parser.parse(text)


def springer_enhance_file(parsed_file):
    return Enhancer()("Springer", parsed_file)


def springer_enrich_file(enhanced_file):
    return Enricher()(enhanced_file)


def springer_validate_record(enriched_file):
    schema = requests.get(enriched_file["$schema"]).json()
    validate(enriched_file, schema)
    return enriched_file


@dag(
    schedule=None,
    tags=["process", "springer"],
    start_date=pendulum.today("UTC").add(days=-1),
    params={
        "parse_pdf": Param(
            False,
            type="boolean",
            description="Enable PDF parsing to populate data availability fields.",
        ),
    },
)
def springer_process_file():
    s3_client = SpringerRepository()

    @task()
    def parse_file(**kwargs):
        return springer_parse_file(**kwargs)

    @task()
    def add_data_availability(parsed_file):
        ctx = get_current_context()
        if not ctx["params"].get("parse_pdf", False):
            logger.warning(
                "PDF parsing disabled. Skipping data availability extraction"
            )
            return parsed_file

        pdfa_path = parsed_file.get("files", {}).get("pdfa")
        if not pdfa_path:
            logger.info("No pdfa path found. Skipping pdf parsing")
            return parsed_file

        logger.info("Extracting text from PDF: %s", pdfa_path)
        pdfa_stream = s3_client.get_by_id(pdfa_path)
        filename = os.path.basename(pdfa_path)
        text = extract_text_from_pdf(pdfa_stream, filename)
        if not text:
            logger.warning("No text extracted from PDF: %s", pdfa_path)
            return parsed_file

        parsed_pdf = springer_parse_pdf(text)
        parsed_file["data_availability"] = {
            "statement": parsed_pdf.get("data_availability", {}).get("statement", "")
            + "\n"
            + parsed_pdf.get("code_availability", {}).get("statement", ""),
            "urls": parsed_pdf.get("data_availability", {}).get("urls", [])
            + parsed_pdf.get("code_availability", {}).get("urls", []),
        }
        return parsed_file

    @task()
    def enhance_file(parsed_file):
        if not parsed_file:
            raise EmptyOutputFromPreviousTask("parse_file")
        return springer_enhance_file(parsed_file)

    @task()
    def enrich_file(enhanced_file):
        if not enhanced_file:
            raise EmptyOutputFromPreviousTask("enhance_file")
        return springer_enrich_file(enhanced_file)

    @task()
    def populate_files(parsed_file):
        if "files" not in parsed_file:
            logger.info("No files to populate")
            return parsed_file

        logger.info("Populating files: %s", parsed_file["files"])

        s3_client_bucket = SpringerRepository().bucket
        s3_scoap3_client = Scoap3Repository()
        doi = get_value(parsed_file, "dois.value[0]")
        files = s3_scoap3_client.copy_files(
            s3_client_bucket, parsed_file["files"], prefix=doi
        )
        parsed_file["files"] = files
        logger.info("Files populated: %s", parsed_file["files"])
        return parsed_file

    @task()
    def create_or_update(enriched_file):
        create_or_update_article(enriched_file)

    @task()
    def save_to_s3(enriched_file):
        upload_json_to_s3(json_record=enriched_file, repo=s3_client)

    parsed_file = parse_file()
    pdf_merged_file = add_data_availability(parsed_file)
    enhanced_file = enhance_file(pdf_merged_file)
    enhanced_file_with_files = populate_files(enhanced_file)
    enriched_file = enrich_file(enhanced_file_with_files)
    save_to_s3(enriched_file=enriched_file)
    create_or_update(enriched_file)


dag_taskflow = springer_process_file()
