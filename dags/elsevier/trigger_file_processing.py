import logging
import os

from common.utils import parse_without_names_spaces
from elsevier.metadata_parser import ElsevierMetadataParser

logger = logging.getLogger("airflow.task")


def trigger_file_processing_elsevier(
    publisher,
    repo,
    logger,
    filenames=None,
):
    files = []
    for filename in filenames:
        if os.path.basename(filename) != "dataset.xml":
            continue
        logger.info("Running processing. File: %s", filename)
        file_bytes = repo.get_by_id(filename)

        dataset_file = parse_without_names_spaces(file_bytes)
        parser = ElsevierMetadataParser(file_path=filename)
        parsed_articles = parser.parse(dataset_file)
        for parsed_article in parsed_articles:
            full_file_path = parsed_article["files"]["xml"]
            logger.info("Processing file. File: %s", full_file_path)
            conf = {
                "file_name": full_file_path,
                "metadata": parsed_article,
            }
            files.append(conf)
    return files
    return files
