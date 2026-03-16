import logging
import os
import re
import xml.etree.ElementTree as ET

import backoff
import requests
from common.cleanup import remove_unnecessary_fields
from common.parsing.generic_parsing import remove_empty_values

logger = logging.getLogger("airflow.task")


class Enricher:
    def _get_schema(self):
        return os.getenv("REPO_URL", "http://repo.qa.scoap3.org/schemas/hep.json")

    def _clean_arxiv(self, arxiv):
        if arxiv is None:
            return None
        try:
            return re.search(r"\d{4}\.\d{4,5}", arxiv).group()
        except AttributeError:
            return None

    def _get_arxiv_categories_from_response_xml(self, xml):
        xml_namespaces = {
            "oai": "http://www.openarchives.org/OAI/2.0/",
            "arxiv": "http://arxiv.org/OAI/arXiv/",
        }

        categories_node = xml.find(
            "./oai:GetRecord/oai:record/oai:metadata/arxiv:arXiv/arxiv:categories",
            namespaces=xml_namespaces,
        )
        if categories_node is None or not categories_node.text:
            return []

        return categories_node.text.strip().split()

    @backoff.on_exception(
        backoff.expo, requests.exceptions.RequestException, max_time=60, max_tries=5
    )
    def _get_arxiv_categories(self, arxiv_id=None):
        if arxiv_id is None:
            raise ValueError("The arxiv_id parameter has to be different than None.")

        arxiv_id = self._clean_arxiv(arxiv_id)
        categories = []

        if arxiv_id:
            response = requests.get(
                f"https://oaipmh.arxiv.org/oai?verb=GetRecord&identifier=oai:arXiv.org:{arxiv_id}&metadataPrefix=arXiv"
            )
        else:
            logger.warning(
                "No arxiv_id provided for article. Skipping arxiv categories enrichment."
            )
            return categories

        if response.status_code == 200:
            xml = ET.fromstring(response.content)
            categories = self._get_arxiv_categories_from_response_xml(xml)
            if not categories:
                logger.warning(
                    "Could not get arxiv categories for arxiv_id: %s",
                    arxiv_id,
                )
        else:
            logger.error(
                "Got arxiv response error %s for arxiv_id: %s",
                response.status_code,
                arxiv_id,
            )
            response.raise_for_status()
        return categories

    def _set_categories(self, eprint):
        if eprint["value"]:
            eprint["categories"] = self._get_arxiv_categories(eprint["value"])
            eprint["value"] = self._clean_arxiv(eprint["value"])
        return eprint

    def __call__(self, article):
        enriched_article = article.copy()
        enriched_article.update(
            {
                "arxiv_eprints": [
                    self._set_categories(eprint)
                    for eprint in enriched_article.get("arxiv_eprints", [])
                ],
            }
        )
        enriched_article = remove_empty_values(enriched_article)
        enriched_article = remove_unnecessary_fields(enriched_article)

        logger.info("Enriched article: %s", enriched_article)
        return enriched_article
