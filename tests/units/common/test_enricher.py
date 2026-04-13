import pytest
from common.enricher import Enricher


@pytest.fixture
def enricher():
    return Enricher()


@pytest.fixture
def arxiv_output_content(datadir):
    return (datadir / "arxiv_output.xml").read_text()


def test_get_schema(enricher):
    assert enricher._get_schema() == "http://repo.qa.scoap3.org/schemas/hep.json"


def test_get_arxiv_categories_arxiv_id(
    enricher, arxiv_output_content, requests_mock, assertListEqual
):
    requests_mock.get(
        "https://oaipmh.arxiv.org/oai?verb=GetRecord&identifier=oai:arXiv.org:0000.00000&metadataPrefix=arXiv",
        text=arxiv_output_content,
    )
    assertListEqual(
        ["hep-ph", "hep-th"],
        enricher._get_arxiv_categories(arxiv_id="0000.00000"),
    )
