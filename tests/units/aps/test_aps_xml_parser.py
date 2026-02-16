import xml.etree.ElementTree as ET
from os import listdir

import pytest
from aps.parser import APSXMLParser


@pytest.fixture(scope="module")
def parser():
    return APSXMLParser()


@pytest.fixture
def articles(shared_datadir):
    articles = []
    for filename in sorted(listdir(shared_datadir)):
        if filename.endswith("xml"):
            with open(shared_datadir / filename) as file:
                article = ET.fromstring(file.read())
                articles.append(article)

    return articles


@pytest.fixture
def parsed_articles(parser, articles):
    return [parser._publisher_specific_parsing(article) for article in articles]


def test_authors(parsed_articles):
    expected_results = {
        "authors": [
            {
                "given_names": "P.",
                "surname": "Agnes",
                "full_name": "P. Agnes",
                "affiliations": [
                    {
                        "value": "Department of Physics, Royal Holloway University of London, Egham TW20 0EX, United Kingdom",
                        "ror": None,
                    }
                ],
            },
            {
                "given_names": "M.",
                "surname": "Kimura",
                "full_name": "M. Kimura",
                "orcid": "https://orcid.org/0000-0002-7015-633X",
                "affiliations": [
                    {
                        "value": "AstroCeNT, 00-614 Warsaw, Poland Nicolaus Copernicus Astronomical Center of the Polish Academy of Sciences",
                        "ror": "https://ror.org/040r57n67",
                    }
                ],
            },
        ]
    }

    assert len(parsed_articles) >= 1
    for parsed_article in parsed_articles:
        assert parsed_article == expected_results
