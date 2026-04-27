import xml.etree.ElementTree as ET
from os import listdir

import pytest
from aps.parser import APSXMLParser


@pytest.fixture(scope="module")
def parser():
    return APSXMLParser()


@pytest.fixture
def parsed_articles(shared_datadir, parser):
    articles = []
    for filename in sorted(listdir(shared_datadir)):
        if filename.endswith("xml"):
            with open(shared_datadir / filename) as file:
                article = ET.fromstring(file.read())
                articles.append(article)
    return [parser._publisher_specific_parsing(article) for article in articles]


@pytest.fixture
def no_data_article(shared_datadir, parser):
    with open(shared_datadir / "aps_record_no_data.xml") as file:
        article = ET.fromstring(file.read())
        return parser._publisher_specific_parsing(article)


@pytest.fixture
def data_article(shared_datadir, parser):
    with open(shared_datadir / "aps_record_data.xml") as file:
        article = ET.fromstring(file.read())
        return parser._publisher_specific_parsing(article)


def test_authors(no_data_article):
    expected_results = {
        "authors": [
            {
                "given_names": "L.",
                "surname": "Nađđerđ",
                "full_name": "L. Nađđerđ",
            },
            {
                "given_names": "J.",
                "surname": "Milošević",
                "full_name": "J. Milošević",
                "orcid": "https://orcid.org/0000-0001-8486-4604",
            },
            {
                "given_names": "D.",
                "surname": "Devetak",
                "full_name": "D. Devetak",
                "orcid": "https://orcid.org/0000-0002-4450-2390",
            },
            {
                "given_names": "F.",
                "surname": "Wang",
                "full_name": "F. Wang",
                "orcid": "https://orcid.org/0000-0002-8313-0809",
            },
            {
                "given_names": "X.",
                "surname": "Zhu",
                "full_name": "X. Zhu",
            },
        ]
    }

    assert no_data_article["authors"] == expected_results["authors"]


def test_data_not_available(no_data_article):
    expected_results = {
        "data_availability": {
            "statement": "No data were created or analyzed in this study.",
            "urls": None,
        }
    }

    assert "data_availability" in no_data_article
    assert no_data_article["data_availability"] == expected_results["data_availability"]


def test_data_available(data_article):
    expected_results = {
        "data_availability": {
            "statement": "The data supporting the findings of this article are openly available .",
            "urls": [
                "10.1103/PhysRevC.88.044910",
                "10.1103/PhysRevC.101.044907",
                "10.1016/j.physletb.2018.12.048",
                "arXiv:2504.02505",
                "10.1103/PhysRevC.83.024913",
                "10.5281/zenodo.15848379",
            ],
        }
    }

    assert "data_availability" in data_article
    assert data_article["data_availability"] == expected_results["data_availability"]


def test_authors_multiple_contrib_groups(parser):
    article = ET.fromstring(
        """
        <article>
            <front>
                <article-meta>
                    <contrib-group>
                        <contrib contrib-type="author">
                            <name>
                                <surname>Kundu</surname>
                                <given-names>Anirban</given-names>
                            </name>
                            <xref ref-type="aff" rid="a1" />
                        </contrib>
                        <aff id="a1">
                            <institution-wrap>
                                <institution>University of Calcutta</institution>
                                <institution-id institution-id-type="ror">https://ror.org/01e7v7w47</institution-id>
                            </institution-wrap>
                        </aff>
                    </contrib-group>
                    <contrib-group>
                        <contrib contrib-type="author">
                            <name>
                                <surname>Mondal</surname>
                                <given-names>Poulami</given-names>
                            </name>
                            <xref ref-type="aff" rid="a2" />
                        </contrib>
                        <aff id="a2">
                            <institution-wrap>
                                <institution>Indian Institute of Technology Kanpur</institution>
                                <institution-id institution-id-type="ror">https://ror.org/05pjsgx75</institution-id>
                            </institution-wrap>
                        </aff>
                    </contrib-group>
                    <contrib-group>
                        <contrib contrib-type="author">
                            <name>
                                <surname>Moultaka</surname>
                                <given-names>Gilbert</given-names>
                            </name>
                            <xref ref-type="aff" rid="a3" />
                        </contrib>
                        <aff id="a3">
                            <institution-wrap>
                                <institution>Laboratoire Univers et Particules de Montpellier (LUPM)</institution>
                                <institution-id institution-id-type="ror">https://ror.org/00nrbzg90</institution-id>
                            </institution-wrap>
                        </aff>
                    </contrib-group>
                </article-meta>
            </front>
        </article>
        """
    )

    assert parser._get_authors(article) == [
        {
            "given_names": "Anirban",
            "surname": "Kundu",
            "full_name": "Anirban Kundu",
        },
        {
            "given_names": "Poulami",
            "surname": "Mondal",
            "full_name": "Poulami Mondal",
        },
        {
            "given_names": "Gilbert",
            "surname": "Moultaka",
            "full_name": "Gilbert Moultaka",
        },
    ]
