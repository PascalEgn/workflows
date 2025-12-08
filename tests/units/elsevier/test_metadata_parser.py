import pytest
from common.utils import parse_without_names_spaces
from elsevier.metadata_parser import ElsevierMetadataParser
from freezegun import freeze_time


@pytest.fixture(scope="module")
def parser():
    return ElsevierMetadataParser(
        file_path="extracted/CERNQ000000010669A/CERNQ000000010669",
    )


@pytest.fixture
def article(shared_datadir):
    with open(shared_datadir / "CERNQ000000010011" / "dataset.xml") as file:
        return parse_without_names_spaces(file.read())


@pytest.fixture
@freeze_time("2023-11-02")
def parsed_articles(parser, article):
    return [article for article in parser.parse(article)]


@pytest.mark.parametrize(
    ("expected", "key"),
    [
        pytest.param(
            [
                [{"value": "10.1016/j.nuclphysb.2023.116106"}],
                [{"value": "10.1016/j.nuclphysb.2023.116107"}],
                [{"value": "10.1016/j.physletb.2023.137730"}],
                [{"value": "10.1016/j.physletb.2023.137751"}],
            ],
            "dois",
            id="test_get_dois",
        ),
        pytest.param(
            [
                [
                    {
                        "journal_title": "Nuclear Physics B",
                        "year": 2023,
                        "artid": "116106",
                        "journal_volume": "None None",
                    }
                ],
                [
                    {
                        "journal_title": "Nuclear Physics B",
                        "year": 2023,
                        "artid": "116107",
                        "journal_volume": "None None",
                    }
                ],
                [
                    {
                        "journal_title": "Physics Letters B",
                        "year": 2023,
                        "artid": "137730",
                        "journal_volume": "None None",
                    }
                ],
                [
                    {
                        "journal_title": "Physics Letters B",
                        "year": 2023,
                        "artid": "137751",
                        "journal_volume": "None None",
                    }
                ],
            ],
            "publication_info",
            id="test_publication_info",
        ),
        pytest.param(
            ["", "", "2023-02-04", ""],
            "date_published",
            id="test_published_date",
        ),
        pytest.param(
            [
                [{"primary": "NUPHB"}],
                [{"primary": "NUPHB"}],
                [{"primary": "PLB"}],
                [{"primary": "PLB"}],
            ],
            "collections",
            id="test_collections",
        ),
        pytest.param(
            [
                [
                    {
                        "license": "CC-BY-3.0",
                        "url": "http://creativecommons.org/licenses/by/3.0/",
                    }
                ],
                [
                    {
                        "license": "CC-BY-3.0",
                        "url": "http://creativecommons.org/licenses/by/3.0/",
                    }
                ],
                [
                    {
                        "license": "CC-BY-3.0",
                        "url": "http://creativecommons.org/licenses/by/3.0/",
                    }
                ],
                [
                    {
                        "license": "CC-BY-3.0",
                        "url": "http://creativecommons.org/licenses/by/3.0/",
                    }
                ],
            ],
            "license",
            id="test_license",
        ),
        pytest.param(
            [
                {
                    "pdf": "extracted/CERNQ000000010669A/S0550321323000354/main.pdf",
                    "pdfa": "extracted/CERNQ000000010669A/S0550321323000354/main_a-2b.pdf",
                    "xml": "extracted/CERNQ000000010669A/S0550321323000354/main.xml",
                },
                {
                    "pdf": "extracted/CERNQ000000010669A/S0550321323000366/main.pdf",
                    "pdfa": "extracted/CERNQ000000010669A/S0550321323000366/main_a-2b.pdf",
                    "xml": "extracted/CERNQ000000010669A/S0550321323000366/main.xml",
                },
                {
                    "pdf": "extracted/CERNQ000000010669A/S0370269323000643/main.pdf",
                    "pdfa": "extracted/CERNQ000000010669A/S0370269323000643/main_a-2b.pdf",
                    "xml": "extracted/CERNQ000000010669A/S0370269323000643/main.xml",
                },
                {
                    "pdf": "extracted/CERNQ000000010669A/S0370269323000850/main.pdf",
                    "pdfa": "extracted/CERNQ000000010669A/S0370269323000850/main_a-2b.pdf",
                    "xml": "extracted/CERNQ000000010669A/S0370269323000850/main.xml",
                },
            ],
            "files",
            id="test_files",
        ),
    ],
)
@freeze_time("2023-11-02")
def test_elsevier_dataset_parsing(parsed_articles, expected, key):
    for parsed_article, expected_article in zip(
        parsed_articles, expected, strict=False
    ):
        assert expected_article == parsed_article.get(key, "")


@pytest.fixture
def articles_with_volume(shared_datadir):
    with open(shared_datadir / "dataset_bfrqq.xml") as file:
        return parse_without_names_spaces(file.read())


@pytest.fixture
@freeze_time("2023-11-02")
def parsed_articles_with_volume(parser, articles_with_volume):
    return [article for article in parser.parse(articles_with_volume)]


@pytest.mark.parametrize(
    ("expected", "key"),
    [
        pytest.param(
            [
                [
                    {
                        "journal_title": "Physics Letters B",
                        "journal_volume": "845 C",
                        "year": 2023,
                        "artid": "138110",
                    }
                ]
            ],
            "publication_info",
            id="test_publication_info",
        ),
    ],
)
@freeze_time("2023-11-02")
def test_elsevier_dataset_parsing_with_volume(
    parsed_articles_with_volume, expected, key
):
    for parsed_article, expected_article in zip(
        parsed_articles_with_volume, expected, strict=False
    ):
        assert expected_article == parsed_article[key]
