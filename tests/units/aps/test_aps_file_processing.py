import json
from io import BytesIO

import pytest
from airflow.models import DagBag
from aps.aps_process_file import enhance_aps, enrich_aps
from aps.parser import APSParser


@pytest.fixture
def articles(shared_datadir):
    json_response = (shared_datadir / "json_response_content.json").read_text()
    return [article for article in json.loads(json_response)["data"]]


@pytest.fixture
def parser():
    return APSParser()


@pytest.fixture
def parsed_articles(articles, parser):
    return [parser._publisher_specific_parsing(article) for article in articles]


@pytest.fixture
def parsed_generic_articles(parsed_articles, parser):
    return [parser._generic_parsing(article) for article in parsed_articles]


@pytest.fixture
def enhance_articles(parsed_generic_articles):
    return [enhance_aps(article) for article in parsed_generic_articles]


@pytest.fixture
def enrich_article(enhance_articles):
    return [enrich_aps(article) for article in enhance_articles]


@pytest.fixture(scope="class")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.mark.usefixtures("dagbag")
class TestUnitApsProcessFileDag:
    def setup_method(self):
        self.dag_id = "aps_process_file"
        self.dag = DagBag(dag_folder="dags/", include_examples=False).get_dag(
            self.dag_id
        )
        assert self.dag is not None, f"DAG {self.dag_id} failed to load"

    def test_parse_xml_without_xml_file_returns_empty_dict(self):
        task = self.dag.get_task("parse_xml")
        function_to_unit_test = task.python_callable

        result = function_to_unit_test({"files": {}})

        assert result == {}

    def test_parse_xml_with_xml_file_reference_extracts_fields(self, monkeypatch):
        task = self.dag.get_task("parse_xml")
        function_to_unit_test = task.python_callable

        xml_content = b"""
        <article>
            <front>
                <article-meta>
                    <contrib-group>
                        <contrib contrib-type='author'>
                            <name>
                                <given-names>Jane</given-names>
                                <surname>Doe</surname>
                            </name>
                        </contrib>
                    </contrib-group>
                </article-meta>
            </front>
            <sec sec-type='data-availability'>
                <title>DATA AVAILABILITY</title>
                <p>Data available on request.</p>
            </sec>
        </article>
        """

        class FakeScoap3Repository:
            def get_by_id(self, id):
                assert id == "scoap3/files/test/article.xml"
                return BytesIO(xml_content)

        monkeypatch.setitem(
            function_to_unit_test.__globals__,
            "Scoap3Repository",
            FakeScoap3Repository,
        )

        result = function_to_unit_test(
            {"files": {"xml": "scoap3/files/test/article.xml"}}
        )

        assert len(result["authors"]) == 1
        assert result["authors"][0]["given_names"] == "Jane"
        assert result["authors"][0]["surname"] == "Doe"
        assert result["authors"][0]["full_name"] == "Doe, Jane"
        assert result["data_availability"]["statement"] == "Data available on request."
