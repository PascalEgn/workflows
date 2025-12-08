from common.constants import COUNTRY_PARSING_PATTERN, ORGANIZATION_PARSING_PATTERN

affiliation_string = (
    "Department of Physics, University of Oregon, Eugene, Oregon 97403, USA"
)


def test_country_regex():
    assert COUNTRY_PARSING_PATTERN.search(affiliation_string).group(0) == "USA"


def test_organization_regex():
    assert (
        ORGANIZATION_PARSING_PATTERN.sub("", affiliation_string)
        == "Department of Physics, University of Oregon, Eugene, Oregon 97403"
    )
