from data.download_bq import beta_version_parse, rls_version_parse
from pytest import raises


def test_rls_version_parse():
    assert rls_version_parse("69.0") == {
        "major": "69",
        "minor": "0",
        "dot": "0",
    }
    assert rls_version_parse("69.0.1") == {
        "major": "69",
        "minor": "0",
        "dot": "1",
    }

    raises(ValueError, rls_version_parse, "70.0b1")


def test_beta_version_parse():
    assert beta_version_parse("70.0") == {"major": "70", "minor": None}
    assert beta_version_parse("70.0b1") == {"major": "70", "minor": "1"}
    assert beta_version_parse("70.0b200") == {"major": "70", "minor": "200"}
    assert beta_version_parse("69.0.1") == {"major": "69", "minor": None}
    assert beta_version_parse("70.0.1b4") == {"major": "70", "minor": "4"}
