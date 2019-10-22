from pytest import raises
from src.download_bq import rls_version_parse, beta_version_parse


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
    assert beta_version_parse("70.0b3") == {"major": "70", "minor": "3"}
    assert beta_version_parse("69.0b200") == {"major": "69", "minor": "200"}
    raises(ValueError, beta_version_parse, "70.0.1")
