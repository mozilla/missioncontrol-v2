from data.download_bq import (  # type: ignore
    beta_version_parse,
    rls_version_parse,
    esr_version_parse,
)
from pytest import raises  # type: ignore


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
    assert beta_version_parse("70.0") == {"major": "71", "minor": "0"}
    assert beta_version_parse("70.0b1") == {"major": "70", "minor": "1"}
    assert beta_version_parse("70.0b200") == {"major": "70", "minor": "200"}
    assert beta_version_parse("69.0.1") == {"major": "70", "minor": "0"}
    assert beta_version_parse("70.0.1b4") == {"major": "70", "minor": "4"}


def test_esr_version_parse():
    assert esr_version_parse("52.9.0") == {"major": "52", "minor": 900}
    assert esr_version_parse("60.4.0esr") == {"major": "60", "minor": 400}
    assert esr_version_parse("60.3.0esr") == {"major": "60", "minor": 300}
    assert esr_version_parse("52.6.0") == {"major": "52", "minor": 600}
    assert esr_version_parse("52.8.0") == {"major": "52", "minor": 800}
    assert esr_version_parse("52.0") == {"major": "52", "minor": 0}
