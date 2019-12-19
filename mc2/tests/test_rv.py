import data.release_versions as rv  # type: ignore
import pandas as pd  # type: ignore
from pytest import fixture, raises  # type: ignore


def test_prod_det_process_nightly_builds():
    n_total_builds = 5
    max_sub_date = "2019-12-10"
    n_days_later = 3
    n_builds, _max_sub_date = rv.prod_det_process_nightly_builds(
        max_sub_date=max_sub_date,
        n_total_builds=n_total_builds,
        n_days_later=n_days_later,
    )
    assert (
        (n_builds.applymap(type) == str).all().all()
    ), "Dataframe should be all strings"
    assert len(n_builds) == n_total_builds
    assert n_builds.till.max() <= max_sub_date
    n_builds_dates = n_builds.apply(pd.to_datetime)
    max_diff = (
        (n_builds_dates.till - n_builds_dates.release_date)
        .astype("timedelta64[D]")
        .astype(int)
        .max()
    )
    assert max_diff <= n_days_later


@fixture
def release_dates():
    rows = [
        ("2011-10-01", "70.0b11"),
        ("2019-11-15", "71.0b10"),
        ("2019-11-19", "71.0b11"),
        ("2019-11-22", "71.0b12"),
        ("2019-12-03", "72.0b1"),
        ("2019-12-04", "72.0b2"),
        ("2019-12-06", "72.0b3"),
        ("2019-12-09", "72.0b4"),
        ("2019-12-11", "72.0b5"),
        ("2019-12-13", "72.0b6"),
        ("2019-12-16", "72.0b7"),
        ("2019-12-18", "72.0b8"),
    ]
    return pd.DataFrame(rows, columns=["date", "version"])


def test_lookup_latest_release(release_dates):
    dates = pd.Series(
        ["2019-12-02", "2019-12-16", "2019-12-17", "2019-12-18", "2019-12-19"]
    )
    assert rv.lookup_latest_release(dates, release_dates) == {
        "2019-12-02": "71.0b12",
        "2019-12-16": "72.0b7",
        "2019-12-17": "72.0b7",
        "2019-12-18": "72.0b8",
        "2019-12-19": "72.0b8",
    }


def test_beta2nightly_version():
    assert rv.beta2nightly_version("72.0b8") == "73.0a1"
    assert rv.beta2nightly_version("69.0") == "70.0a1"
    assert rv.beta2nightly_version("200.0b9") == "201.0a1"


def test_validate_sub_date_str():
    assert rv.validate_sub_date_str("2019-01-01")
    with raises(ValueError):
        rv.validate_sub_date_str("2019-14-01")
    with raises(AssertionError):
        rv.validate_sub_date_str("2019-01-011")
