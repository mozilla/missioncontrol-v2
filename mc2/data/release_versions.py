"""
This module calculates date ranges and the relevant versions
that were active for those dates.

The general format for `prod_det_process_{channel}` is to return
a dataframe with a `version` field and a corresponding
`release_date` and `till` field which convey the beginning and
ending submission dates in which that version was the primary
version (and should have high `nvc`).
"""

import datetime as dt
import re
from collections import OrderedDict
from typing import List, Optional

import buildhub_bid as bh
import numpy as np  # type: ignore
import pandas as pd  # type: ignore

SUB_DATE_FMT = "%Y-%m-%d"
BUILD_DAY_FMT = "%Y%m%d"
dash_date_fmt = str


def to_sub_date_fmt(d):
    return d.strftime(SUB_DATE_FMT)


def to_build_day_fmt(d):
    return d.strftime(BUILD_DAY_FMT)


def validate_sub_date_str(d):
    m = re.match(r"^\d\d\d\d-\d\d-\d\d$", d)
    assert m, "Date doesn't match format YYYY-MM-dd"
    assert pd.to_datetime(d)
    return True


def read_product_details():
    pd_url = "https://product-details.mozilla.org/1.0/firefox.json"
    js = pd.read_json(pd_url)
    df = (
        pd.DataFrame(js.releases.tolist())
        .assign(release_label=js.index.tolist())
        .assign(date=lambda x: pd.to_datetime(x.date))
    )
    return df


def next_release_date(d: "pd.Series[dt.datetime]", max_sub_date: str):
    """
    d: Series of dates of releases, ordered by time
    return: date of next release. For most recent release,
        return `max_sub_date`
    """
    max_sub_date = pd.to_datetime(max_sub_date)
    return d.shift(-1).fillna(max_sub_date)


def pull_bh_data_beta(min_build_date):
    beta_docs = bh.pull_build_id_docs(
        min_build_day=min_build_date, channel="beta"
    )
    return bh.version2build_ids(
        beta_docs,
        major_version=None,
        keep_rc=False,
        keep_release=False,
        as_df=True,
    ).assign(chan="beta")
    return bh.version2df(beta_docs, keep_rc=False, keep_release=True).assign(
        chan="beta"
    )


def get_beta_release_dates(min_build_date="2019", min_pd_date="2019-01-01"):
    """
    Stitch together product details and buildhub
    (for rc builds).
    - read_product_details()
    """
    bh_beta_rc = (
        pull_bh_data_beta(min_build_date=min_build_date)
        .rename(columns={"pub_date": "date", "dvers": "version"})
        .assign(rc=lambda x: x.version.map(is_rc), src="buildhub")
        .query("rc")[["date", "version", "src"]]
        .sort_values(["date"], ascending=True)
        .drop_duplicates(["version"], keep="first")
    )
    # return bh_beta_rc
    prod_details_all = read_product_details()
    prod_details_beta = prod_details_all.query(
        f"category == 'dev' & date > '{min_pd_date}'"
    )[["date", "version"]].assign(src="product-details")

    beta_release_dates = (
        prod_details_beta.append(bh_beta_rc, ignore_index=False)
        .assign(
            maj_vers=lambda x: x.version.map(lambda x: int(x.split(".")[0]))
        )
        .sort_values(["date"], ascending=True)
        .reset_index(drop=1)
        # Round down datetimes to nearest date
        .assign(date=lambda x: pd.to_datetime(x.date.dt.date))
        .astype(str)
    )
    return beta_release_dates


def end_till_date(date: pd.Series, n_days_later, max_sub_date):
    max_sub_date = pd.to_datetime(max_sub_date)
    till = pd.to_datetime(date) + pd.Timedelta(days=n_days_later)
    return np.minimum(till.dt.date, max_sub_date)


def prod_det_process_beta(max_sub_date: str, n_total_builds=4, n_days_later=4):
    beta_release_dates = get_beta_release_dates(
        min_build_date="2019", min_pd_date="2019-01-01"
    )
    beta_release_dates = (
        beta_release_dates.assign(date=lambda x: pd.to_datetime(x.date))
        .query(f"date < '{max_sub_date}'")
        .sort_values(["date"], ascending=True)[-n_total_builds:]
        # .assign(till=lambda x: x.date + pd.Timedelta(days=n_days_later))
        .assign(
            # till=lambda x: end_till_date(x.date, n_days_later, max_sub_date),
            till=lambda x: next_release_date(x.date, max_sub_date)
        )
    )

    return beta_release_dates


def is_rc(v):
    """
    Is pattern like 69.0, rather than 69.0b3
    """
    return "b" not in v


def latest_n_release_beta(beta_release_dates, sub_date, n_releases: int = 1):
    """
    Given dataframe with beta release dates and a given
    submission date (can be from the past till today), return the
    `n_releases` beta versions that were released most recently.
    beta_release_dates: df[['date', 'version', 'src', 'maj_vers']]
    """
    beta_release_dates = beta_release_dates[
        ["date", "version", "src", "maj_vers"]
    ]
    latest = (
        beta_release_dates
        # Don't want versions released in the future
        .query("date < @sub_date")
        .sort_values(["date"], ascending=True)
        .iloc[-n_releases:]
    )

    return latest


###########
# Nightly #
###########
def prod_det_process_nightly_builds(
    max_sub_date=None, n_total_builds=4, n_days_later=4
):
    """
    `max_sub_date`: "today," or a day that was today at some point
    `n_total_builds`: if 2, then pull the builds from today and the day before
    `n_days_later`: if 1, then for each build, pull for activity of
    that day and the day after.
    """
    max_sub_date_ts = pd.to_datetime(max_sub_date or dt.datetime.today())
    max_sub_date: dt.date = max_sub_date_ts.date()

    build_dates = pd.date_range(end=max_sub_date, periods=n_total_builds)
    meta = (
        pd.DataFrame(
            OrderedDict(
                [
                    ("build_id", build_dates.strftime(BUILD_DAY_FMT)),
                    ("release_date", build_dates),
                    ("till", build_dates + pd.Timedelta(days=n_days_later)),
                ]
            )
        )
        .assign(till=lambda x: np.minimum(x.till.dt.date, max_sub_date))
        .assign(
            release_date=lambda x: x.release_date.map(to_sub_date_fmt),
            till=lambda x: x.till.map(to_sub_date_fmt),
        )
    )
    return meta, max_sub_date


def lookup_latest_release(dates, release_dates):
    """
    release_dates: DataFrame["date", "version"]
    """
    # Get a subset of the most recent release dates, starting
    # with the one just before the minimum `dates` to query,
    # and ending with the most recent.
    rls_srtd = release_dates[["date", "version"]].sort_values(
        ["date"], ascending=True
    )
    min_lookup_date = dates.min()  # noqa
    min_release_date = rls_srtd.query(  # noqa
        "date <= @min_lookup_date"
    ).date.iloc[-1]
    rls_recent = rls_srtd.query("date >= @min_release_date")

    dates_dct = {}
    for date in dates:
        dates_dct[date] = rls_recent.query("date <= @date").version.iloc[-1]

    return dates_dct


def beta2nightly_version(disp):
    betav, *_ = disp.split(".")
    return f"{int(betav) + 1}.0a1"


def prod_det_process_nightly(
    max_sub_date: Optional[dash_date_fmt] = None,
    product_details: Optional[pd.DataFrame] = None,
    n_total_builds=4,
    n_days_later=4,
):
    meta, max_sub_date = prod_det_process_nightly_builds(
        max_sub_date=max_sub_date,
        n_total_builds=n_total_builds,
        n_days_later=n_days_later,
    )
    max_sub_date = to_sub_date_fmt(pd.to_datetime(max_sub_date))

    product_details = (
        read_product_details() if product_details is None else product_details
    )
    pd_beta = product_details.query("category == 'dev' & date >= '2019-01'")[
        ["date", "version"]
    ]

    date2disp_version = lookup_latest_release(meta.release_date, pd_beta)
    meta = meta.assign(
        current_beta=lambda x: x.release_date.map(date2disp_version)
    ).assign(
        nightly_display_version=lambda x: x.current_beta.map(
            beta2nightly_version
        )
    )
    return meta


#######
# ESR #
#######
def prod_det_extract_esr(max_sub_date: str, pd_all=None, n_days_later: int = 2):
    """
    Extract esr-related data from product-details document. This will
    return 2 dataframes, one for the current release and the one which has
    received stability updates.
    """
    if pd_all is None:
        pd_all = read_product_details()
    max_sub_date_ts = pd.to_datetime(max_sub_date or dt.datetime.today())
    max_sub_date_dt: dt.date = max_sub_date_ts.date()

    pd_esr_all = (
        pd_all.rename(columns={"date": "release_date"})
        .pipe(lambda x: x[x.release_label.str.endswith("esr")])
        .drop(["description", "is_security_driven", "product"], axis=1)
        .assign(release_date=lambda x: pd.to_datetime(x.release_date))
        .query(f"release_date < '{max_sub_date_dt}'")
        .sort_values(["release_date"], ascending=True)
        .reset_index(drop=1)
    )

    proc = lambda df: df.assign(
        till=lambda x: next_release_date(x.release_date, max_sub_date)
        + pd.Timedelta(days=n_days_later)
    )
    pd_esr_stab = pd_esr_all.query("category == 'stability'").pipe(proc)
    pd_esr_new = pd_esr_all.query("category == 'esr'").pipe(proc)

    # print(f"pd_esr_stab: {pd_esr_stab}")
    # print(f"pd_esr_new: {pd_esr_new}")

    return pd_esr_stab, pd_esr_new


def esr_lookup_version(sub_dates: List[str], esr_meta: pd.DataFrame):
    """
    @esr_meta: DataFrame[release_date, version, till]
    Returns list of (sub_date, version) pairs. Both sub_dates and versions
    can be duplicated (but not the combination).
    """
    version_dates = []
    for sub_date in sub_dates:
        release_data = esr_meta.query(
            f"release_date <= '{sub_date}' & till >= '{sub_date}'"
        )
        for version in release_data.version:
            version_dates.append((sub_date, version))
    return version_dates


def esr_query_sub_dates(sub_dates, meta_current, meta_stable):
    """
    For a given submission date that we're interested in,
    this will look up what the most recent 'new' and 'stability'
    release versions are from product details.

    `meta_current` and `meta_stable` are prod-details-derived metadata
    tables for current and stable ESR releases. They should be of
    type `DataFrame[release_date, version, till]`.
    """
    sub_date_versions_ = esr_lookup_version(
        sub_dates, meta_current
    ) + esr_lookup_version(sub_dates, meta_stable)
    sub_date_versions = (
        pd.DataFrame(sub_date_versions_, columns=["sub_date", "version"])
        .groupby(["version"])
        .sub_date.agg(["min", "max"])
    )

    return sub_date_versions


def prod_det_process_esr(
    max_sub_date: str,
    pd_all=None,
    n_days_later: int = 4,
    n_days_activity: int = 2,
):
    """
    ESR data will typically be pulled with `max_sub_date == today`, but it can
    be set for other dates in the past for backfill.
    @pd_all: product-details DataFrame from `read_product_details()`
    @n_days_activity: if 2, then pull data for `max_sub_date` and the day
    before.
    @n_days_later: if 4, then extend the window of activity for version V to 4
    days past the release of version V + 1.
    """
    sub_dates = pd.date_range(
        end=max_sub_date, periods=n_days_activity, freq="D"
    )
    pd_esr_stab, pd_esr_new = prod_det_extract_esr(
        max_sub_date=max_sub_date, pd_all=pd_all, n_days_later=n_days_later
    )
    sub_date_versions = esr_query_sub_dates(
        sub_dates=sub_dates, meta_current=pd_esr_new, meta_stable=pd_esr_stab
    ).reset_index(drop=0)

    return sub_date_versions
