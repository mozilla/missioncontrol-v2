import datetime as dt
import os
import re
import sys
import tempfile
from contextlib import contextmanager
from typing import Optional

import pandas as pd  # type: ignore
import release_versions as rv
from bq_utils import BqLocation
from pandas import DataFrame
from pandas.testing import assert_frame_equal  # type: ignore

SQL_FNAME = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "download_template.sql"
)
dbg = lambda: None
SUB_DATE_FMT = "%Y-%m-%d"
dash_date_fmt = str  # "%Y-%m-%d"


def to_sub_date_fmt(d):
    return d.strftime(SUB_DATE_FMT)


os_dtype = pd.CategoricalDtype(
    categories=["Linux", "Darwin", "Windows_NT"], ordered=True
)


def next_release_date(
    d: "pd.Series[dt.datetime]", max_days_future=365, use_today_as_max=False
):
    """
    d: Series of dates of releases, ordered by time
    return: date of next release. For most recent release,
        return that date + `max_days_future`
    """
    max_date = pd.to_datetime(dt.date.today()) if use_today_as_max else d.max()

    way_later = max_date + pd.Timedelta(days=max_days_future)
    return d.shift(-1).fillna(way_later)


def add_version_elements(df, element_parser, colname, to=int):
    """
    @element_parser: str -> {element_name: element_val}
    e.g., 65.0b10 -> {'major': '65', 'minor': '10'}
    """
    version_elems = df[colname].map(element_parser).tolist()
    elems_df = DataFrame(version_elems, index=df.index)
    for c in elems_df:
        df[c] = elems_df[c].astype(to)
    return df


def rls_version_parse(disp_vers: str):
    pat = re.compile(r"(?P<major>\d+)\.(?P<minor>\d+)(?:\.(?P<dot>\d+))?$")
    m = pat.match(disp_vers)
    if not m:
        raise ValueError(f"unable to parse version string: {disp_vers}")
    res = m.groupdict()
    if not res["dot"]:
        res["dot"] = "0"
    return res


def beta_version_parse(disp_vers: str):
    """
    This has a little twist for rc builds. If it detects an rc build,
    it sets the minor number to 0 and increments the major number.
    So `69.0b3 => (69, 3)`, but `70.0 => 71, 0`. This will allow for
    proper sorting by (major, minor).
    """
    pat = re.compile(r"(?P<major>\d+)(?:\.\d+)+b?(?P<minor>\d+)?")
    m = pat.match(disp_vers)
    if not m:
        raise ValueError(f"unable to parse version string: {disp_vers}")
    d = m.groupdict()
    if d.get("minor") is None:
        d["minor"] = "0"
        d["major"] = str(int(d["major"]) + 1)
    return d


def esr_version_parse(disp_vers: str):
    disp_vers = disp_vers.rstrip("esr")
    pat = re.compile(r"(?P<major>\d+)\.(?P<minor>\d+)(?:\.(?P<dot>\d+))?$")

    m = pat.match(disp_vers)
    if not m:
        raise ValueError(f"unable to parse version string: {disp_vers}")
    d = m.groupdict()
    if not d.get("dot"):
        d["dot"] = "0"
    minor = int(d.pop("minor")) * 100 + int(d.pop("dot"))
    d["minor"] = minor
    return d


def get_peak_date(vers_df, vers_col="dvers", date_col="date"):
    """
    Get nvc peak date
    """

    def max_nvc_date(ss):
        i = ss.nvc.values.argmax()
        return ss[date_col].iloc[i]

    if not len(vers_df):
        print("Empty dataframe passed. Recent release date?")
        return vers_df.assign(peak_date=[])

    agg_v_o_max_date = (
        vers_df.groupby([vers_col, "os"])
        .apply(max_nvc_date)
        .rename("peak_date")
    )
    vers_df2 = vers_df.merge(
        agg_v_o_max_date, left_on=[vers_col, "os"], right_index=True
    ).sort_index()
    assert_frame_equal(vers_df2.drop("peak_date", axis=1), vers_df)

    return vers_df2


########################
# Product Detail Pulls #
########################
# TODO: choose better min day than "2019-01-01"
def prod_det_process_release(df_all, sub_date: dash_date_fmt):
    """
    Process and categorize data from
    https://product-details.mozilla.org/1.0/firefox.json
    * filter major/release versions
    * parse versions
    * find most recent major (e.g., 71), designate it as the current version
        * subset to the 3 most recent major versions (71, 70, 69)

    Useful return dataframe is `df_release_data`, but others are returned as
    well to fit the pattern for `prod_det_process_beta`, where the intermediate
    DataFrame is needed for the nightly pull.
    `till` column is added so that `add_fields` (called in pull_data_base)
    can use it as an endpoint to calculate whether a version is still 'active'
    or 'current'.
    """
    # Release
    max_days_future = 365

    df = df_all.query(
        'category in ["major", "stability"]'
        '& date >= "2019-01-01" & date < @sub_date'
    ).copy()

    df = add_version_elements(
        df, rls_version_parse, colname="version", to=int
    ).sort_values(["major", "minor", "dot"], ascending=True)

    # Subset for building model
    cur_major = df.major.iloc[-1]  # noqa

    # Still need for data pull
    df_release_data = (
        df.query("major == @cur_major")[["version", "date"]]
        .assign(
            till=lambda x: next_release_date(
                x["date"], max_days_future=max_days_future
            )
        )
        .assign(date=lambda x: x.date.dt.date, till=lambda x: x.till.dt.date)
    )  # better string repr

    return df_release_data


def prod_det_process_beta(df_all):
    """
    For most recent beta release, pull 7 days into the future.
    Use product-details data to
    - filter data relevant for beta releases (category = "dev")
    - parse versions to be able to correctly sort
    """
    days_future = 7
    category = "dev"  # noqa

    df = df_all.query('category == @category & date >= "2019-01-01"').copy()
    df = add_version_elements(df, beta_version_parse, "version").sort_values(
        ["major", "minor"], ascending=True
    )

    cur_major = df.major.iloc[-1]
    # Still need for data pull
    df_download_data = (
        df.query(f"major == {cur_major}")[["version", "date"]]
        .assign(till=lambda x: next_release_date(x["date"], days_future))
        # better string repr
        .assign(date=lambda x: x.date.dt.date, till=lambda x: x.till.dt.date)
        .reset_index(drop=1)
    )
    return df_download_data


#################
# Build Queries #
#################
def sql_arg_dict(row):
    return dict(current_version=row.version, cur_vers_release_date=row.date)


def to_sql_str_list(xs):
    return ", ".join("'{}'".format(s) for s in xs)


def query_from_row_release(row, sql_template):
    """
    Given a sql template string, and a row from product details
    https://product-details.mozilla.org/1.0/firefox.json
    that's been processed, fill in the sql template with
    dates and details for a particular release.
    """
    kwargs = sql_arg_dict(row)
    kwargs_channel = dict(
        current_version_crash=f"'{row.version}'",
        app_version_field="app_version",
        crash_build_version_field="environment.build.version",
        norm_channel="release",
        nday=72,
    )
    return sql_template.format(**kwargs, **kwargs_channel)


def query_from_row_beta(row, sql_template):
    kwargs = sql_arg_dict(row)
    kwargs_channel = dict(
        current_version_crash=f"'{row.version}'",
        app_version_field="app_display_version",
        crash_build_version_field="environment.build.display_version",
        norm_channel="beta",
        nday=14,
    )
    return sql_template.format(**kwargs, **kwargs_channel)


def query_from_row_nightly(row, sql_template):
    kwargs = sql_arg_dict(row)
    kwargs["current_version"] = row.build_id
    kwargs_channel = dict(
        current_version_crash=f"'{row.build_id}'",
        app_version_field="substr(app_build_id,1,8)",
        crash_build_version_field=("substr(environment.build.build_id, 1, 8)"),
        norm_channel="nightly",
        nday=7,
    )

    return sql_template.format(**kwargs, **kwargs_channel)


########################
# Actual Data Download #
########################
def add_fields(df, current_version, date0, till):
    """
    Applied df by df to chunks as they are downloaded
    """
    date0, till = map(pd.to_datetime, [date0, till])
    round_down = lambda x: 0 if x < 60 / 3600 else x

    df = df.assign(
        # date=date0,
        c_version=current_version,
        c_version_rel=date0,
        isLatest=lambda x: x.date <= till,
        t=lambda x: (x.date - date0).astype("timedelta64[D]").astype(int),
    ).assign(
        cmi=lambda x: (1 + x.dau_cm_crasher_cversion) / x.dau_cversion,
        cmr=lambda x: (1 + x.cmain)
        / x.usage_cm_crasher_cversion.map(round_down),
        # cmr=lambda x: (1 + x.cmain)
        # .div(x.usage_cm_crasher_cversion).map(round_down),
        cci=lambda x: (1 + x.dau_cc_crasher_cversion) / x.dau_cversion,
        ccr=lambda x: (1 + x.ccontent)
        / x.usage_cc_crasher_cversion.map(round_down),
        nvc=lambda x: x.usage_cversion / x.usage_all,
        os=lambda x: x.os.astype(os_dtype),
    )

    # TODO: this might not be necessary
    date_cols = "date c_version_rel".split()  # c_version
    for date_col in date_cols:
        df[date_col] = df[date_col].dt.date
    return df


def add_fields_single(df):
    """
    Similar to add_fields, but for sql pull where all versions
    are pulled in single query.
    Currently works for beta, but should be applied to other channels
    as well.
    """
    round_down = lambda x: 0 if x < 60 / 3600 else x

    df = df.assign(
        isLatest=lambda x: x.date <= x.till,
        t=lambda x: (x.date - x.release_date)
        .astype("timedelta64[D]")
        .astype(int),
    ).assign(
        cmi=lambda x: (1 + x.dau_cm_crasher_cversion) / x.dau_cversion,
        cmr=lambda x: (1 + x.cmain)
        / x.usage_cm_crasher_cversion.map(round_down),
        cci=lambda x: (1 + x.dau_cc_crasher_cversion) / x.dau_cversion,
        ccr=lambda x: (1 + x.ccontent)
        / x.usage_cc_crasher_cversion.map(round_down),
        nvc=lambda x: x.usage_cversion / x.usage_all,
        os=lambda x: x.os.astype(os_dtype),
    )

    date_cols = "date release_date".split()
    for date_col in date_cols:
        df[date_col] = df[date_col].dt.date
    return df


#############
# Data pull #
#############
def verbose_query(q):
    def pipe_func(df):
        bm = df.eval(q)
        df_drop = df[~bm]
        print("Dropping {} rows that don't match `{}`:".format(len(df_drop), q))
        print(df_drop[["date", "os", "c_version"]])
        return df[bm].copy()

    return pipe_func


def pull_data_base(
    sql_template, download_meta_data, row2query, bq_read, version_col="version"
):
    dfs = []
    for _ix, row in download_meta_data.iterrows():
        print(f"pulling {version_col}={row[version_col]}")
        query = row2query(row, sql_template)
        dbg.query = query
        df = bq_read(query)
        df2 = add_fields(df, row[version_col], row.date, row.till)
        dfs.append(df2)

    res = pd.concat(dfs, ignore_index=True)
    return res


def pull_data_release(download_meta_data, sql_template, bq_read, process=True):
    """
    isLatest comes from product_details.json, looking for next release
    date within a channel.
    """
    data = pull_data_base(
        sql_template,
        download_meta_data,
        query_from_row_release,
        bq_read=bq_read,
    )
    data = add_version_elements(data, rls_version_parse, "c_version", to=int)
    if process:
        data = (
            get_peak_date(data, "c_version")
            .pipe(verbose_query("(date <= peak_date) or isLatest"))
            .drop(["minor", "peak_date"], axis=1)
            .rename(columns={"dot": "minor"})
            .reset_index(drop=1)
        )
    return data


def build_version_date_filter(meta, date_field, version_field):
    """
    meta: DataFrame['version', 'release_date', 'till']
        - for channels with faster release cycles like
        beta and nightly, `release_date` makes more sense,
        but more generally the idea is
        DataFrame['version', 'submission_start', 'submission_end']
    date_field: SQL field name for dates
    version_field: SQL field name for version

    Given dataframe with version metadata, munge together a SQL filter
    of the form
    ```
    (channel_app_version = '70.0b1'
      and date between '2019-12-01' and '2019-12-02')
    OR (channel_app_version = '70.0b2'
      and date between '2019-12-03' and '2019-12-04')
    OR ...
    ```
    """
    version_filters = []
    for row in meta.itertuples(index=False):
        filter_str = (
            f"({version_field} = '{row.version}' and "
            f"{date_field} between '{row.release_date}' and '{row.till}')"
        )
        version_filters.append(filter_str)

    return "(" + "\n\tOR ".join(version_filters) + ")"


def pull_data_beta(download_meta_data, sql_template, bq_read):
    download_meta_data = download_meta_data[
        ["version", "release_date", "till"]
    ].assign(
        release_date=lambda x: x.release_date.map(to_sub_date_fmt),
        till=lambda x: x.till.map(to_sub_date_fmt),
    )
    app_version_field = "app_display_version"
    crash_build_version_field = "environment.build.display_version"

    version_filter = build_version_date_filter(
        download_meta_data,
        date_field="date",
        version_field="channel_app_version",
    )
    crash_version_filter = build_version_date_filter(
        download_meta_data,
        date_field="date",
        version_field="channel_app_version_crash",
    )

    kwargs = dict(
        norm_channel="beta",
        app_version_field=app_version_field,
        crash_build_version_field=crash_build_version_field,
        min_sub_date=download_meta_data.release_date.min(),
        max_sub_date=download_meta_data.till.max(),
        current_usage_versions_dates=version_filter,
        crash_current_versions_dates=crash_version_filter,
    )

    beta_query = sql_template.format(**kwargs)
    data = bq_read(beta_query)

    # date, till, c_version_rel need to be datetime
    # This gets us the `release_date` and `till` columns,
    # latter based on future release dates
    data = (
        data.merge(
            download_meta_data,
            left_on=["channel_app_version"],
            right_on=["version"],
        )
        .drop("version", axis=1)
        .assign(
            **{
                date_col: lambda x, c=date_col: pd.to_datetime(x[c])
                for date_col in ["release_date", "date", "till"]
            }
        )
    )

    # `add_fields_single` needs `till` to calculate whether a version
    # is still active (`isLatest`). We don't need it anymore after that though.
    data = (
        add_fields_single(data)
        .rename(
            columns={
                "release_date": "c_version_rel",
                "channel_app_version": "c_version",
            }
        )
        .drop(["till"], axis=1)
    )
    data = add_version_elements(data, beta_version_parse, "c_version", to=int)
    data = (
        get_peak_date(data, "c_version")
        .pipe(verbose_query("date <= peak_date"))
        .drop(["peak_date"], axis=1)
        .reset_index(drop=1)
    )
    return data


def pull_data_nightly(download_meta_data, sql_template, bq_read):
    # TODO: convert to single pull
    meta = download_meta_data.assign(
        major=lambda x: x.nightly_display_version.map(
            lambda v: int(v.split(".")[0])
        )
    ).rename(
        columns={"nightly_display_version": "version", "release_date": "date"}
    )

    data = pull_data_base(
        sql_template,
        meta,
        query_from_row_nightly,
        bq_read=bq_read,
        version_col="build_id",
    )
    data = (
        get_peak_date(data, "c_version")
        .pipe(verbose_query("date <= peak_date"))
        .drop(["peak_date"], axis=1)
        .assign(minor=lambda x: x.c_version)
        .reset_index(drop=1)
    )

    # Get major version based on release dates of beta releases
    data2 = data.merge(
        meta.rename(columns={"build_id": "c_version"})[["c_version", "major"]],
        on="c_version",
    )
    # Making sure we don't get duplicate rows or anything from the join
    assert_frame_equal(data, data2.drop(["major"], axis=1))
    return data2


def pull_data_esr(download_meta_data, sql_template, bq_read):
    """
    @download_meta_data: DataFrame[version, min, max]
        - version, and corresponding min/max submission dates
    """
    download_meta_data = download_meta_data.rename(
        columns={"min": "release_date", "max": "till"}
    )[["version", "release_date", "till"]].assign(
        release_date=lambda x: x.release_date.map(to_sub_date_fmt),
        till=lambda x: x.till.map(to_sub_date_fmt),
    )

    # Sometimes `app_display_version` has esr suffix, sometimes not
    # https://sql.telemetry.mozilla.org/queries/67591/source
    app_version_field = "app_version"
    # https://sql.telemetry.mozilla.org/queries/67592/source
    crash_build_version_field = "environment.build.version"

    version_filter = build_version_date_filter(
        download_meta_data,
        date_field="date",
        version_field="channel_app_version",
    )
    crash_version_filter = build_version_date_filter(
        download_meta_data,
        date_field="date",
        version_field="channel_app_version_crash",
    )

    kwargs = dict(
        norm_channel="esr",
        app_version_field=app_version_field,
        crash_build_version_field=crash_build_version_field,
        min_sub_date=download_meta_data.release_date.min(),
        max_sub_date=download_meta_data.till.max(),
        current_usage_versions_dates=version_filter,
        crash_current_versions_dates=crash_version_filter,
    )

    esr_query = sql_template.format(**kwargs)

    with open("/tmp/s.sql", "w") as fp:
        fp.write(esr_query)
    # aa
    data = bq_read(esr_query)

    # date, till, c_version_rel need to be datetime
    # This gets us the `release_date` and `till` columns,
    # latter based on future release dates
    data = (
        data.merge(
            download_meta_data,
            left_on=["channel_app_version"],
            right_on=["version"],
        )
        .drop("version", axis=1)
        .assign(
            **{
                date_col: lambda x, c=date_col: pd.to_datetime(x[c])
                for date_col in ["release_date", "date", "till"]
            }
        )
    )

    # `add_fields_single` needs `till` to calculate whether a version
    # is still active (`isLatest`). We don't need it anymore after that though.
    data = (
        add_fields_single(data)
        .rename(
            columns={
                "release_date": "c_version_rel",
                "channel_app_version": "c_version",
            }
        )
        .drop(["till"], axis=1)
    )
    data = add_version_elements(data, esr_version_parse, "c_version", to=int)
    data = (
        get_peak_date(data, "c_version")
        .pipe(verbose_query("date <= peak_date"))
        .drop(["peak_date"], axis=1)
        .reset_index(drop=1)
    )
    return data


###########
# Combine #
###########
@contextmanager
def pull_done(msg):
    try:
        print("{}...".format(msg), end="")
        sys.stdout.flush()
        yield
    finally:
        print(" Done.")


def read(fname):
    if not os.path.exists(fname):
        raise Exception(
            "{} not found. Make sure pwd is set to project dir.".format(fname)
        )
    with open(fname, "r") as fp:
        txt = fp.read()
    return txt


def write(fname, txt):
    with open(fname, "w") as fp:
        fp.write(txt)


def pull_all_model_data(
    bq_read,
    sql_fname=SQL_FNAME,
    sub_date_str: Optional[str] = None,
    # channel: Optional[str] = None,
    esr=False,
    n_days_activity: int = 2,
):
    """
    @n_days_activity: only implemented for ESR.
    """
    sub_date_str = sub_date_str or dt.datetime.today().strftime("%Y-%m-%d")
    sql_template = read(sql_fname)
    if esr:
        channels = {"esr"}
    else:
        channels = {"release", "beta", "nightly", "esr"}
    # Implemented for nightly, beta, esr
    sql_template_single = read("data/download_template_single.sql")

    pd_all = rv.read_product_details()

    if "esr" in channels:
        metadata_esr = rv.prod_det_process_esr(
            sub_date_str,
            pd_all=pd_all,
            n_days_activity=n_days_activity,
            n_days_later=4,
        )
        print("ESR metadata:")
        print(metadata_esr)

        with pull_done("\nPulling ESR data"):
            df_esr = pull_data_esr(metadata_esr, sql_template_single, bq_read)
        df_esr = df_esr.assign(channel="esr")

    if esr:
        return df_esr

    pd_release_download = prod_det_process_release(
        pd_all, sub_date=sub_date_str
    )

    with pull_done("\nPulling release data"):
        df_release = pull_data_release(
            pd_release_download, sql_template, bq_read
        )

    pd_beta_download = rv.prod_det_process_beta(
        sub_date_str, n_total_builds=4, n_days_later=4
    )

    print("Beta metadata:")
    print(pd_beta_download)
    with pull_done("\nPulling beta data"):
        df_beta = pull_data_beta(
            pd_beta_download.rename(columns={"date": "release_date"}),
            sql_template_single,
            bq_read,
        )

    pd_nightly_download = rv.prod_det_process_nightly(
        product_details=pd_all, max_sub_date=sub_date_str
    )
    print("Nightly metadata:")
    print(pd_nightly_download)

    with pull_done("\nPulling nightly data"):
        df_nightly = pull_data_nightly(
            pd_nightly_download, sql_template, bq_read
        )

    df_all = pd.concat(
        [
            df_release.assign(channel="release"),
            df_beta.assign(channel="beta"),
            df_nightly.assign(channel="nightly"),
        ],
        ignore_index=True,
        sort=False,
    ).assign(
        minor=lambda x: x.minor.astype(int), major=lambda x: x.major.astype(int)
    )

    return df_all


############################################
# Pull model data after it's been uploaded #
############################################
def pull_model_data_pre_query(
    bq_read_fn, channel, n_majors, bq_loc: BqLocation
):
    pre_query = f"""
    select distinct major from {bq_loc.sql}
    where channel = '{channel}'
    ORDER BY major desc
    LIMIT {n_majors}
    """
    print(pre_query)
    pre_data = bq_read_fn(pre_query)
    prev_majors = pre_data.major.astype(int).tolist()
    prev_major_strs = ", ".join(map(str, prev_majors))
    print(
        "Previous majors for `{}` were: {}. Pulling now...".format(
            channel, prev_majors
        )
    )

    return prev_major_strs


def pull_model_data_(bq_read_fn, channel, n_majors, bq_loc):
    prev_major_strs = pull_model_data_pre_query(
        bq_read_fn, channel, n_majors, bq_loc
    )

    pull_all_recent_query = f"""
    select * from {bq_loc.sql}
    where channel = '{channel}'
          and major in ({prev_major_strs})
    """
    print("Running query:\n", pull_all_recent_query)
    df = bq_read_fn(pull_all_recent_query)
    return df


def download_raw_data(
    bq_read_fn,
    channel,
    n_majors: int,
    table,
    dataset="analysis",
    project_id="moz-fx-data-derived-datasets",
    outname=None,
):
    """
    Given a channel and a number of recent major version, return all of the
    uploaded raw mission control v2 data that matches the `n_majors` most
    recent major version of that channel.

    First do this by querying the MC v2 table for what those major versions
    are.

    This will write the file to a temporary feather file and return the
    filename.
    """
    bq_loc = BqLocation(table, dataset=dataset, project_id=project_id)
    df = pull_model_data_(bq_read_fn, channel, n_majors, bq_loc)
    if outname is None:
        outname = tempfile.NamedTemporaryFile(delete=False, mode="w+").name
    df.to_feather(outname)

    print("feather file saved to {}".format(outname))
    return outname
