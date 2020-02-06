import os
from functools import partial
from os.path import abspath, exists, expanduser

import fire  # type: ignore
import pandas as pd  # type: ignore
import release_versions as rv
from bq_utils import BqLocation
from download_bq import download_raw_data, pull_all_model_data
from google.cloud import bigquery  # type: ignore
from google.oauth2 import service_account  # type: ignore
from upload_bq import delete_versions, drop_table, run_model_upload, upload
import schema


# project_id: 'moz-fx-data-derived-datasets'
# bucket: 'moz-fx-data-derived-datasets-analysis'


def assert_eq_collections(s1, s2):
    s1 = set(s1)
    s2 = set(s2)
    errs = []
    s1_extra = s1 - s2
    if s1_extra:
        errs.append(f"First collection has extra elems {s1_extra}")
    s1_msg = s2 - s1
    if s1_msg:
        errs.append(f"First collection has extra elems {s1_msg}")
    if not errs:
        return
    raise AssertionError(". ".join(errs))


def strong_bool(b):
    """
    Make sure we don't accidentally pass a false string
    in from bash that gets interpreted as true.
    """
    if isinstance(b, bool):
        return b
    bool_map = {"True": True, "False": False}
    res = bool_map.get(b, None)
    if res is None:
        raise ValueError("Non-boolean value {} was passed".format(b))
    return res


def get_creds(creds_loc=None):
    if not creds_loc:
        creds_loc = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not creds_loc:
            raise RuntimeError(
                "Bigquery credentials not passed, or found in environment."
            )

    creds_loc = abspath(expanduser(creds_loc))
    creds = service_account.Credentials.from_service_account_file(creds_loc)
    return creds


def mk_bq_reader(
    creds_loc=None, cache=False, base_project_id="moz-fx-data-derived-datasets"
):
    """
    Returns function that takes a BQ sql query and
    returns a pandas dataframe
    """
    creds = get_creds(creds_loc=creds_loc)

    bq_read = partial(
        pd.read_gbq,
        project_id=base_project_id,
        credentials=creds,
        dialect="standard",
    )
    if cache:
        fn = cache_reader(bq_read)
        loc = os.path.abspath(fn.store_backend.location)
        print("Caching sql results to {}".format(loc))
        return fn
    return bq_read


def cache_reader(bq_read):
    from joblib import Memory  # type: ignore

    if not exists("cache"):
        os.mkdir("cache")
    mem = Memory(cachedir="cache", verbose=0)

    @mem.cache
    def bq_read_cache(*a, **k):
        return bq_read(*a, **k)

    return bq_read_cache


def mk_query_func(creds_loc=None):
    """
    This function will block until the job is done...and
    take a while if a lot of queries are repeatedly made.
    """
    creds = get_creds(creds_loc=creds_loc)
    client = bigquery.Client(project=creds.project_id, credentials=creds)

    def blocking_query(*a, **k):
        job = client.query(*a, **k)
        for i in job:
            break
        assert job.done(), "Uh oh, job not done??"
        return job

    return blocking_query


def mk_query_func_async(creds_loc=None):
    creds = get_creds(creds_loc=creds_loc)
    client = bigquery.Client(project=creds.project_id, credentials=creds)
    return client.query


def dl_raw(
    channel,
    n_majors: int,
    creds_loc=None,
    table="missioncontrol_v2_raw_data",
    dataset="analysis",
    project_id="moz-fx-data-derived-datasets",
    outname=None,
    cache=False,
    base_project_id="moz-fx-data-derived-datasets",
):
    """
    Wrapper for download_bq.download_raw_data(). After running `main`,
    this will download rows corresponding to the specified `channel`
    and save them to a feather format file.

    The `base_project_id` param is used to generate the query client.
    """
    bq_read_fn = mk_bq_reader(
        creds_loc=creds_loc, cache=cache, base_project_id=base_project_id
    )
    fname = download_raw_data(
        bq_read_fn,
        channel,
        n_majors,
        table=table,
        dataset=dataset,
        project_id=project_id,
        outname=outname,
    )
    return fname


def upload_model_data(
    feather_fname,
    creds_loc=None,
    json_fname=None,
    table_name="missioncontrol_v2_model_output",
    project_id="moz-fx-data-shared-prod",
    dataset="analysis",
    overwrite=False,
):
    query_func = mk_query_func(creds_loc=creds_loc)
    bq_loc = BqLocation(table_name, dataset=dataset, project_id=project_id)
    run_model_upload(
        query_func,
        feather_fname=feather_fname,
        bq_loc=bq_loc,
        json_fname=json_fname,
        overwrite=overwrite,
    )
    print_rows_loc(bq_loc=bq_loc, creds_loc=creds_loc)


def print_rows_dau(bq_loc: BqLocation, creds_loc=None):
    bq_read_no_cache = mk_bq_reader(
        creds_loc=creds_loc, base_project_id=bq_loc.project_id, cache=False
    )
    summary = bq_read_no_cache(
        "select count(*) as n_rows, avg(dau_cversion) / 1e6"
        f" as dau_cversion_mm from {bq_loc.sql}"
    )
    n_rows = summary.iloc[0, 0]
    print(summary)
    print(f"=> {bq_loc.sql} now has {n_rows} rows")


def print_rows_loc(bq_loc: BqLocation, creds_loc=None):
    bq_read_no_cache = mk_bq_reader(
        creds_loc=creds_loc,
        base_project_id="moz-fx-data-bq-data-science",
        cache=False
        # creds_loc=creds_loc, base_project_id=bq_loc.project_id, cache=False
    )
    summary = bq_read_no_cache(f"select count(*) as n_rows from {bq_loc.sql}")
    n_rows = summary.iloc[0, 0]
    print(f"=> {bq_loc.sql} now has {n_rows} rows")


def main(
    add_schema: bool = False,
    creds_loc=None,
    cache: bool = False,
    project_id="moz-fx-data-derived-datasets",
    dataset="analysis",
    table_name="wbeard_crash_rate_raw",
    drop_first: bool = False,
    return_df: bool = False,
    force: bool = False,
    skip_delete: bool = False,
    add_fake_columns: bool = True,
    sub_date=None,
    # ESR params
    esr=False,
):
    """
    Process and upload raw data. For debugging (including dropping the test
    table):
    python data/crud.py main \
        --table_name="missioncontrol_v2_raw_data_test" --cache=True \
        --drop_first=True --add_schema=True \
        --creds_loc="<path to bigquery creds>"

    To drop the production table, pass `--force=True`.
    When called from the command line, it won't return anything by default,
    but if calling as a python function, passing `return_df` will have it
    return the resulting dataframe.

    `add_fake_columns`: add plugin columns. Interrim solution to keep the schema
    consistent until it's finalized.
    `sub_date` pulls for nightly and release as if it were today. Format should
        be the standard bigquery format 'YYYY-mm-dd'
    """
    if sub_date:
        rv.validate_sub_date_str(sub_date)
    add_schema, cache, drop_first, return_df, force = map(
        strong_bool, [add_schema, cache, drop_first, return_df, force]
    )
    bq_loc = BqLocation(table_name, dataset=dataset, project_id=project_id)
    if table_name == "missioncontrol_v2_raw_data":
        if cache:
            print(
                "WARNING--CACHE TURNED ON. YOU SHOULD DISABLE UNLESS DEBUGGING"
            )
        if drop_first and not force:
            raise ValueError(
                f"If you really want to drop {table_name}, pass force == True"
            )
    if cache:
        print("Cache turned on.")
    else:
        print("Not using cached queries")
    if drop_first:
        drop_table(table_name=table_name)
    bq_read = mk_bq_reader(
        creds_loc=creds_loc, base_project_id=project_id, cache=cache
    )
    query_func = mk_query_func(creds_loc=creds_loc)

    print("Starting data pull")
    df_all = pull_all_model_data(bq_read, sub_date_str=sub_date, esr=esr)

    if add_fake_columns:
        # First, give dummy values for plugin columns that are no longer
        # needed; they need to be correctly ordered or bq chokes on the new
        # schema
        new_cols = [
            ("dau_cp_crasher_cversion", 8, 0),
            ("usage_cp_crasher_cversion", 12, 0.0),
            ("cplugin", 16, 0),
        ]

        for col, loc, val in new_cols:
            df_all.insert(loc, col, val)

    assert_eq_collections(schema.raw_col_order, df_all)
    df_all = df_all[schema.raw_col_order]

    if not skip_delete:
        delete_versions(df_all, query_func=query_func, bq_loc=bq_loc)
    upload(
        df_all,
        project_id=project_id,
        table_name=table_name,
        add_schema=add_schema,
    )

    # Double check: print how many rows
    print_rows_dau(bq_loc=bq_loc, creds_loc=creds_loc)

    if return_df:
        return df_all


if __name__ == "__main__":
    fire.Fire(
        {"main": main, "dl_raw": dl_raw, "upload_model_data": upload_model_data}
    )
