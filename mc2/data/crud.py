# pip
# google-cloud-bigquery
# pandas-gbq
# Optional:
# pip install joblib

import os
from functools import partial
from os.path import abspath, exists, expanduser

import fire
import pandas as pd
from download_bq import download_raw_data, pull_all_model_data
from google.cloud import bigquery  # noqa
from google.oauth2 import service_account  # noqa
from upload_bq import delete_versions, drop_table, run_model_upload, upload


# from src.download_bq import pull_all_model_data
# from src.upload_bq import delete_versions, drop_table, upload


# pandas-gbq -> google-cloud-bigquery
#            -> google-auth

# project_id: 'moz-fx-data-derived-datasets'
# bucket: 'moz-fx-data-derived-datasets-analysis'


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
        creds_loc = os.environ.get("BQCREDS")
        if not creds_loc:
            raise RuntimeError(
                "Bigquery credentials not passed, or found in environment."
            )

    creds_loc = abspath(expanduser(creds_loc))
    creds = service_account.Credentials.from_service_account_file(creds_loc)
    return creds


def mk_bq_reader(creds_loc=None, cache=False):
    """
    Returns function that takes a BQ sql query and
    returns a pandas dataframe
    """
    creds = get_creds(creds_loc=creds_loc)

    bq_read = partial(
        pd.read_gbq,
        project_id="moz-fx-data-derived-datasets",
        # TODO: delete following
        # creds.project_id,
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
    from joblib import Memory  # noqa

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
    analysis_table="missioncontrol_v2_raw_data",
    outname=None,
    cache=False,
):
    """
    Wrapper for download_bq.download_raw_data(). After running `main`,
    this will download rows corresponding to the specified `channel`
    and save them to a feather format file.
    """
    bq_read_fn = mk_bq_reader(creds_loc=creds_loc, cache=cache)
    fname = download_raw_data(
        bq_read_fn,
        channel,
        n_majors,
        analysis_table=analysis_table,
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
    run_model_upload(
        query_func,
        feather_fname=feather_fname,
        json_fname=json_fname,
        table_name=table_name,
        project_id=project_id,
        dataset=dataset,
        overwrite=overwrite,
    )


def main(
    add_schema: bool = False,
    creds_loc=None,
    cache: bool = False,
    table_name="wbeard_crash_rate_raw",
    drop_first: bool = False,
    return_df: bool = False,
    force: bool = False,
):
    """
    Process and upload raw data. For debugging (including dropping the test table):
    python data/crud.py main \
        --table_name="missioncontrol_v2_raw_data_test" --cache=True \
        --drop_first=True --add_schema=True \
        --creds_loc="<path to bigquery creds>"

    To drop the production table, pass `--force=True`.
    When called from the command line, it won't return anything by default,
    but if calling as a python function, passing `return_df` will have it
    return the resulting dataframe.
    """
    add_schema, cache, drop_first, return_df, force = map(
        strong_bool, [add_schema, cache, drop_first, return_df, force]
    )
    if table_name == "missioncontrol_v2_raw_data":
        if cache:
            print(
                "WARNING--CACHE TURNED ON. YOU SHOULD DISABLE UNLESS DEBUGGING"
            )
        if drop_first and not force:
            raise ValueError(
                "If you really want to drop {}, pass force == True".format(
                    table_name
                )
            )
    if cache:
        print("Cache turned on.")
    else:
        print("Not using cached queries")
    if drop_first:
        drop_table(table_name=table_name)
    bq_read = mk_bq_reader(creds_loc=creds_loc, cache=cache)
    query_func = mk_query_func(creds_loc=creds_loc)

    print("Starting data pull")
    df_all = pull_all_model_data(bq_read)

    delete_versions(df_all, query_func, table_name=table_name)
    upload(df_all, table_name=table_name, add_schema=add_schema)

    # Double check: print how many rows
    bq_read_no_cache = mk_bq_reader(creds_loc=creds_loc, cache=False)
    n_rows = bq_read_no_cache(
        "select count(*) from `moz-fx-data-derived-datasets`.analysis.{}".format(
            table_name
        )
    ).iloc[0, 0]
    print("=> {} now has {} rows".format(table_name, n_rows))

    if return_df:
        return df_all


if __name__ == "__main__":
    fire.Fire(
        {"main": main, "dl_raw": dl_raw, "upload_model_data": upload_model_data}
    )
