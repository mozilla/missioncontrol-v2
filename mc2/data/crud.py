# pip
# google-cloud-bigquery
# pandas-gbq
# Optional:
# pip install joblib

from functools import partial
import os
from os.path import abspath, expanduser, exists

import pandas as pd
import fire
from google.oauth2 import service_account  # noqa
from google.cloud import bigquery  # noqa

from download_bq import pull_all_model_data, download_raw_data
from upload_bq import delete_versions, drop_table, upload

# from src.download_bq import pull_all_model_data
# from src.upload_bq import delete_versions, drop_table, upload


# pandas-gbq -> google-cloud-bigquery
#            -> google-auth

# project_id: 'moz-fx-data-derived-datasets'
# bucket: 'moz-fx-data-derived-datasets-analysis'


def get_creds(creds_loc=None):
    if creds_loc is None:
        creds_loc = abspath(
            expanduser(
                "~/repos/moz-fx-data-derived-datasets-46a8a03da99b-wbeard.json"
            )
        )
    else:
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


def main(
    add_schema: bool = False,
    creds_loc=None,
    cache=False,
    table_name="wbeard_crash_rate_raw",
    drop_first=False,
    return_df=False,
):
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
    if return_df:
        return df_all


if __name__ == "__main__":
    fire.Fire({"main": main, "dl_raw": dl_raw})
