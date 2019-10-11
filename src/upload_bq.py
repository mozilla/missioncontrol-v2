import subprocess
import tempfile

import numpy as np


def delete_versions(df, query_func, table_name="wbeard_crash_rate_raw"):
    """
    @df: DataFrame[[major, channel]]
    Drop all unique (major, channel) values in `table_name`. To be used
    before upload of new data.
    """
    q_temp = (
        "delete from analysis.{table_name} where major={major:.0f} and channel='{channel}'"
    )
    for major, channel in (
        df[["major", "channel"]].drop_duplicates().itertuples(index=False)
    ):
        q = q_temp.format(table_name=table_name, major=major, channel=channel)
        print("Executing", q, "...", end=" ")
        query_func(q)
        print("Done.")


def get_schema(df, as_str=False):
    dtypes = df.dtypes
    dtypes["date"] = "DATE"
    dtypes.loc[dtypes == "category"] = "STRING"
    dtypes.loc[dtypes == "float64"] = "FLOAT64"
    dtypes.loc[dtypes == np.int32] = "INT64"
    dtypes.loc[dtypes == object] = "STRING"
    dtypes.loc[dtypes == bool] = "BOOL"
    if not as_str:
        return dtypes
    res = ["{}:{}".format(c, t) for c, t in dtypes.iteritems()]
    res = ",".join(res)
    return res


def drop_table(table_name="wbeard_crash_rate_raw"):
    cmd = ["bq", "rm", "-t", "analysis.{}".format(table_name)]
    print("running command", cmd)
    run_command(cmd, "Success! Table {} dropped.".format(table_name))


def upload(df, table_name="wbeard_crash_rate_raw", add_schema=False):
    with tempfile.NamedTemporaryFile(delete=False, mode="w+") as fp:
        df.to_csv(fp, index=False, na_rep="NA")

    cmd = [
        "bq",
        "load",
        "--noreplace",
        "--project_id",
        "moz-fx-data-derived-datasets",
        "--source_format",
        "CSV",
        "--skip_leading_rows",
        "1",
        "--null_marker",
        "NA",
        "analysis.wbeard_crash_rate_raw",
        "/var/folders/9c/51dwz9bj2bv0txl75ldhxnq40000gn/T/tmpk_fwhg92",
    ]
    if add_schema:
        schema = get_schema(df, True)
        cmd.append(schema)

    success_msg = "Success! Data uploaded to {}".format(table_name)
    run_command(cmd, success_msg)


def run_command(cmd, success_msg="Success!"):
    "No idea why this isn't built into python..."
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode()
        success = True
    except subprocess.CalledProcessError as e:
        output = e.output.decode()
        success = False

    if success:
        print(success_msg)
        print(output)
    else:
        print("Command Failed")
        print(output)


# def upload(df, table_name="wbeard_crash_rate_raw", add_schema=False):
#     cmd = mk_upload_cmd(df, table_name, add_schema=add_schema)
#     success_msg = "Success! Data uploaded to {}".format(table_name)
#     run_command(cmd, success_msg)


# delete_versions(df, query_func, table_name="wbeard_crash_rate_raw")
# upload(df, table_name="wbeard_crash_rate_raw", add_schema=False)
