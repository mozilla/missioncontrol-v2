import subprocess
import tempfile

import numpy as np
from pandas import Series


def delete_versions_OLD(df, query_func, table_name="wbeard_crash_rate_raw"):
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
        print("Executing `{}`...".format(q), end="")
        query_func(q)
        print(" Done.")


def delete_versions(df, query_func, table_name="wbeard_crash_rate_raw"):
    """
    @df: DataFrame[['channel', 'c_version', 'date']]
    Drop all unique ('channel', 'c_version', 'date') values in `table_name`. To be used
    before upload of new data.
    """
    df = df[["channel", "c_version", "date"]]
    q_temp = (
        "delete from analysis.{table_name} "
        "where c_version='{c_version}' "
        "and channel='{channel}' and date in ({dates})"
    )
    for (channel, c_version), gdf in (
        df[["channel", "c_version", "date"]]
        .drop_duplicates()
        .groupby(["channel", "c_version"])
    ):
        dates = [d.strftime("%Y-%m-%d") for d in gdf.date]
        dates_str = ", ".join(map("'{}'".format, dates))

        q = q_temp.format(
            table_name=table_name,
            channel=channel,
            c_version=c_version,
            dates=dates_str,
        )
        print("Executing `{}`...".format(q), end="")
        query_func(q)
        print(" Done.")


def get_schema(df, as_str=False):
    dtype_srs = df.dtypes
    dtype_srs.loc[dtype_srs == "category"] = "STRING"
    dtype_srs.loc[dtype_srs == "float64"] = "FLOAT64"
    dtype_srs.loc[dtype_srs == np.int] = "INT64"
    dtype_srs.loc[dtype_srs == object] = "STRING"
    dtype_srs.loc[dtype_srs == bool] = "BOOL"
    manual_dtypes = dict(
        date="DATE", c_version_rel="DATE", major="INT64", minor="INT64"
    )
    dtype_srs.update(Series(manual_dtypes))

    non_strings = dtype_srs.map(type).pipe(lambda x: x[x != str])
    if len(non_strings):
        raise ValueError(
            "Schema values should be strings: {}".format(non_strings)
        )
    if not as_str:
        return dtype_srs
    res = ",".join(["{}:{}".format(c, t) for c, t in dtype_srs.items()])
    return res


def drop_table(table_name="wbeard_crash_rate_raw"):
    cmd = ["bq", "rm", "-f", "-t", "analysis.{}".format(table_name)]
    print("running command", cmd)
    run_command(cmd, "Success! Table {} dropped.".format(table_name))


def upload(df, table_name="wbeard_crash_rate_raw", add_schema=False):
    with tempfile.NamedTemporaryFile(delete=False, mode="w+") as fp:
        df.to_csv(fp, index=False, na_rep="NA")
    print("CSV saved to {}".format(fp.name))

    full_table_name = "analysis.{}".format(table_name)
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
        full_table_name,
        fp.name,
    ]
    if add_schema:
        schema = get_schema(df, True)
        cmd.append(schema)

    success_msg = "Success! Data uploaded to {}".format(full_table_name)
    run_command(cmd, success_msg)


def run_command(cmd, success_msg="Success!"):
    """
    @cmd: List[str]
    No idea why this isn't built into python...
    """
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
