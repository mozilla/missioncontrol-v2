import subprocess
import tempfile

import numpy as np
import pandas as pd
from pandas import Series


def dates_to_sql_str(dates):
    """
    Format dates for SQL templates

    >>> from pandas import Timestamp
    >>> dates_to_sql_str([Timestamp('2019-10-01 00:00:00'), Timestamp('2019-10-02 00:00:00')])
    => "'2019-10-01', '2019-10-02'"
    """
    dates = [d.strftime("%Y-%m-%d") for d in dates]
    return ", ".join(map("'{}'".format, dates))


def check_job_done(job):
    """
    Bigquery jobs are usually async. This will block
    until they're done.
    """
    for i in job:
        break
    assert job.done(), "Uh oh, job not done??"
    return job


def delete_model_versions(
    df,
    query_func,
    table_name,
    project_id="moz-fx-data-shared-prod",
    dataset="analysis",
):
    """
    Drop rows from model table with same model_date
    before upload of new data.
    """
    sql_dataset_name = "`{}`.{}".format(project_id, dataset)
    del dataset
    if not check_table_exists(
        query_func, table_name, sql_dataset_name=sql_dataset_name
    ):
        print("Table does not yet exist. Not dropping rows")
        return
    q_temp = """
    delete from {sql_dataset_name}.{table_name}
    where model_date in ({model_dates})
    """
    mdates_str = (
        df.model_date.pipe(pd.to_datetime)
        .drop_duplicates()
        .pipe(dates_to_sql_str)
    )
    q = q_temp.format(
        sql_dataset_name=sql_dataset_name,
        table_name=table_name,
        model_dates=mdates_str,
    )
    print("Executing {}".format(q), end="\n...")
    query_func(q)
    print(" Done.")


def delete_versions(df, query_func, table_name="wbeard_crash_rate_raw"):
    """
    @df: DataFrame[['channel', 'c_version', 'date']]
    Drop all unique ('channel', 'c_version', 'date') values in `table_name`. To be used
    before upload of new data.
    """
    if not check_table_exists(query_func, table_name):
        print("Table does not yet exist. Not dropping rows")
        return
    df = df[["channel", "c_version", "date"]]
    q_temp = (
        "delete from `moz-fx-data-derived-datasets`.analysis.{table_name} "
        "where c_version='{c_version}' "
        "and channel='{channel}' and date in ({dates})"
    )
    for (channel, c_version), gdf in (
        df[["channel", "c_version", "date"]]
        .drop_duplicates()
        .groupby(["channel", "c_version"])
    ):
        dates_str = dates_to_sql_str(gdf.date)

        q = q_temp.format(
            table_name=table_name,
            channel=channel,
            c_version=c_version,
            dates=dates_str,
        )
        print("Executing `{}`...".format(q), end="")
        query_func(q)
        print(" Done.")


def make_model_upload_cmd(
    json_fname, full_table_name_noticks, schema, overwrite: bool = False
):
    # True => 'true'
    replace_val = str(bool(overwrite)).lower()
    cmds = [
        "bq",
        "load",
        "--replace=" + replace_val,
        "--project_id=moz-fx-data-bq-data-science",
        "--source_format=NEWLINE_DELIMITED_JSON",
        full_table_name_noticks,
        json_fname,
        schema,
    ]
    return cmds


def process_model_df(df):
    def check_name(col):
        err_msg = (
            "Column name {} has wrong format. Should have `_` instead of `.`"
        )
        assert "." not in col, err_msg.format(col)

        return col

    assert "model_date" in df, "Model file missing `model_date` column"
    return df.rename(columns=check_name).assign(
        date=lambda x: x.date.astype(str),
        major=lambda x: x.major.astype(int),
        minor=lambda x: x.minor.astype(int),
    )


def run_model_upload(
    query_func,
    feather_fname,
    json_fname=None,
    table_name="missioncontrol_v2_model_output_test",
    project_id="moz-fx-data-shared-prod",
    dataset="analysis",
    overwrite=False,
):
    """
    Upload dataframe saved as feather file to `feather_fname`
    up to BQ. Delete any rows if they have the same model date.
    """
    table_name_noticks = "{proj}:{dataset}.{table}".format(
        proj=project_id, dataset=dataset, table=table_name
    )

    # Read, save df as json
    df = pd.read_feather(feather_fname).pipe(process_model_df)
    if json_fname is None:
        json_fname = tempfile.NamedTemporaryFile(delete=False, mode="w+").name
    print("Writing to temp file {}".format(json_fname))
    df.to_json(json_fname, orient="records", lines=True)

    # Generate schema
    schema = get_schema(df, as_str=True, model_date="DATE")
    load_cmd = make_model_upload_cmd(
        json_fname=json_fname,
        full_table_name_noticks=table_name_noticks,
        schema=schema,
        overwrite=overwrite,
    )

    # Delete potentially redundant rows
    delete_model_versions(
        df,
        query_func,
        table_name=table_name,
        project_id=project_id,
        dataset=dataset,
    )

    print("Uploading to {}".format(table_name_noticks))
    res = run_command(load_cmd)
    return res


def check_table_exists(
    query_func,
    table_name,
    sql_dataset_name="`moz-fx-data-derived-datasets`.analysis",
):
    q = """
    SELECT count(*) as exist FROM {dataset_name}.__TABLES__
    WHERE table_id='{table_name}'
    """.format(
        dataset_name=sql_dataset_name, table_name=table_name
    )
    [row] = query_func(q)
    [exists] = row.values()
    return bool(exists)


def get_schema(df, as_str=False, **override):
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
    dtype_srs.update(Series(override))
    missing_override_keys = set(override) - set(dtype_srs.index)
    if missing_override_keys:
        raise ValueError("Series missing keys {}".format(missing_override_keys))

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
    cmd = [
        "bq",
        "rm",
        "-f",
        "-t",
        "moz-fx-data-derived-datasets.analysis.{}".format(table_name),
    ]
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
