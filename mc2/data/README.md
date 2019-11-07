# Bigquery download

The main entry point for these python download functions is [crud.main](crud.py) with the following signature.


```python
crud.main(
    add_schema: bool = False,
    creds_loc=None,
    cache=False,
    table_name="wbeard_crash_rate_raw",
    drop_first=False,
)
```

It requires the filepath of the bigquery credentials to be passed as `creds_loc`. If no argument is passed, the program will try to use the `BQCREDS` environment variable.


While debugging, it could be useful to pass
- `cache = True`, so that it will cache the results of bigquery sql pulls with joblib
- `drop_first = True`, to delete the table before uploading the data
- `add_schema = True` to manually specify the table schema after dropping the table
- custom `table_name` not override the production table (currently `missioncontrol_v2_raw_data`)

## Example CLI commands

Process and upload raw data. For debugging (including dropping the test table):
```bash
conda activate mc2
cd mc2
python data/crud.py main \
    --table_name="missioncontrol_v2_raw_data_test" --cache=True \
    --drop_first=True --add_schema=True \
    --creds_loc="<path to bigquery creds>"
```

For the production table, the call would be
```bash
export BQCREDS="<path to bigquery creds>"
conda activate mc2
cd mc2
python data/crud.py main --table_name="missioncontrol_v2_raw_data"
```

Download data after it's been processed and uploaded
```bash
python data/crud.py dl_raw --channel release --n_majors 3 \
    --creds_loc "~/creds.json" --outname '/tmp/out.fth' \
    --cache True
```


# Strategy
Most of the download functionality is in `download_bq.py`. The overall strategy to download everything is to
- Download release data from [product details](https://product-details.mozilla.org/1.0/firefox.json) to get basic dates and release version data
- Use information from product details to fill in the sql template string that pulls
    - DAU and active hours data from cliens_daily
    - crash data from `telemetry.crash` pings
    - most of this funtionality is channel specific, in functions named `prod_det_process_{channel}()`
    - functions that fill in the sql template have names patterned as `query_from_row_{channel}`
- For beta, also download build information from buildhub. We can't get version information from cliens_daily, so we need to get the version -> build_id information from buildhub, and query those build_id's from cliens_daily.


# Download after updating table

Use `download_bq.pull_model_data(bq_read_fn, channel, n_majors: int, analysis_table)` to download all the data in the mission control table that correspond to the channel and `n_majors` most recent major versions.
