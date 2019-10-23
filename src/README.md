# Bigquery download

The main entry point for these python download functions is [crud.main](src/crud.py) with the following signature.


```python
crud.main(
    add_schema: bool = False,
    creds_loc=None,
    cache=False,
    table_name="wbeard_crash_rate_raw",
    drop_first=False,
)
```

It requires the filepath of the bigquery credentials to be passed as `creds_loc`.


While debugging, it could be usefuly to pass
- `cache = True`, so that it will cache the results of bigquery sql pulls with joblib
- `drop_first = True`, to delete the table before uploading the data
- `add_schema = True` to manual specify the table schema after dropping the table
- custom `table_name` to not override the table currently in use


# Strategy
Most of the download functionality is in `download_bq.py`. The overall strategy to download everything is to
- Download release data from [product details](https://product-details.mozilla.org/1.0/firefox.json) to get basic dates and release version data
- Use information from product details to fill in the sql template string that pulls
    - DAU and active hours data from cliens_daily
    - crash data from crash_summary
    - most of this funtionality is channel specific, in functions named `prod_det_process_{channel}()`
    - functions that fill in the sql template have names patterned as `query_from_row_{channel}`
- For beta, also download build information from buildhub. We can't get version information from cliens_daily, so we need to get the version -> build_id information from buildhub, and query those build_id's from cliens_daily.