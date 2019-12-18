import buildhub_bid as bh
import pandas as pd  # type: ignore


def read_product_details():
    pd_url = "https://product-details.mozilla.org/1.0/firefox.json"
    js = pd.read_json(pd_url)
    df = (
        pd.DataFrame(js.releases.tolist())
        .assign(release_label=js.index.tolist())
        .assign(date=lambda x: pd.to_datetime(x.date))
    )
    return df


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
    return bh.version2df(
        beta_docs,
        keep_rc=False,
        keep_release=True,
    ).assign(chan="beta")


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
