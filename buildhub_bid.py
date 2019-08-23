from requests import post
import datetime as dt
import itertools as it

uri = "https://buildhub.moz.tools/api/search"


def pull_build_id_docs(min_build_day="20180701"):
    query = {
        "aggs": {
            "buildid": {
                "terms": {
                    "field": "build.id",
                    "size": 100000,
                    "order": {"_term": "desc"},
                },
                "aggs": {"version": {"terms": {"field": "target.version"}}},
            }
        },
        "query": {
            "bool": {
                "filter": [
                    {"term": {"target.platform": "win64"}},
                    {"term": {"target.channel": "beta"}},
                    {"term": {"source.product": "firefox"}},
                    {"range": {"build.id": {"gte": min_build_day}}},
                ]
            }
        },
        "size": 0,
    }
    resp = post(uri, json=query)
    docs = resp.json()["aggregations"]["buildid"]["buckets"]
    return docs


def build_id_versions(
    docs, major_version=None, keep_rc=False, keep_release=False
):
    """
    @major_version: int
    From json results, return build_id, version pairs.
    Some build_id's go to multiple versions (usually rc's or a major version)
    Some versions go to multiple build_id's.
    """

    def major_version_filt(v):
        if major_version is None:
            return True
        return int(v.split(".")[0]) == major_version

    def extract_bid_vers(doc):
        build_id = doc["key"]
        versions = [bucket["key"] for bucket in doc["version"]["buckets"]]
        return [
            (version, build_id)
            for version in versions
            if version_filter(
                version, keep_rc=keep_rc, keep_release=keep_release
            )
            and major_version_filt(version)
        ]

    return [pair for doc in docs for pair in extract_bid_vers(doc)]


def version2build_ids(
    docs, major_version=None, keep_rc=False, keep_release=False
):
    version_build_ids = build_id_versions(
        docs,
        major_version=major_version,
        keep_rc=keep_rc,
        keep_release=keep_release,
    )
    return {
        version: [build_id for v, build_id in build_ids]
        for version, build_ids in it.groupby(version_build_ids, lambda x: x[0])
    }


def version_filter(x, keep_rc=False, keep_release=False):
    if not keep_rc and "rc" in x:
        return False
    if not keep_release and ("b" not in x):
        return False
    return True


def test_version_filter():
    rc_vers = "65.0b6rc"
    assert version_filter(rc_vers, keep_rc=False, keep_release=False) is False
    assert version_filter(rc_vers, keep_rc=True, keep_release=False)

    rls_vers = "65.0"
    assert version_filter(rls_vers, keep_rc=False, keep_release=True)
    assert version_filter(rls_vers, keep_rc=False, keep_release=False) is False

    beta_vers = "65.0b6"
    assert version_filter(beta_vers, keep_rc=False, keep_release=True)
    assert version_filter(beta_vers, keep_rc=False, keep_release=False)


def months_ago(months=12):
    return (dt.date.today() - dt.timedelta(days=30 * months)).strftime("%Y%m%d")


def main(vers=67):
    # result_docs = pull_build_id_docs(min_build_day="20180701")
    result_docs = pull_build_id_docs(min_build_day=months_ago(12))
    res = version2build_ids(
        result_docs, major_version=67, keep_rc=False, keep_release=False
    )
    print(res)


if __name__ == "__main__":
    main()
