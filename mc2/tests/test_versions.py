from src.versions import find_prev, label_y
from pandas import DataFrame


def mk_dftest():
    data = [
        {"dvers": "67.0b17", "mvers": 67, "bvers": 17},
        {"dvers": "67.0b18", "mvers": 67, "bvers": 18},
        {"dvers": "67.0b19", "mvers": 67, "bvers": 19},
        {"dvers": "68.0b3", "mvers": 68, "bvers": 3},
        {"dvers": "68.0b4", "mvers": 68, "bvers": 4},
        {"dvers": "68.0b5", "mvers": 68, "bvers": 5},
        {"dvers": "68.0b6", "mvers": 68, "bvers": 6},
    ]
    return DataFrame(data)


def test_find_prev():
    df = mk_dftest()

    cur_vers, prev_vers, rest, later_v = find_prev(df, (68, 4))

    assert cur_vers == "68.0b4"
    assert prev_vers == "68.0b3"
    assert set(rest) == {"67.0b17", "67.0b18", "67.0b19"}
    assert set(later_v) == {"68.0b5", "68.0b6"}


def test_find_prev_unordered():
    df = mk_dftest()
    df = df[::-1]
    cur_vers, prev_vers, rest, later_v = find_prev(df, (68, 4))

    assert cur_vers == "68.0b4"
    assert prev_vers == "68.0b3"
    assert set(rest) == {"67.0b17", "67.0b18", "67.0b19"}
    assert set(later_v) == {"68.0b5", "68.0b6"}


def test_label_y():
    df = mk_dftest()
    y = label_y(df, (68, 4))
    should_be = [
        "rest",
        "rest",
        "rest",
        "prev_vers",
        "cur_vers",
        "later",
        "later",
    ]
    assert y.tolist() == should_be

    dfrev = mk_dftest()[::-1]
    yrev = label_y(dfrev, (68, 4))
    assert (
        yrev.tolist()[::-1] == should_be
    ), "Reversing beforehand shouldn't mess things up"


def test_label_y_same_m():
    df = mk_dftest()
    y = label_y(df, (68, 5), same_m=True)
    df["y"] = y
    should_be = [
        "rest",
        "rest",
        "rest",
        "prev_vers",
        "prev_vers",
        "cur_vers",
        "later",
    ]
    assert y.tolist() == should_be

    dfrev = mk_dftest()[::-1]
    yrev = label_y(dfrev, (68, 5), same_m=True)
    assert (
        yrev.tolist()[::-1] == should_be
    ), "Reversing beforehand shouldn't mess things up"


# test_label_y()
# test_find_prev()
# test_find_prev_unordered()
# test_label_y_same_m()
