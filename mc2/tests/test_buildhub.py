from data.buildhub_bid import rc_major_version


def test_rc_major_version():
    assert rc_major_version("71") is None
    assert rc_major_version("71.0") == 71
    assert rc_major_version("71.0b1") is None
    assert rc_major_version("71.0.0") == 71
