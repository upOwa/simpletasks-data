import datetime

from simpletasks_data.formatting import (
    create_str2strnullable,
    dump_datetime,
    dump_int,
    dump_percentage,
    dump_str,
    formatList,
    formatMontant,
    parseShittyDate,
    parseShittyDateAsDate,
    parseShittyDateTime,
    parseShittyTime,
    str2bool,
    str2boolnullable,
    str2floatamount,
    str2floatnullable,
    str2intamount,
    str2intnullable,
    str2strnullable,
    to_list,
)


def test_dump_datetime() -> None:
    d = datetime.date(2020, 1, 2)
    dt = datetime.datetime(2020, 1, 2, 3, 4, 5)

    assert dump_datetime(None) is None
    assert dump_datetime(d) == "2020-01-02T00:00:00"
    assert dump_datetime(dt) == "2020-01-02T03:04:05"

    assert dump_datetime(d, "json") == "2020-01-02T00:00:00"
    assert dump_datetime(dt, "json") == "2020-01-02T03:04:05"

    assert dump_datetime(d, "gs") == "2020-01-02 00:00:00"
    assert dump_datetime(dt, "gs") == "2020-01-02 03:04:05"

    assert dump_datetime(d, "%d/%m/%Y") == "02/01/2020"
    assert dump_datetime(dt, "%d/%m/%Y") == "02/01/2020"


def test_dump_int() -> None:
    assert dump_int(None) == "0"
    assert dump_int(0) == "0"
    assert dump_int(42) == "42"
    assert dump_int(0.45) == "0"


def test_dump_percentage() -> None:
    assert dump_percentage(None) == "0.0"
    assert dump_percentage(0) == "0.0"
    assert dump_percentage(1) == "100.0"
    assert dump_percentage(0.456) == "45.6"
    assert dump_percentage(0.45678) == "45.7"


def test_dump_str() -> None:
    assert dump_str(None) == "-"
    assert dump_str("") == "-"
    assert dump_str("    ") == "-"
    assert dump_str("1") == "1"


def test_formatList() -> None:
    assert formatList(None) == []
    assert formatList("") == []
    assert formatList("val1") == ["val1"]
    assert formatList('"val1"') == ["val1"]
    assert formatList("'val1'") == ["val1"]
    assert formatList("val1; val2 et val3") == ["val1", "val2", "val3"]
    assert formatList("val1, val2 et val3") == ["val1", "val2", "val3"]
    assert formatList("val1, val2; val3 et val4") == ["val1", "val2", "val3", "val4"]
    assert formatList(" val1 , val2 et val3 ") == ["val1", "val2", "val3"]
    assert formatList(" ,val1 , val2 et val3, ") == ["val1", "val2", "val3"]
    assert formatList("val1 (1,2,3), val2 et val3") == ["val1 (1,2,3)", "val2", "val3"]
    assert formatList('val1 (1,2,3), "val2 et val3"') == ["val1 (1,2,3)", "val2, val3"]
    assert formatList("caractères, accentués") == ["caractères", "accentués"]


def test_formatMontant() -> None:
    assert formatMontant(None) == "0"
    assert formatMontant("") == "0"
    assert formatMontant("-") == "0"
    assert formatMontant("(1,000.0)") == "-1000.0"
    assert formatMontant("-1,000.0") == "-1000.0"
    assert formatMontant("-1 000,0", "fr_FR") == "-1000.0"
    assert formatMontant(" ") == "0"


def test_str2bool() -> None:
    assert str2bool("") is False
    assert str2bool(" ") is False

    assert str2bool("YES") is True
    assert str2bool("TRUE") is True
    assert str2bool("NO") is False
    assert str2bool("FALSE") is False


def test_str2boolnullable() -> None:
    assert str2boolnullable("") is None
    assert str2boolnullable(" ") is False

    assert str2boolnullable("YES") is True
    assert str2boolnullable("TRUE") is True
    assert str2boolnullable("NO") is False
    assert str2boolnullable("FALSE") is False


def test_str2strnullable() -> None:
    assert str2strnullable("") is None
    assert str2strnullable(" ") is None

    assert str2strnullable("foobar", None) == "foobar"
    assert str2strnullable("foobar", 2) == "fo"
    assert str2strnullable("  foobar  ", None) == "foobar"
    assert str2strnullable("  foobar  ", 2) == "fo"


def test_create_str2strnullable() -> None:
    o = create_str2strnullable(None)
    p = create_str2strnullable(2)

    assert o("") is None
    assert o(" ") is None

    assert o("foobar") == "foobar"
    assert p("foobar") == "fo"
    assert o("  foobar  ") == "foobar"
    assert p("  foobar  ") == "fo"


def test_str2intnullable() -> None:
    assert str2intnullable("") is None
    assert str2intnullable("42") == 42
    assert str2intnullable("42.56") is None
    assert str2intnullable("foobar") is None


def test_str2intamount() -> None:
    assert str2intamount("") == 0
    assert str2intamount("42") == 42
    assert str2intamount("-1,000") == -1000
    assert str2intamount("-1,000.0") == 0


def test_str2floatnullable() -> None:
    assert str2floatnullable("") is None
    assert str2floatnullable("42") == 42.0
    assert str2floatnullable("42.56") == 42.56
    assert str2floatnullable("foobar") is None


def test_str2floatamount() -> None:
    assert str2floatamount("") == 0.0
    assert str2floatamount("42") == 42.0
    assert str2floatamount("-1,000") == -1000.0
    assert str2floatamount("-1,000.0") == -1000.0


def test_parseShittyDate() -> None:
    dt = datetime.datetime(2020, 1, 2)
    dt2 = datetime.datetime(2020, 1, 24)

    assert parseShittyDate("") is None

    assert parseShittyDate("02/01/2020") == dt
    assert parseShittyDate("02.01.2020") == dt
    assert parseShittyDate("01/02/2020", "en_US") == dt
    assert parseShittyDate("01.02.2020", "en_US") == dt
    assert parseShittyDate("2020-01-02") == dt

    assert parseShittyDate("01/24/2020") == dt2
    assert parseShittyDate("01.24.2020") == dt2
    assert parseShittyDate("24/01/2020", "en_US") == dt2
    assert parseShittyDate("24.01.2020", "en_US") == dt2
    assert parseShittyDate("2020-01-24") == dt2

    assert parseShittyDate("10000-01-01") == datetime.datetime.max
    assert parseShittyDate("10000-01-01 0:00:00") == datetime.datetime.max


def test_parseShittyDateAsDate() -> None:
    dt = datetime.date(2020, 1, 2)
    dt2 = datetime.date(2020, 1, 24)

    assert parseShittyDateAsDate("") is None

    assert parseShittyDateAsDate("02/01/2020") == dt
    assert parseShittyDateAsDate("02.01.2020") == dt
    assert parseShittyDateAsDate("01/02/2020", "en_US") == dt
    assert parseShittyDateAsDate("01.02.2020", "en_US") == dt
    assert parseShittyDateAsDate("2020-01-02") == dt

    assert parseShittyDateAsDate("01/24/2020") == dt2
    assert parseShittyDateAsDate("01.24.2020") == dt2
    assert parseShittyDateAsDate("24/01/2020", "en_US") == dt2
    assert parseShittyDateAsDate("24.01.2020", "en_US") == dt2
    assert parseShittyDateAsDate("2020-01-24") == dt2

    assert parseShittyDateAsDate("10000-01-01") == datetime.date.max
    assert parseShittyDateAsDate("10000-01-01 0:00:00") == datetime.date.max


def test_parseShittyDateTime() -> None:
    dt = datetime.datetime(2020, 1, 2, 3, 4, 5)
    dt2 = datetime.datetime(2020, 1, 24, 3, 4, 5)

    assert parseShittyDateTime("") is None

    assert parseShittyDateTime("02/01/2020 03:04:05", "fr_FR") == dt
    assert parseShittyDateTime("02.01.2020 03:04:05", "fr_FR") == dt
    assert parseShittyDateTime("01/02/2020 03:04:05") == dt
    assert parseShittyDateTime("01.02.2020 03:04:05") == dt
    assert parseShittyDateTime("2020-01-02 03:04:05", "fr_FR") == dt

    assert parseShittyDateTime("01/24/2020 03:04:05", "fr_FR") == dt2
    assert parseShittyDateTime("01.24.2020 03:04:05", "fr_FR") == dt2
    assert parseShittyDateTime("24/01/2020 03:04:05") == dt2
    assert parseShittyDateTime("24.01.2020 03:04:05") == dt2
    assert parseShittyDateTime("2020-01-24 03:04:05", "fr_FR") == dt2

    assert parseShittyDateTime("10000-01-01 0:00:00") == datetime.datetime.max


def test_parseShittyTime() -> None:
    dt = datetime.timedelta(hours=1, minutes=2, seconds=3)

    assert parseShittyTime("") is None

    assert parseShittyTime("01:02:03") == dt


def test_to_list() -> None:
    assert to_list([]) == []
    assert to_list([None, False, True, "1", "", None]) == [True, "1"]
