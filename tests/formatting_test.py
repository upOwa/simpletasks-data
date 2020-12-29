from simpletasks_data.formatting import formatList, formatMontant


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
