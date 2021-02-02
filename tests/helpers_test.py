import pytest

from simpletasks_data.helpers import cell2coords, col2num, num2col, range2tab


def test_col2num() -> None:
    assert col2num("A") == 0
    assert col2num("Z") == 25
    assert col2num("AA") == 26


def test_num2col() -> None:
    assert num2col(1) == "A"
    assert num2col(26) == "Z"
    assert num2col(27) == "AA"


def test_cell2coords() -> None:
    assert cell2coords("A1") == (0, 0)
    assert cell2coords("AA5") == (26, 4)
    assert cell2coords("Z26") == (25, 25)
    assert cell2coords("A") == (0, None)
    assert cell2coords("AA") == (26, None)
    assert cell2coords("1") == (None, 0)

    with pytest.raises(ValueError) as e:
        cell2coords("1A")
    assert str(e.value) == "Could not parse range 1A"


def test_range2tab() -> None:
    assert range2tab("A1:A") == (0, 0, 0, None)
    assert range2tab("A1:A5") == (0, 0, 0, 4)
    assert range2tab("A1:B") == (0, 0, 1, None)
    assert range2tab("A1:B5") == (0, 0, 1, 4)
    assert range2tab("A1:*") == (0, 0, None, None)

    with pytest.raises(ValueError) as e:
        range2tab("*:A")
    assert str(e.value) == "Could not parse range *"

    with pytest.raises(ValueError) as e:
        range2tab("AB")
    assert str(e.value) == "Could not parse range AB"

    with pytest.raises(ValueError) as e:
        range2tab("A:B:C")
    assert str(e.value) == "Could not parse range A:B:C"

    with pytest.raises(ValueError) as e:
        range2tab("A:*")
    assert str(e.value) == "Could not parse range A:*"

    with pytest.raises(ValueError) as e:
        range2tab("1:*")
    assert str(e.value) == "Could not parse range 1:*"
