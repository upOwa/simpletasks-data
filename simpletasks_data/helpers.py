import re
import string
from typing import Optional, Tuple


def col2num(col: str) -> int:
    """Returns the column number (starting from 0) corresponding to the name.

    Example: column "A" gives 0, "B" gives 1

    Args:
    - col (str): Name of the column (e.g. AG)

    Returns:
    - int: Number of the column (starting from 0)
    """
    num = 0
    for c in col:
        if c in string.ascii_letters:
            num = num * 26 + (ord(c.upper()) - ord("A")) + 1
    return num - 1


def num2col(n: int) -> str:
    """Returns the column name corresponding to a column number (1-based).

    Taken from https://stackoverflow.com/a/23862195/1795676

    Example: 1 gives "A"

    Args:
    - n (int): Column number (starting from 1)

    Returns:
    - str: Column name
    """
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string


def cell2coords(cell: str) -> Tuple[Optional[int], Optional[int]]:
    """Returns coordinates of a cell from its A1N1 notation

    Args:
    - cell (str): Cell reference (e.g. A1, or A, or 1)

    Raises:
    - ValueError: Range could not be parsed

    Returns:
    - Tuple[Optional[int], Optional[int]]: Tuple of (column, row)
    """
    coords = re.search(r"^([A-Z]*)(\d*)$", cell)

    if not coords:
        raise ValueError("Could not parse range {}".format(cell))

    column = col2num(coords.group(1)) if coords.group(1) != "" else None
    row = (int(coords.group(2)) - 1) if coords.group(2) != "" else None

    return (column, row)


def range2tab(range: str) -> Tuple[int, int, Optional[int], Optional[int]]:
    """Takes a range as a string in A1N1 notation (e.g. A1:R) and returns the coordinates of the range
    (usable for low-level operations, see operations module)

    Args:
    - range (str): Range in A1N1 notation (e.g. A1:G) - accepts wildcard for end of range (e.g. A1:*)

    Raises:
    - ValueError: Could not parse range

    Returns:
    - Tuple[int, int, Optional[int]]: Tuple of:
        - Column of the start of the range (starting from 0)
        - Row of the start of the range (starting from 0)
        - Column of the end of the range (or None if wildcard)
        - Row of the end of the range (or None if wildcard)
    """

    extremes = range.split(":")

    if len(extremes) != 2:
        raise ValueError("Could not parse range {}".format(range))

    columnIndexStart, rowIndexStart = cell2coords(extremes[0])
    if columnIndexStart is None or rowIndexStart is None:
        raise ValueError("Could not parse range {}".format(range))

    if extremes[1] != "*":
        columnIndexEnd, rowIndexEnd = cell2coords(extremes[1])
    else:
        columnIndexEnd = None
        rowIndexEnd = None

    return columnIndexStart, rowIndexStart, columnIndexEnd, rowIndexEnd
