import datetime
from typing import Callable, List, Optional, Sequence, TypeVar, Union

try:
    import geoalchemy2

    has_geoalchemy2 = True
except ImportError:
    has_geoalchemy2 = False
import pyparsing
from jinja2.utils import is_undefined


def dump_datetime(value: Optional[Union[datetime.datetime, datetime.date]], fmt="json") -> Optional[str]:
    """Serializes Date and datetime objects into strings for JSON or other formats.

    Supported shorthand formats are:
    - json: "%Y-%m-%dT%H:%M:%S%z", e.g. 2021-02-01T21:49:25+00:0 (compatible ISO 8601)
    - gs: "%Y-%m-%d %H:%M:%S", e.g. 2021-02-01 21:49:25 (recognized by Google Sheets)

    Args:
    - value (Optional[Union[datetime.datetime, datetime.date]]): Value to serialize - if None will return None
    - fmt (str, optional): Format - if not recognized as shorthand format, will pass it to strftime. Defaults to "json".

    Returns:
    - Optional[str]: Serialized string
    """
    if value is None or is_undefined(value):
        return None
    if fmt == "json":
        fmt = "%Y-%m-%dT%H:%M:%S%z"
    elif fmt == "gs":
        fmt = "%Y-%m-%d %H:%M:%S"
    return value.strftime(fmt)


if has_geoalchemy2:

    def dump_gps(coordinates: geoalchemy2.Geography) -> Optional[str]:
        """Serializes a POINT geography into  a lat, lng string

        Args:
        - coordinates (geoalchemy2.Geography): Value to serialize - if None will return None

        Returns:
        - Optional[str]: Serialized string
        """
        if coordinates is None or is_undefined(coordinates):
            return None
        shape = geoalchemy2.shape.to_shape(coordinates)
        return "{:f}, {:f}".format(shape.y, shape.x)


def dump_int(val: Optional[Union[int, float]]) -> str:
    """Serializes an int or float.

    Args:
    - val (Optional[Union[int, float]]): Value to serialize. Returns 0 if None

    Returns:
    - str: String (non-empty)
    """
    if val is None or is_undefined(val):
        return "0"
    return "{:.0f}".format(val)


def dump_percentage(val: Optional[float]) -> str:
    """Serializes a float as a percentage.

    Args:
    - val (Optional[float]): Value to serialize, between 0 and 1. Returns 0 if None.

    Returns:
    - str: String with 1 decimal (non-empty)
    """
    if val is None or is_undefined(val):
        return "0.0"
    return "{:.1f}".format(val * 100)


def dump_str(val: Optional[str]) -> str:
    """Serializes a string.

    Args:
    - val (Optional[str]): Value to serialize. Returns - if None or empty.

    Returns:
    - str: String (non-empty)
    """
    if val is None or is_undefined(val) or val.strip() == "":
        return "-"
    return val


def formatList(string: Optional[str], replaceSemicolons=True, replaceAnds=True) -> List[str]:
    """Parses a list.

    Garantees that each element of the list is non-null and non-empty. Gracefully supports quoting: does
    not split items that are quoted (single or double quotes).

    Args:
    - string (Optional[str]): String to convert into a list, using colons as separators
    - replaceSemicolons (bool, optional): Also use semicolons as separators. Defaults to True.
    - replaceAnds (bool, optional): Also uses "and" as separators. Defaults to True.

    Returns:
    - List[str]: List of strings
    """
    if string is None:
        return []

    value = string.strip()
    if replaceSemicolons:
        value = value.replace(";", ", ")
    if replaceAnds:
        value = value.replace(" et ", ", ").replace(" and ", ", ")

    quotedstring = pyparsing.quotedString.copy()
    quotedstring.addParseAction(pyparsing.removeQuotes)
    element = pyparsing.originalTextFor(
        pyparsing.ZeroOrMore(
            pyparsing.Word(pyparsing.printables + pyparsing.alphas8bit, excludeChars="(),")
            | pyparsing.nestedExpr()
        )
    )

    expr = pyparsing.delimitedList(quotedstring | element)
    parsed = expr.parseString(value, parseAll=True)

    return [x for x in parsed.asList() if x]


def formatMontant(montant: Optional[str], shittyFormat: str = "en_US") -> str:
    """Parses an amount.

    Does not guarantee the string is a valid int/float.

    Args:
    - montant (Optional[str]): String to parse - if empty or None, returns 0
    - shittyFormat (str, optional): Format for parsing. Defaults to "en_US" where decimal separator is ".", also supports "fr_FR" where decimal separator is ","

    Returns:
    - str: Parsed value as a string, 0 if None or empty
    """
    if montant is None:
        return "0"

    value = montant.strip()
    if not value:
        return "0"

    if "(" in value:
        value = "-" + value.replace("(", "").replace(")", "")
    if shittyFormat == "en_US":
        value = value.replace(",", "")
    else:
        value = value.replace(",", ".")
    value = value.replace(" ", "")
    value = value.replace(" ", "")  # Unicode

    if value == "" or value == "-":
        return "0"
    return value


def str2bool(v: str) -> bool:
    """Parses a boolean value.

    "yes", true", "t", "1", "oui", "vrai" are matched as True (case-insensitive), any other values (including empty) are False

    Args:
    - v (str): String to parse

    Returns:
    - bool: Value
    """
    return v.lower() in ("yes", "true", "t", "1", "oui", "vrai")


def str2boolnullable(v: str) -> Optional[bool]:
    """Parses a boolean value.

    "yes", true", "t", "1", "oui", "vrai" are matched as True (case-insensitive), any other non-empty values are False. Empty value returns None.

    Args:
    - v (str): String to parse

    Returns:
    - Optional[bool]: Value, or None if empty
    """
    return (str2bool(v)) if v else None


def str2strnullable(v: str, maxlength: Optional[int] = None) -> Optional[str]:
    """Parses a string.

    Returns None if None or empty (including only whitespaces).

    Args:
    - v (str): String to parse.
    - maxlength (Optional[int], optional): Max length to parse. Defaults to None.

    Returns:
    - Optional[str]: String or None if empty
    """
    if not v:
        return None
    val = v.strip()
    if not val:
        return None
    if maxlength is not None and len(val) > maxlength:
        val = val[0:maxlength]
    return val


def create_str2strnullable(length: Optional[int]) -> Callable[[str], Optional[str]]:
    """Creates a function to parse a string

    This is useful when used in lambdas.

    Args:
    - length (Optional[int]): Max length to parse.

    Returns:
    - Callable[[str], Optional[str]]: Parser
    """
    return lambda x: str2strnullable(x, maxlength=length)


def str2intnullable(v: str) -> Optional[int]:
    """Parses an int

    Args:
    - v (str): String to parse

    Returns:
    - Optional[int]: Value, or None if could not parse the string
    """
    try:
        return int(v.strip())
    except ValueError:
        pass
    return None


def str2intamount(v: str, shittyFormat: str = "en_US") -> int:
    """Parses an amount.

    See formatMontant

    Args:
    - v (str): String to parse
    - shittyFormat (str, optional): Format for parsing. Defaults to "en_US" where decimal separator is ".", also supports "fr_FR" where decimal separator is ","

    Returns:
    - int: Parsed value, or 0 if could not parse the string
    """
    try:
        return int(formatMontant(v, shittyFormat))
    except ValueError:
        return 0


def str2floatnullable(v: str) -> Optional[float]:
    """Parses a float

    Args:
    - v (str): String to parse

    Returns:
    - Optional[float]: Value, or None if could not parse the string
    """
    try:
        val = float(v.strip())
        return val
    except ValueError:
        pass
    return None


def str2floatamount(v: str, shittyFormat: str = "en_US") -> float:
    """Parses an amount.

    See formatMontant

    Args:
    - v (str): String to parse
    - shittyFormat (str, optional): Format for parsing. Defaults to "en_US" where decimal separator is ".", also supports "fr_FR" where decimal separator is ","

    Returns:
    - float: Parsed value, or 0 if could not parse the string
    """
    try:
        return float(formatMontant(v, shittyFormat))
    except ValueError:
        return 0.0


def parseShittyDate(date: str, shittyFormat: str = "fr_FR") -> Optional[datetime.datetime]:
    """Parses a date.

    Depending on shittyFormat parameter, several formats are tested. Ultimately, if no format is valid, raises a ValueError.

    - fr_FR, tests for:
        - %d/%m/%y then %m/%d/%Y
        - %d.%m.%Y then %m.%d.%Y
        - %Y-%m-%d
    - en_US, tests for:
        - %m/%d/%Y then %d/%m/%y
        - %m.%d.%Y then %d.%m.%Y
        - %Y-%m-%d

    Args:
    - date (str): String to parse
    - shittyFormat (str, optional): Format to use. Defaults to "fr_FR", other format support is "en_US.

    Returns:
    - Optional[datetime.datetime]: Datetime object, or None if empty

    Raises:
    - ValueError: Unknown format
    """
    stripped = date.strip()
    if "/" in stripped:
        try:
            return datetime.datetime.strptime(stripped, "%d/%m/%Y" if shittyFormat == "fr_FR" else "%m/%d/%Y")
        except ValueError:
            return datetime.datetime.strptime(stripped, "%m/%d/%Y" if shittyFormat == "fr_FR" else "%d/%m/%Y")
    elif "." in stripped:
        try:
            return datetime.datetime.strptime(stripped, "%d.%m.%Y" if shittyFormat == "fr_FR" else "%m.%d.%Y")
        except ValueError:
            return datetime.datetime.strptime(stripped, "%m.%d.%Y" if shittyFormat == "fr_FR" else "%d.%m.%Y")
    else:
        if stripped == "10000-01-01" or stripped == "10000-01-01 0:00:00":
            return datetime.datetime.max
        return datetime.datetime.strptime(stripped, "%Y-%m-%d") if stripped else None


def parseShittyDateAsDate(date: str, shittyFormat: str = "fr_FR") -> Optional[datetime.date]:
    """Parses a date.

    See parseShittyDate.

    Args:
    - date (str): String to parse
    - shittyFormat (str, optional): Format to use. Defaults to "fr_FR", other format support is "en_US.

    Returns:
    - Optional[datetime.date]: Date object, or None if empty

    Raises:
    - ValueError: Unknown format
    """
    parsed = parseShittyDate(date, shittyFormat)
    return parsed.date() if parsed else None


def parseShittyDateTime(date: str, shittyFormat: str = "en_US") -> Optional[datetime.datetime]:
    """Parses a datetime.

    Depending on shittyFormat parameter, several formats are tested. Ultimately, if no format is valid, raises a ValueError.

    - fr_FR, tests for:
        - %d/%m/%y %H:%M:%S then %m/%d/%Y %H:%M:%S
        - %d.%m.%Y %H:%M:%S then %m.%d.%Y %H:%M:%S
        - %Y-%m-%d %H:%M:%S
    - en_US, tests for:
        - %m/%d/%Y %H:%M:%S then %d/%m/%y %H:%M:%S
        - %m.%d.%Y %H:%M:%S then %d.%m.%Y %H:%M:%S
        - %Y-%m-%d %H:%M:%S

    Args:
    - date (str): String to parse
    - shittyFormat (str, optional): Format to use. Defaults to "fr_FR", other format support is "en_US.

    Returns:
    - Optional[datetime.datetime]: Datetime object, or None if empty

    Raises:
    - ValueError: Unknown format
    """
    stripped = date.strip()
    if "/" in date:
        try:
            return datetime.datetime.strptime(
                stripped, "%m/%d/%Y %H:%M:%S" if shittyFormat == "en_US" else "%d/%m/%Y %H:%M:%S"
            )
        except ValueError:
            return datetime.datetime.strptime(
                stripped, "%d/%m/%Y %H:%M:%S" if shittyFormat == "en_US" else "%m/%d/%Y %H:%M:%S"
            )
    elif "." in date:
        try:
            return datetime.datetime.strptime(
                stripped, "%m.%d.%Y %H:%M:%S" if shittyFormat == "en_US" else "%d.%m.%Y %H:%M:%S"
            )
        except ValueError:
            return datetime.datetime.strptime(
                stripped, "%d.%m.%Y %H:%M:%S" if shittyFormat == "en_US" else "%m.%d.%Y %H:%M:%S"
            )
    else:
        if stripped == "10000-01-01 0:00:00":
            return datetime.datetime.max
        return datetime.datetime.strptime(stripped, "%Y-%m-%d %H:%M:%S") if stripped else None


def parseShittyTime(date: str) -> Optional[datetime.timedelta]:
    """Parses a time using %H:%M:%S format

    Args:
    - date (str): String to parse

    Returns:
    - Optional[datetime.timedelta]: Timedelta object, or None if empty

    Raises:
    - ValueError: Unknown format
    """
    stripped = date.strip()
    if stripped:
        t = datetime.datetime.strptime(stripped, "%H:%M:%S")
        return datetime.timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    return None


X = TypeVar("X")


def to_list(values: Sequence[Optional[X]]) -> List[X]:
    """Returns a list without any falsy object

    Args:
    - values (Sequence[Optional[X]]): Input list

    Returns:
    - List[X]: List without any falsy object
    """
    return [x for x in values if x]
