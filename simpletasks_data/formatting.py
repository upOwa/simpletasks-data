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
    """Deserialize datetime object into string form for JSON processing."""
    if value is None or is_undefined(value):
        return None
    if fmt == "json":
        fmt = "%Y-%m-%dT%H:%M:%S%z"
    elif fmt == "gs":
        fmt = "%Y-%m-%d %H:%M:%S"
    return value.strftime(fmt)


if has_geoalchemy2:

    def dump_gps(coordinates: geoalchemy2.Geography) -> Optional[str]:
        """Deserialize GPS object into string form for JSON processing."""
        if coordinates is None or is_undefined(coordinates):
            return None
        shape = geoalchemy2.shape.to_shape(coordinates)
        return "{:f}, {:f}".format(shape.y, shape.x)


def dump_int(val: Optional[Union[int, float]]) -> str:
    if val is None or is_undefined(val):
        return "0"
    return "{:.0f}".format(val)


def dump_percentage(val: Optional[float]) -> str:
    if val is None or is_undefined(val):
        return "0.0"
    return "{:.1f}".format(val * 100)


def dump_str(val: Optional[str]) -> str:
    if val is None or is_undefined(val) or val.strip() == "":
        return "-"
    return val


def formatList(string: Optional[str], replaceSemicolons=True, replaceAnds=True) -> List[str]:
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
    return v.lower() in ("yes", "true", "t", "1", "oui", "vrai")


def str2boolnullable(v: str) -> Optional[bool]:
    return (str2bool(v)) if v else None


def str2strnullable(v: str, maxlength: Optional[int] = None) -> Optional[str]:
    if not v:
        return None
    val = v.strip()
    if not val:
        return None
    if maxlength is not None and len(val) > maxlength:
        val = val[0:maxlength]
    return val


def create_str2strnullable(length: Optional[int]) -> Callable[[str], Optional[str]]:
    return lambda x: str2strnullable(x, maxlength=length)


def str2intnullable(v: str) -> Optional[int]:
    try:
        return int(v.strip())
    except ValueError:
        pass
    return None


def str2intamount(v: str, shittyFormat: str = "en_US") -> int:
    try:
        return int(formatMontant(v, shittyFormat))
    except ValueError:
        return 0


def str2floatnullable(v: str) -> Optional[float]:
    try:
        val = float(v.strip())
        return val
    except ValueError:
        pass
    return None


def str2floatamount(v: str, shittyFormat: str = "en_US") -> float:
    try:
        return float(formatMontant(v, shittyFormat))
    except ValueError:
        return 0.0


def parseShittyDate(date: str, shittyFormat: str = "fr_FR") -> Optional[datetime.datetime]:
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
    parsed = parseShittyDate(date, shittyFormat)
    return parsed.date() if parsed else None


def parseShittyDateTime(date: str, shittyFormat: str = "en_US") -> Optional[datetime.datetime]:
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
    stripped = date.strip()
    if stripped:
        t = datetime.datetime.strptime(stripped, "%H:%M:%S")
        return datetime.timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    return None


X = TypeVar("X")


def to_list(values: Sequence[Optional[X]]) -> List[X]:
    return [x for x in values if x]
