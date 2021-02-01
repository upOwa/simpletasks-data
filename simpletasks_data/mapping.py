import abc
import inspect
from typing import Any, Callable, Dict, Generic, Iterable, List, Sequence, Tuple, TypeVar, Union, cast

import sqlalchemy
from flask_sqlalchemy.model import Model as BaseModel

from .formatting import (
    create_str2strnullable,
    parseShittyDateAsDate,
    parseShittyDateTime,
    parseShittyTime,
    str2boolnullable,
    str2floatnullable,
    str2intnullable,
)
from .helpers import col2num

ColumnOutputType = TypeVar("ColumnOutputType")
ColumnInputType = TypeVar("ColumnInputType")
DestinationModel = TypeVar("DestinationModel", bound=BaseModel)
SourceModel = TypeVar("SourceModel", bound=BaseModel)
SourceType = TypeVar("SourceType", bound=Union[BaseModel, Sequence[str], None])
DestinationModelHistory = TypeVar("DestinationModelHistory", bound=BaseModel)


class _Caching:
    values: Dict["_Column", Any] = {}


class _Column(Generic[ColumnOutputType, ColumnInputType, SourceType], metaclass=abc.ABCMeta):
    """Base class for columns mapping"""

    def __init__(
        self,
        warn_on_error: bool = True,
        warn_if_empty: bool = False,
        should_update: bool = True,
        should_update_only_if_null: bool = False,
        keep_history: bool = False,
        comparator: Callable[[ColumnOutputType, ColumnOutputType], bool] = None,
        formatter: Callable[[ColumnOutputType], str] = None,
        header: str = None,
    ) -> None:
        """Constructor

        Args:
        - warn_on_error (bool, optional): Logs a warning if parsing failed; otherwise will ignore the column silently. Defaults to True.
        - warn_if_empty (bool, optional): Logs a warning if parsed value is empty. Defaults to False.
        - should_update (bool, optional): If False, disables update of the column, thus only using this column when creating an item. Defaults to True.
        - should_update_only_if_null (bool, optional): If True, disables update of the column if its stored value is non-null. Defaults to False.
        - keep_history (bool, optional): If True, keeps history of the column on changes. Defaults to False.
        - comparator (Callable[[ColumnOutputType, ColumnOutputType], bool], optional): Comparator to use to compare the stored value and the parsed value when importing - this is useful for types where "==" operator does not work. Defaults to None.
        - formatter (Callable[[ColumnOutputType], str], optional): Formatter to output the value when exported. Defaults to None (using "str(x)" method).
        - header (str, optional): Name of the column. Defaults to None.
        """
        self.warn_on_error = warn_on_error
        self.warn_if_empty = warn_if_empty
        self.should_update = should_update
        self.should_update_only_if_null = should_update_only_if_null
        self.keep_history = keep_history
        if comparator:
            self.comparator = comparator
        else:
            self.comparator = lambda x, y: x == y
        if formatter:
            self.formatter = formatter
        else:
            self.formatter = lambda x: str(x) if x is not None else ""
        self.header = header

    @abc.abstractmethod
    def get_raw_values(self, row: SourceType) -> List[str]:
        """Returns the raw value(s) from the source

        Args:
        - row (SourceType): Raw data

        Returns:
        - List[str]: One or more values
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def get(self, row: SourceType) -> ColumnOutputType:
        """Gets the value of the column

        Args:
        - row (SourceType): Raw data

        Returns:
        - ColumnOutputType: Parsed value

        Raises:
        - ValueError, KeyError, AttributeError: will be handled gracefully during import. Other exceptions will fail the whole import
        """
        pass  # pragma: no cover


class Column(_Column[ColumnOutputType, str, Sequence[str]]):
    """Base class for CSV columns"""

    def __init__(
        self,
        column_number: int,
        parser: Callable[[str], ColumnOutputType] = None,
        warn_on_error: bool = True,
        warn_if_empty: bool = False,
        should_update: bool = True,
        should_update_only_if_null: bool = False,
        keep_history: bool = False,
        comparator: Callable[[ColumnOutputType, ColumnOutputType], bool] = None,
        fail_on_out_of_range: bool = True,
        formatter: Callable[[ColumnOutputType], str] = None,
        header: str = None,
    ) -> None:
        """Constructor

        Args:
        - column_number (int): Column number in the CSV (0-based)
        - parser (Callable[[str], ColumnOutputType], optional): Parser - if none provided, will be computed from the model. Defaults to None.
        - warn_on_error (bool, optional): Logs a warning if parsing failed; otherwise will ignore the column silently. Defaults to True.
        - warn_if_empty (bool, optional): Logs a warning if parsed value is empty. Defaults to False.
        - should_update (bool, optional): If False, disables update of the column, thus only using this column when creating an item. Defaults to True.
        - should_update_only_if_null (bool, optional): If True, disables update of the column if its stored value is non-null. Defaults to False.
        - keep_history (bool, optional): If True, keeps history of the column on changes. Defaults to False.
        - comparator (Callable[[ColumnOutputType, ColumnOutputType], bool], optional): Comparator to use to compare the stored value and the parsed value when importing - this is useful for types where "==" operator does not work. Defaults to None.
        - fail_on_out_of_range (bool, optional): Logs a warning if column number does not exist, otherwise will ignore the column silently. Defaults to True.
        - formatter (Callable[[ColumnOutputType], str], optional): Formatter to output the value when exported. Defaults to None (using "str(x)" method).
        - header (str, optional): Name of the column. Defaults to None.
        """
        super().__init__(
            warn_on_error=warn_on_error,
            warn_if_empty=warn_if_empty,
            should_update=should_update,
            should_update_only_if_null=should_update_only_if_null,
            keep_history=keep_history,
            comparator=comparator,
            formatter=formatter,
            header=header,
        )
        self.column_number = column_number
        self.parser = parser
        self.fail_on_out_of_range = fail_on_out_of_range

    def get_raw_values(self, row: Sequence[str]) -> List[str]:
        """Returns the raw value from the source

        Args:
        - row (Sequence[str]): Raw data

        Returns:
        - List[str]: List with a single item
        """
        if self.fail_on_out_of_range:
            return [row[self.column_number]]
        else:
            try:
                return [row[self.column_number]]
            except IndexError:
                return [""]

    def get(self, row: Sequence[str]) -> ColumnOutputType:
        """Gets the value of the column.

        Uses caching to avoid multiple computations (see _Caching).

        Args:
        - row (Sequence[str]): Raw data

        Returns:
        - ColumnOutputType: Parsed value
        """
        if self in _Caching.values:
            return _Caching.values[self]

        val = cast(Callable[[str], ColumnOutputType], self.parser)(self.get_raw_values(row)[0])
        _Caching.values[self] = val
        return val


class ComputedColumn(_Column[ColumnOutputType, str, Sequence[str]]):
    """Column that is computed based on one or several CSV columns"""

    def __init__(
        self,
        columns: Iterable[_Column[ColumnOutputType, str, Sequence[str]]],
        computer: Callable[[Sequence[Any]], ColumnOutputType],
        warn_on_error: bool = True,
        warn_if_empty: bool = False,
        should_update: bool = True,
        should_update_only_if_null: bool = False,
        keep_history: bool = False,
        comparator: Callable[[ColumnOutputType, ColumnOutputType], bool] = None,
        formatter: Callable[[ColumnOutputType], str] = None,
        header: str = None,
    ) -> None:
        """Constructor

        Args:
        - columns (Iterable[_Column[ColumnOutputType, str, Sequence[str]]]): List of input columns
        - computer (Callable[[Sequence[Any]], ColumnOutputType]): Callback to compute the value of the column
        - warn_on_error (bool, optional): Logs a warning if parsing failed; otherwise will ignore the column silently. Defaults to True.
        - warn_if_empty (bool, optional): Logs a warning if parsed value is empty. Defaults to False.
        - should_update (bool, optional): If False, disables update of the column, thus only using this column when creating an item. Defaults to True.
        - should_update_only_if_null (bool, optional): If True, disables update of the column if its stored value is non-null. Defaults to False.
        - keep_history (bool, optional): If True, keeps history of the column on changes. Defaults to False.
        - comparator (Callable[[ColumnOutputType, ColumnOutputType], bool], optional): Comparator to use to compare the stored value and the parsed value when importing - this is useful for types where "==" operator does not work. Defaults to None.
        - formatter (Callable[[ColumnOutputType], str], optional): Formatter to output the value when exported. Defaults to None (using "str(x)" method).
        - header (str, optional): Name of the column. Defaults to None.
        """
        super().__init__(
            warn_on_error=warn_on_error,
            warn_if_empty=warn_if_empty,
            should_update=should_update,
            should_update_only_if_null=should_update_only_if_null,
            keep_history=keep_history,
            comparator=comparator,
            formatter=formatter,
            header=header,
        )
        self.columns = columns
        self.computer = computer

    def get_raw_values(self, row: Sequence[str]) -> List[str]:
        """Returns the raw values from the source

        Args:
        - row (Sequence[str]): Raw data

        Returns:
        - List[str]: List of raw values from input columns
        """
        lst = []
        for x in self.columns:
            lst.extend(x.get_raw_values(row))
        return lst

    def get(self, row: Sequence[str]) -> ColumnOutputType:
        """Gets the value of the column.

        Uses caching to avoid multiple computations (see _Caching).

        Args:
        - row (Sequence[str]): Raw data

        Returns:
        - ColumnOutputType: Computed value
        """
        if self in _Caching.values:
            return _Caching.values[self]

        val = self.computer([x.get(row) for x in self.columns])
        _Caching.values[self] = val
        return val


class StaticColumn(_Column[ColumnOutputType, str, None]):
    """Column that has a static value, that never changes."""

    def __init__(
        self,
        value: ColumnOutputType,
        should_update: bool = True,
        should_update_only_if_null: bool = False,
        keep_history: bool = False,
        comparator: Callable[[ColumnOutputType, ColumnOutputType], bool] = None,
        formatter: Callable[[ColumnOutputType], str] = None,
        header: str = None,
    ) -> None:
        """Constructor

        Args:
        - value (ColumnOutputType): Value of the column
        - should_update (bool, optional): If False, disables update of the column, thus only using this column when creating an item. Defaults to True.
        - should_update_only_if_null (bool, optional): If True, disables update of the column if its stored value is non-null. Defaults to False.
        - keep_history (bool, optional): If True, keeps history of the column on changes. Defaults to False.
        - comparator (Callable[[ColumnOutputType, ColumnOutputType], bool], optional): Comparator to use to compare the stored value and the parsed value when importing - this is useful for types where "==" operator does not work. Defaults to None.
        - formatter (Callable[[ColumnOutputType], str], optional): Formatter to output the value when exported. Defaults to None (using "str(x)" method).
        - header (str, optional): Name of the column. Defaults to None.
        """

        super().__init__(
            should_update=should_update,
            should_update_only_if_null=should_update_only_if_null,
            keep_history=keep_history,
            comparator=comparator,
            formatter=formatter,
            header=header,
        )
        self.value = value

    def get_raw_values(self, row: Any) -> List[str]:
        """Returns the raw values from the source

        Args:
        - row (Any): Raw data (not used)

        Returns:
        - List[str]: List with a single item
        """
        return [str(self.value)]

    def get(self, row: Any) -> ColumnOutputType:
        """Gets the static value of the column.

        Args:
        - row (Any): Raw data (not used)

        Returns:
        - ColumnOutputType: Static value
        """
        return self.value


class Field(_Column[ColumnOutputType, ColumnInputType, SourceModel]):
    """Base class for model-based columns"""

    def __init__(
        self,
        field: str = None,
        parser: Callable[[ColumnInputType], ColumnOutputType] = None,
        warn_on_error: bool = True,
        warn_if_empty: bool = False,
        should_update: bool = True,
        should_update_only_if_null: bool = False,
        keep_history: bool = False,
        comparator: Callable[[ColumnOutputType, ColumnOutputType], bool] = None,
        formatter: Callable[[ColumnOutputType], str] = None,
        header: str = None,
    ) -> None:
        """Constructor

        Args:
        - field (str, optional): Field name. Defaults to None, will use the name of the class field (e.g. `self.foobar = Field()` will set `foobar` as field name). Can use dotted syntax to traverse relations.
        - parser (Callable[[ColumnInputType], ColumnOutputType], optional): Parser - if none provided, will be computed from the model. Defaults to None.
        - warn_on_error (bool, optional): Logs a warning if parsing failed; otherwise will ignore the column silently. Defaults to True.
        - warn_if_empty (bool, optional): Logs a warning if parsed value is empty. Defaults to False.
        - should_update (bool, optional): If False, disables update of the column, thus only using this column when creating an item. Defaults to True.
        - should_update_only_if_null (bool, optional): If True, disables update of the column if its stored value is non-null. Defaults to False.
        - keep_history (bool, optional): If True, keeps history of the column on changes. Defaults to False.
        - comparator (Callable[[ColumnOutputType, ColumnOutputType], bool], optional): Comparator to use to compare the stored value and the parsed value when importing - this is useful for types where "==" operator does not work. Defaults to None.
        - formatter (Callable[[ColumnOutputType], str], optional): Formatter to output the value when exported. Defaults to None (using "str(x)" method).
        - header (str, optional): Name of the column. Defaults to None.
        """
        super().__init__(
            warn_on_error=warn_on_error,
            warn_if_empty=warn_if_empty,
            should_update=should_update,
            should_update_only_if_null=should_update_only_if_null,
            keep_history=keep_history,
            comparator=comparator,
            formatter=formatter,
            header=header,
        )
        self.field = field
        self.parser = parser

    def _get(self, row: SourceModel) -> ColumnInputType:
        item: Any = row
        for part in cast(str, self.field).split("."):
            item = getattr(item, part)
            if item is None:
                break
        return cast(ColumnInputType, item)

    def get_raw_values(self, row: SourceModel) -> List[str]:
        """Returns the raw value from the source

        Args:
        - row (SourceModel): Raw data

        Returns:
        - List[str]: List with a single item
        """
        return [str(self._get(row))]

    def get(self, row: SourceModel) -> ColumnOutputType:
        """Gets the value of the column.

        Uses caching to avoid multiple computations (see _Caching).

        Args:
        - row (SourceModel): Raw data

        Returns:
        - ColumnOutputType: Parsed value
        """
        if self in _Caching.values:
            return _Caching.values[self]

        val = cast(Callable[[ColumnInputType], ColumnOutputType], self.parser)(self._get(row))
        _Caching.values[self] = val
        return val


class ComputedField(_Column[ColumnOutputType, Any, SourceModel]):
    """Field that is computed based on one or several fields"""

    def __init__(
        self,
        columns: Iterable[Field[ColumnOutputType, Any, SourceModel]],
        computer: Callable[[Sequence[Any]], ColumnOutputType],
        warn_on_error: bool = True,
        warn_if_empty: bool = False,
        should_update: bool = True,
        should_update_only_if_null: bool = False,
        keep_history: bool = False,
        comparator: Callable[[ColumnOutputType, ColumnOutputType], bool] = None,
        formatter: Callable[[ColumnOutputType], str] = None,
        header: str = None,
    ) -> None:
        """Constructor

        Args:
        - columns (Iterable[_Column[ColumnOutputType, str, SourceModel]]): List of input columns
        - computer (Callable[[Sequence[Any]], ColumnOutputType]): Callback to compute the value of the column
        - warn_on_error (bool, optional): Logs a warning if parsing failed; otherwise will ignore the column silently. Defaults to True.
        - warn_if_empty (bool, optional): Logs a warning if parsed value is empty. Defaults to False.
        - should_update (bool, optional): If False, disables update of the column, thus only using this column when creating an item. Defaults to True.
        - should_update_only_if_null (bool, optional): If True, disables update of the column if its stored value is non-null. Defaults to False.
        - keep_history (bool, optional): If True, keeps history of the column on changes. Defaults to False.
        - comparator (Callable[[ColumnOutputType, ColumnOutputType], bool], optional): Comparator to use to compare the stored value and the parsed value when importing - this is useful for types where "==" operator does not work. Defaults to None.
        - formatter (Callable[[ColumnOutputType], str], optional): Formatter to output the value when exported. Defaults to None (using "str(x)" method).
        - header (str, optional): Name of the column. Defaults to None.
        """
        super().__init__(
            warn_on_error=warn_on_error,
            warn_if_empty=warn_if_empty,
            should_update=should_update,
            should_update_only_if_null=should_update_only_if_null,
            keep_history=keep_history,
            comparator=comparator,
            formatter=formatter,
            header=header,
        )
        self.columns = columns
        self.computer = computer

    def get_raw_values(self, row: SourceModel) -> List[str]:
        """Returns the raw values from the source

        Args:
        - row (SourceModel): Raw data

        Returns:
        - List[str]: List of raw values from input columns
        """
        lst = []
        for x in self.columns:
            lst.extend(x.get_raw_values(row))
        return lst

    def get(self, row: SourceModel) -> ColumnOutputType:
        """Gets the value of the column.

        Uses caching to avoid multiple computations (see _Caching).

        Args:
        - row (SourceModel): Raw data

        Returns:
        - ColumnOutputType: Computed value
        """
        if self in _Caching.values:
            return _Caching.values[self]

        val = self.computer([x.get(row) for x in self.columns])
        _Caching.values[self] = val
        return val


class Mapping(Generic[DestinationModel]):
    """Defines a mapping between a source and a model (for importing), or between a model and a destination (for exporting)"""

    def __init__(self) -> None:
        """Constructor"""
        self._auto_counter = 0

    def auto(self, *args, **kwargs) -> Column:
        """Shorthand to register a Column with the next available column number.

        Typically used as `self.foobar = self.auto()` in constructor.

        Args: same as `Column`.

        Returns:
        - Column: Created column
        """
        val = self._auto_counter
        self._auto_counter += 1
        return Column(val, *args, **kwargs)

    def col(self, column: Union[str, int], *args, **kwargs) -> Column:
        """Shorthand to register a Column with a custom name/index.

        Will reset the next available column number.

        Args:
        - column (Union[str, int]): Column name in A1N1 notation (e.g. "A", "F", "AF", etc.) or Column number (0-indexed)
        - Same as `Column`

        Returns:
        - Column: Created column
        """
        column_idx = col2num(column) if isinstance(column, str) else column
        self._auto_counter = column_idx + 1
        return Column(column_idx, *args, **kwargs)

    def get_key_column_name(self) -> str:
        """Method that can be implemented in the concrete class to define a custom column as key column.

        The key column will be used to match input data with the model.
        By default, "id" is used. You can provide another name by reimplementing this in your class.
        The column name *MUST* be a field member of the class (e.g. `self.id = self.auto()`).

        Returns:
        - str: Name of the column
        """
        return "id"

    def get_key_column_comparator(self) -> Callable[[Any], Any]:
        """Method that can be implemented in the concrete class to define a custom comparator for the key column.

        The comparator converts the value from the model into a value that is 1- hashable and 2- comparable with the input data via "==" operator.
        As some types are not well handled (e.g. geographical objects), it can be necessary to implement this.

        Returns:
        - Callable[[Any], Any]: Method to convert your data
        """
        return lambda x: x

    def get_header_line_number(self) -> int:
        """Method that can be implemented in the concrete class to define the number of lines to skip in the source.

        By default, one header line is defined and skipped (value of 0). Use `-1` not to skip any lines.

        Returns:
        - int: Number of lines to skip in the source
        """
        return 0

    def get_columns(self) -> List[Tuple[str, _Column]]:
        """Returns the columns defined in this mapping

        Returns:
        - List[Tuple[str, _Column]]: List of tuples (name of the column in the model, _Column object)
        """
        columns = []
        for i in inspect.getmembers(self):
            if not i[0].startswith("_") and not inspect.ismethod(i[1]):
                if isinstance(i[1], _Column):
                    columns.append((i[0], i[1]))
        return columns

    def _complete_from_model(self, model: DestinationModel) -> Tuple[str, _Column]:
        """Generates from the model:
        - parsers
        - field names
        - comparators
        - headers

        Args:
        - model (DestinationModel): Model

        Raises:
        - NotImplementedError: Unknown SQLAlchemy column type to determine parser - must define parser explicitely
        - ValueError: computer parameter not provided for ComputedColumn/ComputedField
        - ValueError: key column could not be found
        - RuntimeError: Unsupported type of column

        Returns:
        - Tuple[str, _Column]: Tuple (keycolumn_name, keycolumn)
        """
        keycolumn_name = None
        keycolumn = None

        for name, column in self.get_columns():
            if isinstance(column, Column):
                if not column.parser:
                    # Find column parser based on the type of the model
                    columnModel = model.__table__.c[name]  # type: ignore
                    if isinstance(columnModel.type, sqlalchemy.sql.sqltypes.Boolean):
                        column.parser = cast(Callable[[str], ColumnOutputType], str2boolnullable)
                    elif isinstance(columnModel.type, sqlalchemy.sql.sqltypes.Integer):
                        column.parser = cast(Callable[[str], ColumnOutputType], str2intnullable)
                    elif isinstance(columnModel.type, sqlalchemy.sql.sqltypes.Numeric):
                        column.parser = cast(Callable[[str], ColumnOutputType], str2floatnullable)
                    elif isinstance(columnModel.type, sqlalchemy.sql.sqltypes.String):
                        column.parser = cast(
                            Callable[[str], ColumnOutputType], create_str2strnullable(columnModel.type.length)
                        )
                    elif isinstance(columnModel.type, sqlalchemy.sql.sqltypes.Date):
                        column.parser = cast(Callable[[str], ColumnOutputType], parseShittyDateAsDate)
                    elif isinstance(columnModel.type, sqlalchemy.sql.sqltypes.DateTime):
                        column.parser = cast(Callable[[str], ColumnOutputType], parseShittyDateTime)
                    elif isinstance(columnModel.type, sqlalchemy.sql.sqltypes.Time):
                        column.parser = cast(Callable[[str], ColumnOutputType], parseShittyTime)
                    else:
                        raise NotImplementedError(
                            "Could not find parser for type {}; you should provide a parser explicitely for the column {}".format(
                                columnModel.type, name
                            )
                        )
            elif isinstance(column, ComputedColumn):
                if not column.computer:
                    raise ValueError("computer cannot be empty for ComputedColumn {}".format(name))
            elif isinstance(column, StaticColumn):
                pass
            elif isinstance(column, Field):
                if not column.parser:
                    column.parser = lambda x: x
                if not column.field:
                    column.field = name
            elif isinstance(column, ComputedField):
                if not column.computer:
                    raise ValueError("computer cannot be empty for ComputedField {}".format(name))
            else:
                raise RuntimeError("Unsupported type of column {}".format(name))

            if not column.comparator:
                column.comparator = lambda x, y: x == y

            if not column.header:
                column.header = name

            if name == self.get_key_column_name():
                keycolumn_name = name
                keycolumn = column

        if keycolumn_name is None or keycolumn is None:
            raise ValueError("Could not find key column {} in mapping".format(self.get_key_column_name()))
        return (keycolumn_name, keycolumn)

    # def column_mapped(self, column_number: int) -> Optional[str]:
    #     for i in inspect.getmembers(self):
    #         if not i[0].startswith("_") and not inspect.ismethod(i[1]):
    #             if isinstance(i[1], _InternalColumn):
    #                 if isinstance(i[1].column_number, list):
    #                     if column_number in i[1].column_number:
    #                         return i[0]
    #                 elif i[1].column_number == column_number:
    #                     return i[0]
    #     return None
