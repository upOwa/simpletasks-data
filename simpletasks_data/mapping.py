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
        pass

    @abc.abstractmethod
    def get(self, row: SourceType) -> ColumnOutputType:
        pass


class Column(_Column[ColumnOutputType, str, Sequence[str]]):
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
        if self.fail_on_out_of_range:
            return [row[self.column_number]]
        else:
            try:
                return [row[self.column_number]]
            except IndexError:
                return [""]

    def get(self, row: Sequence[str]) -> ColumnOutputType:
        if self in _Caching.values:
            return _Caching.values[self]

        val = cast(Callable[[str], ColumnOutputType], self.parser)(self.get_raw_values(row)[0])
        _Caching.values[self] = val
        return val


class ComputedColumn(_Column[ColumnOutputType, str, Sequence[str]]):
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
        lst = []
        for x in self.columns:
            lst.extend(x.get_raw_values(row))
        return lst

    def get(self, row: Sequence[str]) -> ColumnOutputType:
        if self in _Caching.values:
            return _Caching.values[self]

        val = self.computer([x.get(row) for x in self.columns])
        _Caching.values[self] = val
        return val


class StaticColumn(_Column[ColumnOutputType, str, None]):
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
        return [str(self.value)]

    def get(self, row: Any) -> ColumnOutputType:
        return self.value


class Field(_Column[ColumnOutputType, ColumnInputType, SourceModel]):
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
        return [str(self._get(row))]

    def get(self, row: SourceModel) -> ColumnOutputType:
        if self in _Caching.values:
            return _Caching.values[self]

        val = cast(Callable[[ColumnInputType], ColumnOutputType], self.parser)(self._get(row))
        _Caching.values[self] = val
        return val


class ComputedField(_Column[ColumnOutputType, Any, SourceModel]):
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
        lst = []
        for x in self.columns:
            lst.extend(x.get_raw_values(row))
        return lst

    def get(self, row: SourceModel) -> ColumnOutputType:
        if self in _Caching.values:
            return _Caching.values[self]

        val = self.computer([x.get(row) for x in self.columns])
        _Caching.values[self] = val
        return val


class Mapping(Generic[DestinationModel]):
    def __init__(self) -> None:
        self._auto_counter = 0

    def auto(self, *args, **kwargs) -> Column:
        val = self._auto_counter
        self._auto_counter += 1
        return Column(val, *args, **kwargs)

    def col(self, column_name: str, *args, **kwargs) -> Column:
        return Column(col2num(column_name), *args, **kwargs)

    def computedcol(
        self,
        *args,
        **kwargs,
    ) -> ComputedColumn:
        return ComputedColumn(*args, **kwargs)

    def staticcol(self, *args, **kwargs) -> StaticColumn:
        return StaticColumn(*args, **kwargs)

    def field(self, *args, **kwargs) -> Field[Any, Any, Any]:
        return Field(*args, **kwargs)

    def computedfield(
        self,
        *args,
        **kwargs,
    ) -> ComputedField:
        return ComputedField(*args, **kwargs)

    def get_key_column_name(self) -> str:
        return "id"

    def get_key_column_comparator(self) -> Callable[[Any], Any]:
        return lambda x: x

    def get_header_line_number(self) -> int:
        return 0

    def get_columns(self) -> List[Tuple[str, _Column]]:
        columns = []
        for i in inspect.getmembers(self):
            if not i[0].startswith("_") and not inspect.ismethod(i[1]):
                if isinstance(i[1], _Column):
                    columns.append((i[0], i[1]))
        return columns

    def _complete_from_model(self, model: DestinationModel) -> Tuple[str, _Column]:
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
