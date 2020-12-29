import abc
import contextlib
import csv
import enum
from typing import TYPE_CHECKING, Any, Dict, Generic, Iterable, Iterator, Optional, Sequence, Tuple

from sqlalchemy.orm import Query

from .mapping import DestinationModel, Mapping, SourceModel, SourceType, _Column

if TYPE_CHECKING:
    from .importtask import ImportTask


class ImportMode(enum.Flag):
    CREATE = enum.auto()
    UPDATE = enum.auto()
    CREATE_AND_UPDATE = CREATE | UPDATE


class ImportSource(Generic[SourceType, DestinationModel], metaclass=abc.ABCMeta):
    def __init__(self, mapping: Mapping) -> None:
        self._mapping = mapping

    @abc.abstractmethod
    @contextlib.contextmanager
    def getGeneratorData(self) -> Iterator[Iterable[SourceType]]:
        raise NotImplementedError

    def _set_parent(self, parent: "ImportTask") -> None:
        self._parent = parent

    def _fill_mapping(self, model: DestinationModel) -> Mapping:
        self.keycolumn_name, self.keycolumn = self._mapping._complete_from_model(model)
        self.importer_mapping: Dict[str, _Column] = dict(self._mapping.get_columns())

        assert self.keycolumn_name is not None
        assert self.keycolumn is not None

        return self._mapping

    def get_keycolumn_name(self) -> str:
        return self.keycolumn_name

    def get_key_from_model(self, value: str) -> Any:
        return self._mapping.get_key_column_comparator()(value)

    def get_columns(self) -> Iterable[Tuple[str, _Column]]:
        return self.importer_mapping.items()

    def get_key(self, row: SourceType) -> Optional[Any]:
        key = self.keycolumn.get(row)
        if key is None:
            return None
        return self._mapping.get_key_column_comparator()(key)

    def get_header_line_number(self) -> int:
        return self._mapping.get_header_line_number()

    def should_import(self, row: SourceType) -> bool:
        return True

    def validate_updates(
        self, model: DestinationModel, row: SourceType, updates: Dict[str, Any], creating: bool
    ) -> bool:
        return True

    def on_data_not_found(self, model: DestinationModel) -> None:
        pass

    @property
    def mode(self) -> ImportMode:
        return ImportMode.CREATE_AND_UPDATE

    @property
    def name(self) -> str:
        return self.__class__.__name__


class ImportCsv(ImportSource[Sequence[str], DestinationModel]):
    def __init__(self, file, mapping: Mapping) -> None:
        super().__init__(mapping)
        self._file = file

    @contextlib.contextmanager
    def getGeneratorData(self) -> Iterator[Iterable[Sequence[str]]]:
        with open(self._file, "r", encoding="utf-8") as csvfile:
            yield csv.reader(csvfile, delimiter=",", quotechar='"')  # type: ignore


class ImportTable(ImportSource[SourceModel, DestinationModel]):
    def __init__(self, query: Query, mapping: Mapping) -> None:
        super().__init__(mapping)
        self._query = query

    @contextlib.contextmanager
    def getGeneratorData(self) -> Iterator[Iterable[SourceModel]]:
        yield self._query.all()
