import abc
import contextlib
import csv
import enum
from typing import TYPE_CHECKING, Any, Dict, Generic, Iterable, Iterator, Optional, Sequence, Tuple

from sqlalchemy.orm import Query

from .mapping import DestinationModel, Mapping, SourceModel, SourceType, _Column

if TYPE_CHECKING:
    from .importtask import ImportTask  # pragma: no cover


class ImportMode(enum.Flag):
    CREATE = enum.auto()
    UPDATE = enum.auto()
    CREATE_AND_UPDATE = CREATE | UPDATE


class ImportSource(Generic[SourceType, DestinationModel], metaclass=abc.ABCMeta):
    """Abstract class to define a source to be used in ImportTask."""

    def __init__(self, mapping: Mapping) -> None:
        self._mapping = mapping

    @abc.abstractmethod
    @contextlib.contextmanager
    def getGeneratorData(self) -> Iterator[Iterable[SourceType]]:
        """Data generator, to be implemented in concrete class"""
        raise NotImplementedError  # pragma: no cover

    def _set_parent(self, parent: "ImportTask") -> None:
        self._parent = parent

    def _fill_mapping(self, model: DestinationModel) -> Mapping:
        self.keycolumn_name, self.keycolumn = self._mapping._complete_from_model(model)
        self.importer_mapping: Dict[str, _Column] = dict(self._mapping.get_columns())

        assert self.keycolumn_name is not None
        assert self.keycolumn is not None

        return self._mapping

    def get_keycolumn_name(self) -> str:
        """Returns the column name in mapping to be used as key for import

        Returns:
        - str: name of the column in mapping
        """
        return self.keycolumn_name

    def get_key_from_model(self, value: str) -> Any:
        """Returns the key value from the model, ready to be compared to the imported data.

        This is useful if "==" operator does not handle comparison well and requires transformation.

        Args:
        - value (str): Value of the keycolumn from the model

        Returns:
        - Any: Value ready to be compared to the imported data
        """
        return self._mapping.get_key_column_comparator()(value)

    def get_columns(self) -> Iterable[Tuple[str, _Column]]:
        """Returns the list of columns from the mapping

        Returns:
        - Iterable[Tuple[str, _Column]]: List of tuples (name of the column in the model, _Column object)
        """
        return self.importer_mapping.items()

    def get_key(self, row: SourceType) -> Optional[Any]:
        """Returns the key of a row.

        Args:
        - row (SourceType): Input data

        Returns:
        - Optional[Any]: Key, or None if non-present
        """
        key = self.keycolumn.get(row)
        if key is None:
            return None
        return self._mapping.get_key_column_comparator()(key)

    def get_header_line_number(self) -> int:
        """Returns the number of lines to skip in the source

        By default, one header line is defined and skipped (value of 0). Use `-1` not to skip any lines.

        Returns:
        - int: Number of lines to skip in the source
        """
        return self._mapping.get_header_line_number()

    def should_import(self, row: SourceType) -> bool:
        """Method that can be implemented in the concrete class to filter-out rows

        Args:
        - row (SourceType): Input data

        Returns:
        - bool: True if should import (default), or False if should skip
        """
        return True

    def validate_updates(
        self, model: DestinationModel, row: SourceType, updates: Dict[str, Any], creating: bool
    ) -> bool:
        """Method that can be implemented in the concrete class to filter-out rows once imported

        Args:
        - model (DestinationModel): Item to update/create
        - row (SourceType): Input data
        - updates (Dict[str, Any]): All updates to apply
        - creating (bool): True if creating the item

        Returns:
        - bool: True if should apply updates (default), or False if should skip this item
        """
        return True

    def on_data_not_found(self, model: DestinationModel) -> None:
        """Method that can be implemented in the concrete class to handle missing data in the source.

        By default, does nothing. Can be used to delete the item for instance.

        Args:
        - model (DestinationModel): Item not found in the source
        """
        pass

    @property
    def mode(self) -> ImportMode:
        """Method to implement in the concrete class to change the import mode.

        Returns:
        - ImportMode: Import mode
        """
        return ImportMode.CREATE_AND_UPDATE

    @property
    def name(self) -> str:
        """Method to implement in the concrete class to change the name of the source (used for logger and displaying progress).

        Returns:
        - str: Name of the source
        """
        return self.__class__.__name__


class ImportCsv(ImportSource[Sequence[str], DestinationModel]):
    """Class to define a CSV source to be used in ImportTask."""

    def __init__(self, file, mapping: Mapping) -> None:
        """Constructor

        Args:
        - file (file-like): file-like object
        - mapping (Mapping): Mapping
        """
        super().__init__(mapping)
        self._file = file

    @contextlib.contextmanager
    def getGeneratorData(self) -> Iterator[Iterable[Sequence[str]]]:
        with open(self._file, "r", encoding="utf-8") as csvfile:
            yield csv.reader(csvfile, delimiter=",", quotechar='"')  # type: ignore


class ImportTable(ImportSource[SourceModel, DestinationModel]):
    """Class to define another model as source to be used in ImportTask."""

    def __init__(self, query: Query, mapping: Mapping) -> None:
        """Constructor

        Args:
        - query (Query): Query to run - will call all() method on it
        - mapping (Mapping): Mapping
        """
        super().__init__(mapping)
        self._query = query

    @contextlib.contextmanager
    def getGeneratorData(self) -> Iterator[Iterable[SourceModel]]:
        yield self._query.all()
