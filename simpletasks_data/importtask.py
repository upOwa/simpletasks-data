import abc
import datetime
from typing import Any, Dict, Generic, Iterable, MutableSequence, Optional, Set, Tuple, cast

from flask_sqlalchemy import SQLAlchemy
from simpletasks import Task

from .importsource import ImportMode, ImportSource
from .mapping import DestinationModel, DestinationModelHistory, _Caching


class ImportTask(Task, Generic[DestinationModel, DestinationModelHistory], metaclass=abc.ABCMeta):
    """Task to import data into a DestinationModel, from one or several sources.
    Supports creation of historical data to track changes - see createHistoryModel.
    """

    db: Optional[SQLAlchemy] = None

    @staticmethod
    def configure(db: SQLAlchemy) -> None:
        """Configures the DB to work on. It is mandatory to call it once before executing any ImportTask.

        Args:
        - db (SQLAlchemy): SQLAlchemy object
        """
        ImportTask.db = db

    @abc.abstractmethod
    def createModel(self) -> DestinationModel:
        """Method to implement in concrete class to initialize a new item.

        Returns:
        - DestinationModel: New item
        """
        raise NotImplementedError  # pragma: no cover

    def createHistoryModel(self, base: DestinationModel) -> Optional[DestinationModelHistory]:
        """Method to implement in a concrete class to initialize a new item to track changes.

        For each field that is tracked in the original item, this item must have 2 fields:
        - "old_" + name
         "new_" + name

        Args:
        - base (DestinationModel): Original item

        Returns:
        - Optional[DestinationModelHistory]: New item, or None if not supported
        """
        return None

    def validate_updates(self, model: DestinationModel, updates: Dict[str, Any], creating: bool) -> bool:
        """Method that can be implemented in the concrete class to filter-out rows once imported.

        By default, items that have None values on non-nullable columns are rejected.

        Args:
        - model (DestinationModel): Item to update/create
        - updates (Dict[str, Any]): All updates to apply
        - creating (bool): True if creating the item

        Returns:
        - bool: True if should apply updates, or False if should skip this item
        """
        for column in self.nonNullableColumns:
            if self.get_updated_value_for(model, column) is None:
                self.logger.warning("Rejecting update of '{}': {} is None".format(model, column))
                return False

        return True

    @abc.abstractmethod
    def get_sources(self) -> Iterable[ImportSource]:
        """Method to implement to define the sources.

        Returns:
        - Iterable[ImportSource]: List of sources to use (will be used in that order)
        """
        raise NotImplementedError  # pragma: no cover

    def pre_process(self) -> Dict[str, int]:
        """Method that can be implemented to do some processing before sources are parsed.

        Returns:
        - Dict[str, int]: Any valuable info, will be returned by do
        """
        return {}

    def post_process(self) -> Dict[str, int]:
        """Method that can be implemented to do some processing after sources are parsed and before updates are applied.

        Returns:
        - Dict[str, int]: Any valuable info, will be returned by do
        """
        return {}

    def pre_commit(self) -> Dict[str, int]:
        """Method that can be implemented to do some processing after updates are applied and before commit in DB is done.

        Returns:
        - Dict[str, int]: Any valuable info, will be returned by do
        """
        return {}

    def post_commit(self) -> Dict[str, int]:
        """Method that can be implemented to do some processing after commit in DB is done.

        Returns:
        - Dict[str, int]: Any valuable info, will be returned by do
        """
        return {}

    def get_model_data(self) -> MutableSequence[DestinationModel]:
        """Method that can be implemented to return the items from the model.

        By default, will query all data from the model.
        It can be useful to re-implement this to filter-out data that is already imported and we know won't be present in any of the sources.

        Returns:
        - MutableSequence[DestinationModel]: Items from the model
        """
        return self._model.query.all()

    def is_updated(self, item: DestinationModel, column: str) -> bool:
        """Returns whether an item has pending updates for a column.

        Args:
        - item (DestinationModel): Item
        - column (str): Name of the column

        Returns:
        - bool: True if there are pending updates for this column
        """
        return item in self.updates and column in self.updates[item][0]

    def get_updated_value_for(self, item: DestinationModel, column: str) -> Any:
        """Returns the value for a column - if not updated, returns the initial value.

        Args:
        - item (DestinationModel): Item
        - column (str): Name of the column

        Returns:
        - Any: Value (can be None)
        """
        return (
            self.updates[item][0][column]
            if item in self.updates and column in self.updates[item][0]
            else getattr(item, column)
        )

    def set_updated_value_for(
        self, item: DestinationModel, column: str, value: Any, keep_history: bool = False
    ) -> None:
        """Sets the updated value for a column.

        Args:
        - item (DestinationModel): Item to update
        - column (str): Name of the column
        - value (Any): New value
        - keep_history (bool, optional): True to keep history of the change (via createHistoryModel). Defaults to False.
        """
        if item not in self.updates:
            self.updates[item] = ({}, set(), False)
        self.updates[item][0][column] = value
        if keep_history:
            self.updates[item][1].add(column)

    def cancel_updated_value_for(self, item: DestinationModel, column: str) -> None:
        """Cancels an update for a column.

        Args:
        - item (DestinationModel): Item to update
        - column (str): Name of the column
        """
        del self.updates[item][0][column]
        self.updates[item][1].discard(column)
        if not self.updates[item][0]:
            del self.updates[item]

    def __init__(self, model: DestinationModel, keep_history: bool = False, *args, **kwargs):
        """Initializes the task

        Args:
        - model (DestinationModel): Model to create/update
        - keep_history (bool, optional): True to enable creation of historical data. Defaults to False.

        Raises:
        - RuntimeError: configure was not called
        """
        super().__init__(*args, **kwargs)
        self._model = model
        self._keep_history = keep_history
        if ImportTask.db is None:
            raise RuntimeError("ImportTask db is not configured")

    def _parseSource(self, source: ImportSource) -> Dict[str, int]:
        read = 0
        ignored = 0
        ignored_missing_id = 0
        ignored_not_created = 0
        ignored_not_updated = 0
        rejected = 0
        not_found = 0

        logger = self.logger.getChild(source.name)

        source._set_parent(self)
        source._fill_mapping(self._model)

        data: Dict[Any, DestinationModel] = {}
        dataNotRead: Set[DestinationModel] = set()
        for x in self.rawData:
            key = source.get_key_from_model(self.get_updated_value_for(x, source.get_keycolumn_name()))
            data[key] = x
            dataNotRead.add(x)

        with source.getGeneratorData() as csvreader:
            for line_idx, row in self.progress(enumerate(csvreader), desc="Reading {}".format(source.name)):
                if line_idx <= source.get_header_line_number():
                    continue

                if not source.should_import(row):
                    ignored += 1
                    continue

                _Caching.values.clear()

                id = source.get_key(row)
                if id is None:
                    ignored_missing_id += 1
                    continue

                if id not in data:
                    if not (source.mode & ImportMode.CREATE):
                        ignored_not_created += 1
                        continue
                    item = self.createModel()
                    # Don't add the item right now in the DB, because we first need to set all fields
                    # (can create Non-Null violations in DB)
                    creating = True
                    self.updates[item] = ({}, set(), True)
                else:
                    item = data[id]
                    dataNotRead.discard(item)
                    if not (source.mode & ImportMode.UPDATE):
                        ignored_not_updated += 1
                        continue
                    creating = False

                for name, _column in source.get_columns():
                    if not creating and not _column.should_update:
                        # Ignore this field when updating
                        continue

                    old_value = self.get_updated_value_for(item, name)

                    if not creating and _column.should_update_only_if_null and old_value is not None:
                        # Ignore this field when updating
                        continue

                    try:
                        new_value = _column.get(row)
                        if not new_value and _column.warn_if_empty:
                            logger.warning("{} has en empty value for {} (row {})".format(name, id, line_idx))
                        if not (_column.comparator(new_value, old_value)):
                            real_old_value = getattr(item, name)
                            if _column.comparator(new_value, real_old_value):
                                # This can happen if there are duplicates
                                self.cancel_updated_value_for(item, name)
                            else:
                                self.set_updated_value_for(
                                    item,
                                    name,
                                    new_value,
                                    not creating and _column.keep_history,
                                )
                    except (ValueError, KeyError, AttributeError) as e:
                        if _column.warn_on_error:
                            logger.warning(
                                "Row {},{} has invalid value for {}: {} -> {} {}".format(
                                    line_idx, id, name, _column.get_raw_values(row), e.__class__.__name__, e
                                )
                            )

                read += 1
                if item in self.updates and not source.validate_updates(
                    item, row, self.updates[item][0], creating
                ):
                    rejected += 1
                    del self.updates[item]
                    continue

                if creating:
                    self.rawData.append(item)
                    if id is not None:
                        data[id] = item

        for x in dataNotRead:
            source.on_data_not_found(x)
            not_found += 1

        return {
            "read": read,
            "ignored": ignored,
            "ignored_missing_id": ignored_missing_id,
            "ignored_not_created": ignored_not_created,
            "ignored_not_updated": ignored_not_updated,
            "rejected": rejected,
            "not_found": not_found,
        }

    def _apply_updates_for(self, item) -> Dict[str, int]:
        res = {"rejected": 0, "created": 0, "updated": 0, "history_created": 0}
        if item not in self.updates:
            return res

        updates, keep_history, creating = self.updates[item]
        if not self.validate_updates(item, updates, creating):
            res["rejected"] += 1
            return res

        item_updated = False
        item_history = None

        for name, new_value in updates.items():
            if self._keep_history and name in keep_history:
                if not item_history:
                    item_history = self.createHistoryModel(item)
                setattr(item_history, "old_" + name, getattr(item, name))
                setattr(item_history, "new_" + name, new_value)
            setattr(item, name, new_value)
            item_updated = True

        if creating:
            res["created"] += 1
            cast(SQLAlchemy, ImportTask.db).session.add(item)
        elif item_updated:
            res["updated"] += 1
            if item_history:
                item_history.date = datetime.datetime.now()  # type: ignore
                cast(SQLAlchemy, ImportTask.db).session.add(item_history)
                res["history_created"] += 1
        return res

    def _read(self) -> Dict[str, Any]:
        rejected = 0
        updated = 0
        created = 0
        history_created = 0

        self.updates: Dict[DestinationModel, Tuple[Dict[str, Any], Set[str], bool]] = {}

        results: Dict[str, Any] = {"sources": []}

        for source in self.get_sources():
            res = self._parseSource(source)
            results["sources"].append(res)

        results["postprocess"] = self.post_process()

        for item in self.progress(list(self.updates.keys()), desc="Applying updates"):
            u = self._apply_updates_for(item)
            rejected += u["rejected"]
            created += u["created"]
            updated += u["updated"]
            history_created += u["history_created"]

        results["precommit"] = self.pre_commit()
        self.execute(lambda: cast(SQLAlchemy, ImportTask.db).session.commit())
        results["postcommit"] = self.post_commit()

        results["rejected"] = rejected
        results["updated"] = updated
        results["created"] = created
        results["history_created"] = history_created
        return results

    def do(self) -> Dict[str, Any]:
        results = {}
        results["preprocess"] = self.pre_process()
        self.rawData = self.get_model_data()

        self.nonNullableColumns = set()
        for columnName, columnModel in self._model.__table__.c.items():  # type: ignore
            if not columnModel.nullable and not columnModel.primary_key:
                # Check non-nullable columns that are not primary keys
                # This assumes primary keys are auto-generated
                # TODO: find a better way
                if columnModel.default is None and columnModel.server_default is None:
                    self.nonNullableColumns.add(columnName)

        results.update(self._read())
        self.logger.info("Done: {}".format(results))
        return results
