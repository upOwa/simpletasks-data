# simpletasks-data

Additional tasks for [simpletasks](https://github.com/upOwa/simpletasks) to handle data.

Provides an `ImportTask` to import data into a [Flask-SQLAlchemy](https://flask-sqlalchemy.palletsprojects.com/) model, from any source of data.

Data sources provided are:
* CSV (`ImportCsv`)
* SQLAlchemy query (`ImportTable`)
Custom data sources can easily be implemented via inheriting `ImportSource`.

Other data sources are provided by other libraries:
* [gapi-helper](https://github.com/upOwa/gapi-helper) provides Google Sheets as source.


Sample:
```python
import contextlib
from typing import Iterable, Iterator, List, Optional, Sequence

import click

from simpletasks import Cli, CliParams
from simpletasks_data import ImportSource, ImportTask, Mapping

from myapp import db

@click.group()
def cli():
    pass


class Asset(db.Model):
    """Model to import to"""
    id = db.Column(db.Integer, primary_key=True)
    serialnumber = db.Column(db.String(128), index=True)
    warehouse = db.Column(db.String(128))
    status = db.Column(db.String(128))
    product = db.Column(db.String(128))
    guid = db.Column(db.String(36))


class AssetHistory(db.Model):
    """Model to keep track of changes"""
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime)
    asset_id = db.Column(db.Integer, db.ForeignKey("asset.id"), nullable=False, index=True)
    asset = db.relationship("Asset", foreign_keys=asset_id)

    old_warehouse = db.Column(db.String(128))
    new_warehouse = db.Column(db.String(128))
    old_status = db.Column(db.String(128))
    new_status = db.Column(db.String(128))


@Cli(cli, params=[CliParams.progress(), CliParams.dryrun()])
class ImportAssetsTask(ImportTask):
    class _AssetsSource(ImportSource):
        class _AssetMapping(Mapping):
            def __init__(self) -> None:
                super().__init__()

                # Defines mapping between the input data and the fields from the model
                # self.<name of the field in the model> = self.auto() -- in the order of the input data
                self.serialnumber = self.auto()
                self.status = self.auto(keep_history=True)
                self.warehouse = self.auto(keep_history=True)
                self.product = self.auto()
                self.guid = self.auto()

                # If there are gaps in the input data (i.e. fields not being used in the model), you can either:
                # - use `self.foobar = self.col()` instead of `self.foobar = self.auto()` to specify the column name after the gap
                # - use `foobar = self.auto()` to still register the gap/column, but not use it in the model

            def get_key_column_name(self) -> str:
                # By default, we use the "id" field - this overrides it
                return "serialnumber"

            def get_header_line_number(self) -> int:
                # By default we skip the first (0-index) line (header) - setting to -1 includes all lines
                return -1

        @contextlib.contextmanager
        def getGeneratorData(self) -> Iterator[Iterable[Sequence[str]]]:
            # Custom data generator
            output: List[Sequence[str]] = []

            for x in o:
                output.append([serialnumber, status, warehouse, product, guid])

            yield output

        def __init__(self) -> None:
            super().__init__(self._AssetMapping())

    def createModel(self) -> Asset:
        return Asset()

    def createHistoryModel(self, base: Asset) -> Optional[AssetHistory]:
        o = AssetHistory()
        o.asset_id = base.id
        return o

    def __init__(self, *args, **kwargs):
        super().__init__(model=Asset(), keep_history=True, *args, **kwargs)

    def get_sources(self) -> Iterable[ImportSource]:
        # Here we can have multiple sources if we wish
        return [self._AssetsSource()]
```

## Contributing

To initialize the environment:
```
poetry install --no-root
poetry install -E geoalchemy
```

To run tests (including linting and code formatting checks), please run:
```
poetry run pytest --mypy --flake8 && poetry run black --check .
```
