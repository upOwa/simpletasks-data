import contextlib
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, MutableSequence, Optional, Sequence

import pytest
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from simpletasks_data.importsource import ImportMode
from simpletasks_data.importtask import ImportSource, ImportTask
from simpletasks_data.mapping import Mapping

app = Flask(__name__)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
db = SQLAlchemy(app)
if TYPE_CHECKING:
    from flask_sqlalchemy.model import DeclarativeMeta, Model

    BaseModel: DeclarativeMeta = db.make_declarative_base(Model)
else:
    BaseModel = db.Model


@pytest.fixture(scope="function")
def service():
    db_old = ImportTask.db
    yield db
    ImportTask.db = db_old


class MyModel(BaseModel):
    id = db.Column(db.Integer, primary_key=True)
    col1 = db.Column(db.String(1), nullable=False)
    col2 = db.Column(db.String(256))
    col3 = db.Column(db.String(10))
    col4 = db.Column(db.String(10))
    col5 = db.Column(db.Integer)
    col6 = db.Column(db.Integer)


class MyModelHistory(BaseModel):
    id = db.Column(db.Integer, primary_key=True)
    model_id = db.Column(db.Integer)
    old_col1 = db.Column(db.String(256))
    new_col1 = db.Column(db.String(256))


class MyTask(ImportTask):
    class _MySource1(ImportSource):
        class _MyMappingA(Mapping):
            def __init__(self) -> None:
                super().__init__()

                self.id = self.auto()
                self.col1 = self.auto(keep_history=True)
                self.col2 = self.auto(should_update=False)
                self.col3 = self.auto()
                self.col4 = self.auto()
                self.col5 = self.auto(warn_on_error=False)

        def __init__(self) -> None:
            super().__init__(self._MyMappingA())

        @contextlib.contextmanager
        def getGeneratorData(self) -> Iterator[Iterable[Sequence[str]]]:
            output: List[Sequence[str]] = [
                # Entête
                ["0", "0", "0", "0", "0", "0"],
                # col1: trimmed + mis à jour + keep_history
                # col5: mis à jour sans keep_history
                ["1", "ABCDEFG", "B", "C", "D", "1"],
                # Déja présent
                ["2", "E", "F", "G", "H", "2"],
                # Nouveau, col5: valeur invalide
                ["3", "I", "J", "K", "L", "a"],
                # Nouveau, id manquant
                ["", "I", "J", "K", "L", "3"],
            ]
            yield output

        def validate_updates(
            self, model: MyModel, row: Sequence[str], updates: Dict[str, Any], creating: bool
        ) -> bool:
            if row[0] == "1":
                assert updates == {"col1": "A", "col5": 1}
                assert creating is False
            elif row[0] == "3":
                assert updates == {"id": 3, "col1": "I", "col2": "J", "col3": "K", "col4": "L"}
                assert creating is True
            else:
                assert False

            return True

        @property
        def mode(self) -> ImportMode:
            return ImportMode.CREATE_AND_UPDATE

    class _MySource2(ImportSource):
        class _MyMappingB(Mapping):
            def __init__(self) -> None:
                super().__init__()

                self.id = self.auto(should_update=False)
                self.col6 = self.auto(should_update_only_if_null=True)

        def __init__(self) -> None:
            super().__init__(self._MyMappingB())

        @contextlib.contextmanager
        def getGeneratorData(self) -> Iterator[Iterable[Sequence[str]]]:
            output: List[Sequence[str]] = [
                ["1", "2"],
                ["2", "2"],
                ["3", "2"],
                ["5", ""],  # Ignored
            ]
            yield output

        def get_header_line_number(self) -> int:
            return -1

        def validate_updates(
            self, model: MyModel, row: Sequence[str], updates: Dict[str, Any], creating: bool
        ) -> bool:
            if row[0] == "1":
                assert updates == {"col1": "A", "col5": 1}
                assert creating is False
            elif row[0] == "3":
                assert updates == {"id": 3, "col1": "I", "col2": "J", "col3": "K", "col4": "L", "col6": 2}
                assert creating is False
            else:
                assert False

            return True

        @property
        def mode(self) -> ImportMode:
            return ImportMode.UPDATE

    def createModel(self) -> MyModel:
        return MyModel()

    def createHistoryModel(self, base: MyModel) -> Optional[MyModelHistory]:
        o = MyModelHistory()
        o.model_id = base.id
        return o

    def __init__(self, *args, **kwargs):
        super().__init__(model=MyModel(), keep_history=True, *args, **kwargs)

    def get_sources(self) -> Iterable[ImportSource]:
        return [self._MySource1(), self._MySource2()]

    def get_model_data(self) -> MutableSequence[MyModel]:
        return [
            MyModel(id=1, col1="", col2="", col3="C", col4="D", col6=1),
            MyModel(id=2, col1="E", col2="F", col3="G", col4="H", col5=2, col6=1),
        ]

    def pre_process(self) -> Dict[str, int]:
        return {"bar": 1}

    def post_process(self) -> Dict[str, int]:
        assert len(self.rawData) == 3
        return {"foo": 0}


def test_nominal(service) -> None:
    ImportTask.db = service
    task = MyTask(dryrun=True)
    res = task.run()
    assert res == {
        "created": 1,
        "history_created": 1,
        "postprocess": {"foo": 0},
        "preprocess": {"bar": 1},
        "postcommit": {},
        "precommit": {},
        "rejected": 0,
        "updated": 1,
        "sources": [
            {
                "ignored": 0,
                "ignored_missing_id": 1,
                "ignored_not_created": 0,
                "ignored_not_updated": 0,
                "read": 3,
                "rejected": 0,
                "not_found": 0,
            },
            {
                "ignored": 0,
                "ignored_missing_id": 0,
                "ignored_not_created": 1,
                "ignored_not_updated": 0,
                "read": 3,
                "rejected": 0,
                "not_found": 0,
            },
        ],
    }
