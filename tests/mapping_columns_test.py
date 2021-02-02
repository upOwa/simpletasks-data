from typing import TYPE_CHECKING, Any, Sequence

import pytest
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from simpletasks_data.formatting import str2intnullable
from simpletasks_data.mapping import Column, ComputedColumn, Field, Mapping, StaticColumn, _Caching

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
def clearCaching():
    _Caching.values.clear()
    yield None
    _Caching.values.clear()


def test_column(clearCaching) -> None:
    col = Column(0, str2intnullable)
    col2 = Column(1, str2intnullable)
    assert col.get_raw_values(["1", "2"]) == ["1"]
    assert col2.get_raw_values(["1", "2"]) == ["2"]

    assert col.get(["1", "2"]) == 1
    assert col2.get(["1", "2"]) == 2
    assert _Caching.values[col] == 1
    assert _Caching.values[col2] == 2
    _Caching.values.clear()
    assert col.get(["-1", "-2"]) == -1
    assert col2.get(["-1", "-2"]) == -2
    assert _Caching.values[col] == -1
    assert _Caching.values[col2] == -2
    _Caching.values.clear()
    assert col.get(["", "1"]) is None
    assert col2.get(["2", ""]) is None
    assert _Caching.values[col] is None
    assert _Caching.values[col2] is None
    _Caching.values.clear()


def test_mappedcolumn(clearCaching) -> None:
    class MyMapping(Mapping[MyModel]):
        def __init__(self) -> None:
            super().__init__()
            self.id = self.col("A")
            self.col1 = self.auto()

    mapping = MyMapping()
    mapping._complete_from_model(MyModel())
    assert mapping.id.column_number == 0
    assert mapping.col1.column_number == 1

    o1 = ["1", ""]

    assert mapping.id.get_raw_values(o1) == ["1"]
    assert mapping.id.get(o1) == 1
    assert _Caching.values[mapping.id] == 1

    assert mapping.col1.get_raw_values(o1) == [""]
    assert mapping.col1.get(o1) is None
    assert _Caching.values[mapping.col1] is None
    _Caching.values.clear()

    o2 = ["2", "E"]
    assert mapping.col1.get_raw_values(o2) == ["E"]
    assert mapping.col1.get(o2) == "E"
    assert _Caching.values[mapping.col1] == "E"
    _Caching.values.clear()


def test_column_outofrange(clearCaching) -> None:
    col = Column(0, lambda x: x)
    with pytest.raises(IndexError) as e:
        assert col.get_raw_values([]) is None
    assert str(e.value) == "list index out of range"

    with pytest.raises(IndexError) as e:
        assert col.get([]) is None
    assert str(e.value) == "list index out of range"
    assert col not in _Caching.values

    col2 = Column(0, lambda x: x, fail_on_out_of_range=False)
    assert col2.get_raw_values([]) == [""]
    assert col2.get([]) == ""
    assert _Caching.values[col2] == ""
    _Caching.values.clear()


def test_computedcolumn(clearCaching) -> None:
    def my_computer(x: Sequence[Any]) -> int:
        return (x[0] if x[0] is not None else 0) + (x[1] if x[1] is not None else 0)

    col = Column(0, str2intnullable)
    col2 = Column(1, str2intnullable)
    computedcol = ComputedColumn([col, col2], computer=my_computer)
    assert computedcol.get_raw_values(["1", "2"]) == ["1", "2"]

    assert computedcol.get(["1", "2"]) == 3
    assert _Caching.values[col] == 1
    assert _Caching.values[col2] == 2
    assert _Caching.values[computedcol] == 3
    _Caching.values.clear()
    assert computedcol.get(["1", ""]) == 1
    assert _Caching.values[col] == 1
    assert _Caching.values[col2] is None
    assert _Caching.values[computedcol] == 1
    _Caching.values.clear()
    assert computedcol.get(["", "2"]) == 2
    assert _Caching.values[col] is None
    assert _Caching.values[col2] == 2
    assert _Caching.values[computedcol] == 2
    _Caching.values.clear()


def test_staticcolumn(clearCaching) -> None:
    col = StaticColumn(1)
    assert col.get_raw_values([]) == ["1"]
    assert col.get([]) == 1


class MyModel(BaseModel):
    id = db.Column(db.Integer, primary_key=True)
    col1 = db.Column(db.String(1), nullable=False)
    col2 = db.Column(db.String(256))
    col3 = db.Column(db.String(10))
    col4 = db.Column(db.String(10))
    col5 = db.Column(db.Integer)


def test_field(clearCaching) -> None:
    col: Field[str, str, MyModel] = Field("col1", parser=lambda x: x)
    o1 = MyModel(id=1, col1="", col2="", col3="C", col4="D")
    assert col.get_raw_values(o1) == [""]
    assert col.get(o1) == ""
    assert _Caching.values[col] == ""
    _Caching.values.clear()

    o2 = MyModel(id=2, col1="E", col2="F", col3="G", col4="H", col5=2)
    assert col.get_raw_values(o2) == ["E"]
    assert col.get(o2) == "E"
    assert _Caching.values[col] == "E"
    _Caching.values.clear()


def test_mappedfield(clearCaching) -> None:
    class MyMapping(Mapping[MyModel]):
        def __init__(self) -> None:
            super().__init__()
            self.id: Field[int, int, MyModel] = Field()
            self.col1: Field[str, str, MyModel] = Field()

    mapping = MyMapping()
    o1 = MyModel(id=1, col1="", col2="", col3="C", col4="D")
    mapping._complete_from_model(o1)
    assert mapping.id.field == "id"
    assert mapping.col1.field == "col1"

    assert mapping.id.get_raw_values(o1) == ["1"]
    assert mapping.id.get(o1) == 1
    assert _Caching.values[mapping.id] == 1

    assert mapping.col1.get_raw_values(o1) == [""]
    assert mapping.col1.get(o1) == ""
    assert _Caching.values[mapping.col1] == ""
    _Caching.values.clear()

    o2 = MyModel(id=2, col1="E", col2="F", col3="G", col4="H", col5=2)
    assert mapping.col1.get_raw_values(o2) == ["E"]
    assert mapping.col1.get(o2) == "E"
    assert _Caching.values[mapping.col1] == "E"
    _Caching.values.clear()
