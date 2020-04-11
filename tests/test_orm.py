import pytest

from tests.conftest import build_base, MyBase, User


@pytest.mark.parametrize("engine_string", ["simple", "postgresql"])
def test_db(engine_string):
    build_base(engine_string)
    MyBase.create_all_tables()
    user = User(id=1, name="a")
    user.save()
    users = User.query().filter_by(name="a")
    assert len(list(users)) == 1
    users = User.query().filter_by(id=1)
    assert len(list(users)) == 1
    users = User.query().filter_by(name="b")
    assert len(list(users)) == 0
    user = User(id=2, name="b")
    user.save()
    users = User.query().filter_by(name="b")
    assert len(list(users)) == 1
    users = User.query().all()
    assert len(list(users)) == 2


@pytest.fixture
def before():
    build_base()
    yield


@pytest.mark.parametrize("table_name", ["users", "posts", "messages", "replies"])
def test_table_exists(before, table_name):
    MyBase.create_all_tables()
    res = MyBase.db.parse_sql(f"select * from {table_name};")
    assert not list(res)
