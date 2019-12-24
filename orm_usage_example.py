from example_orm.orm import Base, Column, ColumnTypes
from example_orm.simple_db import db

MyBase = Base.build("test_str")


class Post(MyBase):
    id = Column(ColumnTypes.int, primary_key=True)
    content = Column(ColumnTypes.string)
    user_id = Column(ColumnTypes.int, foreign_key="user.id")


class User(MyBase):
    id = Column(ColumnTypes.int, primary_key=True)
    name = Column(ColumnTypes.string)


class Message(MyBase):
    id = Column(ColumnTypes.int, primary_key=True)
    sending_user_id = Column(ColumnTypes.int, foreign_key="user.id")
    receiving_user_id = Column(ColumnTypes.int, foreign_key="user.id")
    content = Column(ColumnTypes.string)


class Reply(MyBase):
    id = Column(ColumnTypes.int, primary_key=True)
    post_id = Column(ColumnTypes.int, foreign_key="post.id")
    content = Column(ColumnTypes.string)


def main():
    MyBase.create_all_tables()
    print(db)
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
    print(users)


if __name__ == "__main__":
    main()
