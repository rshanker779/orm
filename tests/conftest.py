from orm import Base, Column, ColumnTypes

default_engine_string = "postgresql"


def build_base(engine_string=None):
    if engine_string is None:
        engine_string = default_engine_string
    MyBase = Base.build(engine_string)
    return MyBase


MyBase = build_base()


class Post(MyBase):
    id = Column(ColumnTypes.Int, primary_key=True)
    content = Column(ColumnTypes.String)
    user_id = Column(ColumnTypes.Int, foreign_key="user.id")


class User(MyBase):
    id = Column(ColumnTypes.Int, primary_key=True)
    name = Column(ColumnTypes.String)


class Message(MyBase):
    id = Column(ColumnTypes.Int, primary_key=True)
    sending_user_id = Column(ColumnTypes.Int, foreign_key="user.id")
    receiving_user_id = Column(ColumnTypes.Int, foreign_key="user.id")
    content = Column(ColumnTypes.String)


class Reply(MyBase):
    table_name = "replies"
    id = Column(ColumnTypes.Int, primary_key=True)
    post_id = Column(ColumnTypes.Int, foreign_key="post.id")
    content = Column(ColumnTypes.String)
