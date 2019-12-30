from orm import Base, Column, ColumnTypes

MyBase = Base.build("a")


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
    id = Column(ColumnTypes.Int, primary_key=True)
    post_id = Column(ColumnTypes.Int, foreign_key="post.id")
    content = Column(ColumnTypes.String)


def main():
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
    print(users)


if __name__ == "__main__":
    main()
