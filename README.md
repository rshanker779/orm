# ORM

Example of how to create (a subset) of ORM behaviour using metaclasses in Python 
(mimicking the excellent SQLAlchemy).

## Usage

```python
from orm import Base, ColumnTypes, Column

#Create Base- the string argument can be postgresql, anything else will default to a basic in memory db
MyBase = Base.build('in-memory')

#Construct our tables

class Post(MyBase):
    id = Column(ColumnTypes.Int, primary_key=True)
    content = Column(ColumnTypes.String)
    user_id = Column(ColumnTypes.Int, foreign_key="user.id")


class User(MyBase):
    id = Column(ColumnTypes.Int, primary_key=True)
    name = Column(ColumnTypes.String)

MyBase.create_all_tables()
#Creating a user, and some simple queries
user = User(id=1, name="a")
user.save()

users = User.query().filter_by(name="a")
assert len(list(users)) == 1

user = User(id=2, name="b")
user.save()

users = User.query().all()
assert len(list(users)) == 2
```

Note SQLAlchemy is a dependency- but is used only for connection logic to 
a postgres DB, and not for any of its ORM features. 