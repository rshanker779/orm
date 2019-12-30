from enum import Enum
from typing import Tuple, List, Iterable

from postgres_db import PostgresORMDB
from simple_db import DB
from utilities import StringMixin


class ColumnTypes(Enum):
    Int = 0
    String = 1

    def to_sql_type(self) -> str:
        if self == ColumnTypes.Int:
            return "Int"
        elif self == ColumnTypes.String:
            return "Varchar"

    def to_python_type(self) -> type:
        if self == ColumnTypes.Int:
            return int
        elif self == ColumnTypes.String:
            return str


class Column:
    def __init__(
        self, column_type: ColumnTypes, foreign_key: str = None, primary_key=False
    ):
        self.column_type = column_type
        self.foreign_key = foreign_key
        self.primary_key = primary_key


class ColumnDependency:
    def __init__(self, col_name, dependency_table_name, dependency_table_column):
        self.col_name = col_name
        self.dependency_table_name = dependency_table_name
        self.dependency_table_column = dependency_table_column


class DependencyChain:
    def __init__(self, name, columns, *args: ColumnDependency):
        self.name = name
        self.columns = columns
        self.dependencies = args


class DependencyProcessingException(Exception):
    pass


class InvalidTypeData(Exception):
    pass


class BaseMeta(type):
    known_tables = {}
    allowed_supers = [StringMixin]

    def __new__(meta, name, bases, class_dict):
        is_potential_table = bases != tuple() and not all(
            i in meta.allowed_supers for i in bases
        )
        if is_potential_table and Base in bases:
            if any(isinstance(v, Column) for _, v in class_dict.items()):
                meta.known_tables[name] = class_dict
        return type.__new__(meta, name, bases, class_dict)


class Base(StringMixin, metaclass=BaseMeta):
    dependencies = {}
    sql_creation_code = []
    processed_tables = set()

    def __init__(self, **kwargs):
        super().__init__()
        columns = self.get_columns(self.__class__.__dict__)
        if columns:
            for col_name, column in columns:
                col_data = kwargs[col_name]
                col_type = column.column_type.to_python_type()
                try:
                    res = col_type(col_data)
                except TypeError as e:
                    raise InvalidTypeData(
                        "Data {} cannot be interpreted as {}".format(col_data, col_type)
                    ) from e
                setattr(self, col_name, res)

    @classmethod
    def build(cls, engine_str: str):
        cls.engine_str = engine_str
        if "postgresql" in engine_str:
            print("Using postgres db")
            cls.db = PostgresORMDB()
        else:
            print("Using simple db")
            cls.db = DB()
        return cls

    @classmethod
    def get_table_name(cls, table_name: str) -> str:
        return table_name.lower() + "s"

    @classmethod
    def get_columns(cls, class_dict=None) -> List[Tuple[str, Column]]:
        class_dict = cls.__dict__ if class_dict is None else class_dict
        return [(i, v) for i, v in class_dict.items() if isinstance(v, Column)]

    @classmethod
    def create_all_tables(cls):
        creation_sql = cls.generate_table_sql()
        cls.execute(creation_sql)

    @classmethod
    def generate_single_sql_creation_statement(
        cls, table_name, columns, dependencies=None
    ) -> str:
        base_sql = "create table {} ( ".format(cls.get_table_name(table_name))
        primary_keys = []
        for column_name, v in columns:
            if not isinstance(v, Column):
                continue
            base_sql += "{} {},".format(column_name, v.column_type.to_sql_type())
            if v.primary_key:
                primary_keys.append(column_name)
        base_sql += "PRIMARY KEY ({}) ".format(",".join(primary_keys))
        foreign_key_sql = ""
        if dependencies:
            for column_dependency in dependencies:
                foreign_key_sql += ",FOREIGN KEY ({}) REFERENCES {} ({})".format(
                    column_dependency.col_name,
                    cls.get_table_name(column_dependency.dependency_table_name),
                    column_dependency.dependency_table_column,
                )
        base_sql += foreign_key_sql + ");"
        return base_sql

    @classmethod
    def generate_table_sql(cls) -> str:
        cls.build_table_dependencies()
        cls.build_sql_creation_statements()
        sql_creation = ""
        for i in cls.sql_creation_code:
            sql_creation += "begin; {} commit;".format(i)
        return sql_creation

    @classmethod
    def build_sql_creation_statements(cls):
        for table, dependency_chain in cls.dependencies.items():

            sql_statement = cls.generate_single_sql_creation_statement(
                table, dependency_chain.columns, dependency_chain.dependencies
            )
            if not dependency_chain.dependencies or all(
                i.dependency_table_name in cls.processed_tables
                for i in dependency_chain.dependencies
            ):
                cls.sql_creation_code.append(sql_statement)
                cls.processed_tables.add(table)
        if len(cls.sql_creation_code) != len(cls.dependencies.items()):
            cls.build_sql_creation_statements()

    @classmethod
    def build_table_dependencies(cls):
        for table_name, attributes in cls.known_tables.items():
            if table_name.lower() in cls.dependencies:
                continue
            columns = cls.get_columns(attributes)
            full_dependencies = [
                (i, v) for i, v in columns if v.foreign_key is not None
            ]
            if not full_dependencies:
                cls.dependencies[table_name.lower()] = DependencyChain(
                    table_name.lower(), columns
                )
                continue
            try:
                simple_dependencies = []
                for col_name, dependency in full_dependencies:
                    str_dependency = dependency.foreign_key
                    dependency_table_name, dependency_col_name = str_dependency.split(
                        "."
                    )
                    if dependency_table_name not in cls.dependencies:
                        # We have no idea the order that the tables are given to us.
                        # So we will skip if there is a dependency not there, will pick up
                        # in recursion
                        raise DependencyProcessingException(
                            "Have not processed dependency {} for table {}".format(
                                dependency_table_name, table_name
                            )
                        )
                    column_dependency = ColumnDependency(
                        col_name, dependency_table_name, dependency_col_name
                    )
                    simple_dependencies.append(column_dependency)
                cls.dependencies[table_name.lower()] = DependencyChain(
                    table_name.lower(), columns, *simple_dependencies
                )
            except DependencyProcessingException as e:
                print(e)
                continue
        if len(cls.dependencies) != len(cls.known_tables):
            return cls.build_table_dependencies()

    def save(self):
        print("Saving data {} using engine {}".format(self, self.__class__.engine_str))
        sql_insert = self.build_sql_insert_statements(self)
        self.execute(sql_insert)

    @classmethod
    def execute(cls, sql_insert):
        return cls.db.parse_sql(sql_insert)

    @classmethod
    def build_sql_insert_statements(cls, instance):
        table_name = cls.get_table_name(instance.__class__.__name__)
        sql_insert = "insert into {} ".format(table_name)
        col_names = []
        values = []
        columns = cls.get_columns()
        for col_name, _ in columns:
            value = getattr(instance, col_name)
            col_names.append(col_name)
            values.append(value)
        sql_insert += "({}) values ({});".format(
            ",".join(str(i) for i in col_names),
            ",".join("'{}'".format(i) for i in values),
        )
        return sql_insert

    @classmethod
    def query(cls):
        table_name = cls.get_table_name(cls.__name__)
        col_names = [i for i, _ in cls.get_columns()]
        return Query(table_name, col_names, cls)


class Query:
    def __init__(self, table_name: str, col_names: Iterable[str], table_class: Base):
        self.table_name = table_name
        self.col_names = col_names
        self.query = "select {} from {}".format(",".join(col_names), table_name)
        self.table_class = table_class

    def query_executor(self, query):
        return self._parse_result(self.table_class.execute(query))

    def filter_by(self, **kwargs):
        return self.query_executor(self._get_filter_by_query(**kwargs))

    def all(self):
        return self.query_executor(self._get_all_query())

    def _get_filter_by_query(self, **kwargs):
        filters = ["{}='{}'".format(i, v) for i, v in kwargs.items()]

        self.query += " where {};".format("and".join(filters))

        return self.query

    def _get_all_query(self):
        self.query += ";"
        return self.query

    def _parse_result(self, result):
        returned_classes = []
        for res in result:
            kwargs = {i: v for i, v in zip(self.col_names, res)}
            returned_classes.append(self.table_class(**kwargs))
        return returned_classes
