from typing import Tuple, List, Dict

import rshanker779_common as utils
from orm.data_structures.column import Column

from orm.exceptions import InvalidTypeData
from orm.database.postgres_db import PostgresORMDB
from orm.database.simple_db import DB
from orm.orm.query_builder import QueryBuilder, PartialSelectQuery
from orm.orm.table_creator import TableCreator
from orm.orm.table_information_builder import TableInformationBuilder
from orm.data_structures.table_information import TableInformation

logger = utils.get_logger(__name__)


class BaseMeta(type):
    known_tables: Dict[str, TableInformation] = {}

    def __new__(meta, name, bases, class_dict):
        built_object = type.__new__(meta, name, bases, class_dict)
        try:
            is_table = Base in bases
        except NameError:

            is_table = False
        if not is_table:
            return built_object

        if not hasattr(built_object, "table_name"):
            built_object.table_name = meta.get_default_table_name(name)
        table_information = TableInformationBuilder.build_table_information(
            built_object
        )
        meta.known_tables[name] = table_information

        return built_object

    @staticmethod
    def get_default_table_name(name):
        return name.lower() + "s"


class Base(utils.StringMixin, metaclass=BaseMeta):
    partial_query = None
    engine_str = None
    db = None

    def __init__(self, **kwargs):
        super().__init__()
        columns = self.get_columns(self.__class__.__dict__)
        self._columns = columns
        if columns:
            for col_name, column in columns:
                col_data = kwargs[col_name]
                col_type = column.column_type.to_python_type()
                try:
                    res = col_type(col_data)
                except TypeError as e:
                    raise InvalidTypeData(
                        f"Data {col_data} cannot be interpreted as {col_type}"
                    ) from e
                setattr(self, col_name, res)

    @classmethod
    def build(cls, engine_str: str):
        cls.engine_str = engine_str
        if "postgresql" in engine_str:
            logger.info("Using postgres db")
            cls.db = PostgresORMDB()
        else:
            logger.info("Using simple db")
            cls.db = DB()
        return cls

    @classmethod
    def get_columns(cls, class_dict=None) -> List[Tuple[str, Column]]:
        class_dict = cls.__dict__ if class_dict is None else class_dict
        return [(i, v) for i, v in class_dict.items() if isinstance(v, Column)]

    @classmethod
    def create_all_tables(cls):
        table_name_map = {
            class_name: table_information.table_name
            for class_name, table_information in cls.known_tables.items()
        }
        for class_name, table_information in cls.known_tables.items():
            table_information.update_column_dependencies(table_name_map)
        table_creator = TableCreator(cls.known_tables)
        creation_sql = table_creator.generate_table_sql()
        cls.execute(creation_sql)

    def save(self):
        logger.info(f"Saving data {self} using engine {self.engine_str}")
        sql_insert = QueryBuilder.build_sql_insert_statements(
            self, self.table_name, self._columns
        )
        self.execute(sql_insert)

    @classmethod
    def execute(cls, sql_insert):
        return cls.db.parse_sql(sql_insert)

    @classmethod
    def initialise_partial_query(cls):
        table_name = cls.table_name
        col_names = [i for i, _ in cls.get_columns()]
        cls.partial_query = QueryBuilder.build_partial_select_query(
            table_name, col_names,
        )

    @classmethod
    def query(cls):
        cls.initialise_partial_query()
        return cls

    @classmethod
    def filter_by(cls, **kwargs):
        full_query = cls.partial_query.filter_by(**kwargs)
        result = cls.execute(full_query)
        return cls._parse_result(cls.partial_query, result)

    @classmethod
    def all(cls):
        full_query = cls.partial_query.all()
        result = cls.execute(full_query)
        return cls._parse_result(cls.partial_query, result)

    @classmethod
    def _parse_result(cls, partial_query, result):
        returned_classes = []
        for res in result:
            kwargs = {i: v for i, v in zip(partial_query.col_names, res)}
            returned_classes.append(cls(**kwargs))
        return returned_classes
