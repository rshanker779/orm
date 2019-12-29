from typing import Iterable, Dict, Any

from example_orm.orm_db import ORMDB
from example_orm.utilities import StringMixin


class IncorrectColumnError(Exception):
    pass


class Column(StringMixin):
    def __init__(self, name: str, data_type: type):
        super().__init__()
        name = name.strip()
        self.name = name
        self.data_type = data_type


class Row(StringMixin):
    def __init__(self, data: Dict[str, Any]):
        super().__init__()
        self.col_names = set(data.keys())
        for i, v in data.items():
            setattr(self, i, v)


class Table(StringMixin):
    def __init__(self, table_name, columns: Iterable[Column]):
        super().__init__()
        table_name = table_name.strip()
        self.name = table_name
        self.columns = columns
        self.col_names = {i.name for i in columns}
        self.rows = []

    def add_row(self, row: Row):
        if set(row.col_names) != self.col_names:
            raise IncorrectColumnError(
                "Row names {} do not match columns {}".format(
                    row.data.keys(), self.col_names
                )
            )
        self.rows.append(row)


class DB(StringMixin, ORMDB):
    def __init__(self):
        super().__init__()
        self.sql_parser = _SQLParser()
        self.tables = {}  # type: Dict[str, Table]

    def _add_table(self, table: Table):
        self.tables[table.name] = table

    def get_table(self, table_name: str) -> Table:
        return self.tables[table_name]

    def parse_sql(self, sql_str):
        print("Executing query '{}'".format(sql_str))
        return self.sql_parser._parse_sql(sql_str)


class _SQLParser:
    _table_creation_special = {"primary key", "foreign key"}
    _column_type_map = {"int": int, "varchar(max)": str}

    @classmethod
    def _parse_sql(cls, sql_str: str):
        sql_str = sql_str.strip().lower()
        cls.raw_sql_str = sql_str
        sql_parts = sql_str.split(";")
        for sql_statement in sql_parts:
            sql_statement = sql_statement.strip()
            if sql_statement == "begin" or sql_statement == "commit":
                print("Transactional code not supported")
                continue
            elif sql_statement.startswith("create"):
                cls._parse_table_creation(sql_statement)
            elif sql_statement.startswith("insert"):
                cls._parse_insert_statement(sql_statement)
            elif sql_statement.startswith("select"):
                return cls._parse_select_statement(sql_statement)

    @classmethod
    def _parse_table_creation(cls, sql_statement: str):
        sql_statement = sql_statement.replace("create table", "")
        table_name = sql_statement.split("(")[0]
        # Dirty way to take section enclosed in parentheses
        table_data = sql_statement.rsplit(")", 1)[0].split("(", 1)[-1]
        columns = []
        for column_data in table_data.split(","):
            column_data = column_data.strip()
            is_special = any(
                column_data.startswith(i) for i in cls._table_creation_special
            )

            if not is_special:
                column_name, column_type = column_data.split(" ")
                column = Column(column_name, cls._column_type_map[column_type.strip()])
                columns.append(column)
            if is_special:
                print("Table keys and relations not currently supported")
            table = Table(table_name, columns)
            db._add_table(table)

    @classmethod
    def _parse_insert_statement(cls, sql_statement):
        sql_statement = sql_statement.replace("insert into", "").strip()
        table_name = sql_statement.split(" ", 1)[0]
        column_names = sql_statement.split("(", 1)[-1].split(")", 1)[0]
        column_values = sql_statement.rsplit("(", 1)[-1].rsplit(")", 1)[0].split(",")
        column_values = [i.replace("'", "").replace('"', "") for i in column_values]
        row_dict = {i: v for i, v in zip(column_names.split(","), column_values)}
        table = db.get_table(table_name)
        row = Row(row_dict)
        table.add_row(row)

    @classmethod
    def _parse_select_statement(cls, sql_statement):
        sql_statement = sql_statement.replace("select", "")
        col_names = sql_statement.split("from", 1)[0].split(",")
        table_name = sql_statement.split("from ", 1)[-1].split(" ", 1)[0]
        has_filters = "where" in sql_statement
        filters = []
        if has_filters:
            # Note, no or support
            filters = sql_statement.split("where", 1)[-1].split("and")

        filters = [i.split("=") for i in filters]
        full_results = db.get_table(table_name).rows
        for filter_col, filter_val in filters:
            filter_val = filter_val.replace("'", "").replace('"', "")
            full_results = [
                row
                for row in full_results
                if getattr(row, filter_col.strip()) == filter_val
            ]
        for row in full_results:
            yield tuple([getattr(row, i.strip()) for i in col_names])


db = DB()
