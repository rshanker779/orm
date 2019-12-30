from enum import Enum
from typing import Iterable, Dict, Any, List

from orm_db import ORMDB
from utilities import StringMixin


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
                    row.col_names, self.col_names
                )
            )
        self.rows.append(row)


class SQLType(Enum):
    CREATE = 0
    INSERT = 1
    SELECT = 2


class SQLReturn:
    def __init__(
        self,
        type: SQLType,
        table: Table = None,
        table_name: str = None,
        row: Row = None,
        filters=None,
        col_names: Iterable[str] = None,
    ):
        self.type = type
        self.table = table
        self.table_name = table_name
        self.row = row
        self.filters = filters
        self.col_names = col_names


class _SQLParser:
    _table_creation_special = {"primary key", "foreign key"}
    _column_type_map = {"int": int, "varchar": str}

    def _parse_sql(cls, sql_str: str) -> SQLReturn:
        sql_str = sql_str.strip().lower()
        cls.raw_sql_str = sql_str
        sql_parts = sql_str.split(";")
        for sql_statement in sql_parts:
            sql_statement = sql_statement.strip()
            if sql_statement == "begin" or sql_statement == "commit":
                print("Transactional code not supported")
                continue
            elif sql_statement.startswith("create"):
                return cls._parse_table_creation(sql_statement)
            elif sql_statement.startswith("insert"):
                return cls._parse_insert_statement(sql_statement)
            elif sql_statement.startswith("select"):
                return cls._parse_select_statement(sql_statement)

    def _parse_table_creation(self, sql_statement: str):
        sql_statement = sql_statement.replace("create table", "")
        table_name = sql_statement.split("(")[0]
        # Dirty way to take section enclosed in parentheses
        table_data = sql_statement.rsplit(")", 1)[0].split("(", 1)[-1]
        columns = []
        for column_data in table_data.split(","):
            column_data = column_data.strip()
            is_special = any(
                column_data.startswith(i) for i in self._table_creation_special
            )
            if not is_special:
                column_name, column_type = column_data.split(" ")
                column = Column(column_name, self._column_type_map[column_type.strip()])
                columns.append(column)
            if is_special:
                print("Table keys and relations not currently supported")
        table = Table(table_name, columns)
        return SQLReturn(SQLType.CREATE, table)

    def _parse_insert_statement(cls, sql_statement):
        sql_statement = sql_statement.replace("insert into", "").strip()
        table_name = sql_statement.split(" ", 1)[0]
        column_names = sql_statement.split("(", 1)[-1].split(")", 1)[0]
        column_values = sql_statement.rsplit("(", 1)[-1].rsplit(")", 1)[0].split(",")
        column_values = [i.replace("'", "").replace('"', "") for i in column_values]
        row_dict = {i: v for i, v in zip(column_names.split(","), column_values)}
        row = Row(row_dict)
        return SQLReturn(SQLType.INSERT, table_name=table_name, row=row)

    def _parse_select_statement(cls, sql_statement):
        sql_statement = sql_statement.replace("select", "")
        col_names = sql_statement.split("from", 1)[0].split(",")
        table_name = sql_statement.split("from ", 1)[-1].split(" ", 1)[0]
        has_filters = "where" in sql_statement
        filters = []
        if has_filters:
            # Note, no 'or' support
            filters = sql_statement.split("where", 1)[-1].split("and")
        filters = [i.split("=") for i in filters]
        return SQLReturn(
            SQLType.SELECT, table_name=table_name, filters=filters, col_names=col_names
        )


class DB(
    StringMixin, ORMDB,
):
    def __init__(self):
        super().__init__()
        self.tables = {}  # type: Dict[str, Table]
        self.sql_parser = _SQLParser()

    def _add_table(self, table: Table):
        print("Adding table {}".format(table))
        self.tables[table.name] = table

    def get_table(self, table_name: str) -> Table:
        return self.tables[table_name]

    def parse_sql(self, sql_str):
        print("Executing query '{}'".format(sql_str))
        sql_return = self.sql_parser._parse_sql(sql_str)
        if sql_return.type == SQLType.CREATE:
            self._add_table(sql_return.table)
        elif sql_return.type == SQLType.INSERT:
            self.get_table(sql_return.table_name).add_row(sql_return.row)
        elif sql_return.type == SQLType.SELECT:
            return list(self._process_select_results(sql_return))

    def _process_select_results(self, sql_return):
        full_results = self.get_table(sql_return.table_name).rows
        for filter_col, filter_val in sql_return.filters:
            filter_val = filter_val.replace("'", "").replace('"', "")
            full_results = [
                row
                for row in full_results
                if getattr(row, filter_col.strip()) == filter_val
            ]
        for row in full_results:
            yield tuple([getattr(row, i.strip()) for i in sql_return.col_names])
