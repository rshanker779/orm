from typing import List, Iterable, Tuple

from orm.data_structures.column import Column
from orm.data_structures.table_information import TableInformation


class PartialSelectQuery:
    def __init__(self, table_name: str, col_names: Iterable[str]):
        self.table_name = table_name
        self.col_names = col_names
        self.query = f"select {','.join(col_names)} from {table_name}"

    def filter_by(self, **kwargs):
        return self._get_filter_by_query(**kwargs)

    def all(self):
        return self._get_all_query()

    def _get_filter_by_query(self, **kwargs):
        filters = [f"{i}='{v}'" for i, v in kwargs.items()]

        self.query += f" where {'and'.join(filters)};"

        return self.query

    def _get_all_query(self):
        self.query += ";"
        return self.query


class QueryBuilder:
    @classmethod
    def build_sql_insert_statements(
        cls, instance, table_name: str, columns: List[Tuple[str, Column]]
    ):
        sql_insert = f"insert into {table_name} "
        col_names = []
        values = []
        for col_name, _ in columns:
            value = getattr(instance, col_name)
            col_names.append(col_name)
            values.append(value)
        full_col_names = ",".join(str(i) for i in col_names)
        full_values = ",".join(f"'{i}'" for i in values)
        sql_insert += f"({full_col_names}) values ({full_values});"
        return sql_insert

    @classmethod
    def wrap_in_transaction(cls, queries: List[str]) -> str:
        return "".join(f"begin;{query} commit;" for query in queries)

    @classmethod
    def build_single_sql_creation_statement(
        cls, table_information: TableInformation
    ) -> str:
        table_name = table_information.table_name
        columns = table_information.columns
        base_sql = f"create table {table_name} ( "
        primary_keys = []
        for column_name, v in columns:
            base_sql += f"{column_name} {v.column_type.to_sql_type()},"
            if v.primary_key:
                primary_keys.append(column_name)
        base_sql += f"PRIMARY KEY ({','.join(primary_keys)}) "
        foreign_key_sql = ""
        for column_dependency in table_information.dependencies:
            foreign_key_sql += (
                f",FOREIGN KEY ({column_dependency.col_name}) "
                f"REFERENCES {column_dependency.dependency_table_name} "
                f"({column_dependency.dependency_table_column})"
            )
        base_sql += foreign_key_sql + ");"
        return base_sql

    @classmethod
    def build_partial_select_query(
        cls, table_name, col_names,
    ):
        return PartialSelectQuery(table_name, col_names,)
