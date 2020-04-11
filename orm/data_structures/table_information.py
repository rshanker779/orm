from typing import Dict, List

from orm.data_structures.column import Column


class ColumnDependency:
    def __init__(
        self, col_name: str, dependency_table_name: str, dependency_table_column: str
    ):
        self.col_name = col_name
        self.dependency_class_name = dependency_table_name
        self.normalised_dependency_class_name = self.dependency_class_name.capitalize()
        self.dependency_table_name = None
        self.dependency_table_column = dependency_table_column

    def update_dependency_table_name(self, table_name_map: Dict[str, str]):
        self.dependency_table_name = table_name_map[
            self.normalised_dependency_class_name
        ]


class TableInformation:
    def __init__(
        self, table, table_name: str, columns: List[Column], *args: ColumnDependency
    ):
        self.table = table
        self.table_name = table_name
        self.class_name = self.table.__name__
        self.normalised_class_name = self.class_name.capitalize()
        self.columns = columns
        self.dependencies = args

    def update_column_dependencies(self, table_name_map):
        for dependency in self.dependencies:
            dependency.update_dependency_table_name(table_name_map)
