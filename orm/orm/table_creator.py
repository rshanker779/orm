from collections import OrderedDict
from typing import Dict, Set

import graphs

from orm.data_structures.table_information import TableInformation
from orm.orm.query_builder import QueryBuilder


class TableCreator:
    def __init__(self, known_tables: Dict[str, TableInformation]):
        self.dependencies = {}
        self.sql_creation_code = []
        self.known_tables = known_tables

    def generate_table_sql(self) -> str:
        self.build_table_dependencies()
        self.build_sql_creation_statements()
        return QueryBuilder.wrap_in_transaction(self.sql_creation_code)

    def build_sql_creation_statements(self):
        for table, table_information in self.dependencies.items():
            sql_statement = QueryBuilder.build_single_sql_creation_statement(
                table_information
            )
            self.sql_creation_code.append(sql_statement)

    def build_table_dependencies(self):
        dependency_graph_map: Dict[str, Set[str]] = {}
        for class_name, table_information in self.known_tables.items():
            table_class_name = table_information.class_name
            dependency_graph_map[table_class_name] = set()

            table_dependencies = table_information.dependencies
            dependency_class_names = {
                col_dependency.normalised_dependency_class_name
                for col_dependency in table_dependencies
            }
            dependency_graph_map[table_class_name] = dependency_class_names

        graph = graphs.Graph.from_graph_dictionary(dependency_graph_map, True)
        ordered_chain = graph.dependency_chain
        self.dependencies = OrderedDict()
        for table in ordered_chain:
            class_name = table.id
            self.dependencies[class_name] = self.known_tables[class_name]
