from orm.data_structures.table_information import TableInformation, ColumnDependency


class TableInformationBuilder:
    @classmethod
    def build_table_information(cls, table,) -> TableInformation:
        columns = table.get_columns()
        normalised_table_name = table.table_name
        full_dependencies = [(i, v) for i, v in columns if v.foreign_key is not None]
        if not full_dependencies:
            return TableInformation(table, normalised_table_name, columns)
        simple_dependencies = []
        for col_name, dependency in full_dependencies:
            str_dependency = dependency.foreign_key
            dependency_class_name, dependency_col_name = str_dependency.split(".")
            column_dependency = ColumnDependency(
                col_name, dependency_class_name, dependency_col_name
            )
            simple_dependencies.append(column_dependency)
        return TableInformation(
            table, normalised_table_name, columns, *simple_dependencies
        )
