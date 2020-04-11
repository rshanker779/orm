from orm.orm.table_information_builder import TableInformationBuilder

from tests.conftest import Post


def test_table_information_builder():
    table_information = TableInformationBuilder.build_table_information(Post)
    assert table_information.table_name == "posts"
    assert len(table_information.columns) == 3
    column_names = {name for name, _ in table_information.columns}
    assert column_names == {"id", "content", "user_id"}
    assert len(table_information.dependencies) == 1
    dependency = table_information.dependencies[0]
    assert dependency.col_name == "user_id"
    assert dependency.dependency_class_name == "user"
    assert dependency.dependency_table_column == "id"
