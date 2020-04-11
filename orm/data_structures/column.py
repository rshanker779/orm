from orm.data_structures.data_types import ColumnTypes


class Column:
    def __init__(
        self, column_type: ColumnTypes, foreign_key: str = None, primary_key=False
    ):
        self.column_type = column_type
        self.foreign_key = foreign_key
        self.primary_key = primary_key
