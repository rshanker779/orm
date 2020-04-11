from enum import Enum


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
