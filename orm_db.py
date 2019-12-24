from abc import ABC, abstractmethod


class ORMDB(ABC):
    @abstractmethod
    def parse_sql(self, sql_str):
        pass
