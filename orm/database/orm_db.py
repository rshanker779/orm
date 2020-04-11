import abc


class ORMDB(abc.ABC):
    @abc.abstractmethod
    def parse_sql(self, sql_str):
        pass
