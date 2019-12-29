from orm_db import ORMDB
import sqlalchemy as sa

class PostgresORMDB(ORMDB):
    _conn_string = 'postgresql:'
    def parse_sql(self, sql_str):