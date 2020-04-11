import rshanker779_common as utils
import sqlalchemy as sa

from orm.database.orm_db import ORMDB

logger = utils.get_logger(__name__)


class PostgresORMDB(ORMDB):
    _conn_string = "postgresql://test:test@localhost/orm_test"

    def __init__(self):
        logger.info("Dropping tables")
        res = self.engine.execute(
            "select 'drop table if exists \"' || tablename || '\" cascade;' "
            "from pg_tables where schemaname = 'public';"
        )
        for table_drop, *_ in res:
            self.engine.execute(table_drop)

    def parse_sql(self, sql_str):
        return self.engine.execute(sql_str)

    @property
    def engine(self):
        eng = sa.create_engine(self._conn_string)
        return eng
