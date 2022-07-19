import logging
import time

import psycopg2
from psycopg2.extras import DictCursor

LOG = logging.getLogger()


class DBConn:
    def __init__(self, dsn, statement_timeout=0, num_retries=120, retry_wait=1, use_dict_cursor=False):
        self.dsn = dsn
        self.conn = None
        self.cursor = None
        self.server_side_cursor = None
        self.statement_timeout = statement_timeout
        self.num_retries = num_retries
        self.retry_wait = retry_wait
        self.use_dict_cursor = use_dict_cursor

    def __enter__(self):
        options = f'-c statement_timeout={self.statement_timeout}'
        trial = 0
        while trial < self.num_retries:
            try:
                self.conn = psycopg2.connect(self.dsn, options=options)
                self.conn.autocommit = True
                self.cursor = self.conn.cursor(cursor_factory=DictCursor if self.use_dict_cursor else None)
                self.server_side_cursor = self.conn.cursor('server-side-cursor')
                break

            except psycopg2.Error as exc:
                LOG.info(f'Cannot connect to DB. Retrying. Error: {exc}')
                trial += 1
                time.sleep(self.retry_wait)

        assert self.conn, 'There is no connection.'
        assert self.cursor, 'There is no cursor.'
        assert self.server_side_cursor, 'There is no server-side cursor.'

        return self

    def __exit__(self, *args):
        self.cursor.close()
        self.conn.close()
