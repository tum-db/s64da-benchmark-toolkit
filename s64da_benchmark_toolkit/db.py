import json
import logging
import time

from collections import namedtuple
from enum import Enum
from urllib.parse import urlparse

from .dbconn import DBConn

import psycopg

LOG = logging.getLogger()
Timing = namedtuple('Timing', ['start', 'stop', 'status'])


class Status(Enum):
    OK = 0
    TIMEOUT = 1
    ERROR = 2


class DB:
    def __init__(self, args, dsn):
        self.args = args
        self.dsn = dsn
        dsn_url = urlparse(dsn)
        self.dsn_pg_db = f'{dsn_url.scheme}://{dsn_url.netloc}/postgres'

    def apply_config(self, config):
        with DBConn(self.dsn_pg_db) as conn:
            for key, value in config.items():
                conn.cursor.execute(f'ALTER SYSTEM SET {key} = $${value}$$')

            conn.cursor.execute('SELECT pg_reload_conf()')

    def reset_config(self):
        with DBConn(self.dsn_pg_db) as conn:
            conn.cursor.execute('ALTER SYSTEM RESET ALL')
            conn.cursor.execute('SELECT pg_reload_conf()')

    def run_query(self, sql, timeout, auto_explain=False, use_server_side_cursors=False):
        status = Status.ERROR
        with DBConn(self.dsn, statement_timeout=timeout) as conn:
            try:
                start = time.time()

                if auto_explain:
                    DB.auto_explain_on(conn)

                cursor = conn.cursor
                if use_server_side_cursors:
                    # See https://github.com/psycopg/psycopg2/issues/941 for why
                    # starting a new connection is so weird.
                    conn.conn.rollback()
                    conn.conn.autocommit = False
                    cursor = conn.server_side_cursor

                cursor.execute(sql)
                rows = cursor.fetchall()

                if use_server_side_cursors:
                    conn.conn.rollback()
                    conn.conn.autocommit = True

                if rows is not None:
                    query_result_columns = [colname[0] for colname in cursor.description]
                    query_result = query_result_columns, rows
                else:
                    query_result = None
                status = Status.OK

            except psycopg.errors.QueryCanceled:
                status = Status.TIMEOUT
                query_result = None

            except (psycopg.InternalError, psycopg.Error, UnicodeDecodeError):
                LOG.exception('Ignoring psycopg Error')
                query_result = None

            finally:
                stop = time.time()
                plan = DB.get_explain_output(conn.conn, sql, self.args.umbra)

            return Timing(start=start, stop=stop, status=status), query_result, plan

    @staticmethod
    def auto_explain_on(conn):
        auto_explain_config = {
            'auto_explain.log_min_duration': 0,
            'auto_explain.log_analyze': 'on',
            'auto_explain.log_verbose': 'on',
            'auto_explain.log_buffers': 'off',
            'auto_explain.log_format': 'json',
            'client_min_messages': 'LOG'
        }

        conn.cursor.execute("LOAD 'auto_explain'")

        for key, value in auto_explain_config.items():
            conn.cursor.execute(f'SET {key} = $${value}$$')

    @staticmethod
    def get_explain_output(connection, sql, umbra):
        try:
            with connection.cursor() as explain_plan_cursor:
                explain_plan_cursor.execute(sql.replace('-- EXPLAIN (FORMAT JSON)', 'EXPLAIN (FORMAT JSON)' if not umbra else 'EXPLAIN VERBOSE'))
                return json.dumps(explain_plan_cursor.fetchone()[0], indent=4)

        except psycopg.Error as e:
            return f'{{"Explain Output failed": "{str(e)}"}}'

        except json.JSONDecodeError as e:
            LOG.warning('Explain Output failed with a JSON Decode Error')
            return f'Explain Output failed with a JSON Decode Error: {str(e)}'

        except TypeError as e:
            return f'{{"Explain Output failed": "{str(e)}"}}'
