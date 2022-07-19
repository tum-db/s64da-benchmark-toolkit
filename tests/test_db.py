from unittest.mock import call

import psycopg
import pytest
import json

from s64da_benchmark_toolkit import db

DSN = 'postgresql://postgres@nowhere:1234/foodb'
DSN_PG = 'postgresql://postgres@nowhere:1234/postgres'


def get_mocked_conn(mocker):
    psycopg_connect = mocker.patch('psycopg.connect')
    return psycopg_connect.return_value


def get_mocked_cursor(mocker):
    return get_mocked_conn(mocker).cursor.return_value


@pytest.fixture
def no_plan(monkeypatch):
    def monkeyplan(conn, sql):
        return None

    some_db = db.DB
    monkeypatch.setattr(some_db, "get_explain_output", monkeyplan)


def test_db_init():
    some_db = db.DB(DSN)
    assert some_db.dsn == DSN
    assert some_db.dsn_pg_db == DSN_PG


def test_db_apply_config(mocker):
    mock_cursor = get_mocked_cursor(mocker)

    db.DB(DSN).apply_config({
        'foo': 'bar',
        'bla': 1
    })

    mock_cursor.execute.assert_has_calls([
        call('ALTER SYSTEM SET foo = $$bar$$'),
        call('ALTER SYSTEM SET bla = $$1$$'),
        call('SELECT pg_reload_conf()')
    ])


def test_db_reset_config(mocker):
    mock_cursor = get_mocked_cursor(mocker)
    db.DB(DSN).reset_config()

    mock_cursor.execute.assert_has_calls([
        call('ALTER SYSTEM RESET ALL'),
        call('SELECT pg_reload_conf()')
    ])


def test_db_run_query_ok(no_plan, mocker):
    mock_cursor = get_mocked_cursor(mocker)
    result, query_output, _ = db.DB(DSN).run_query('SELECT 1', 0)

    assert result.status == db.Status.OK
    assert (result.stop - result.start) > 0
    assert ([], mock_cursor.fetchall()) == query_output
    mock_cursor.execute.assert_called_once_with('SELECT 1')


def test_db_run_query_timeout(no_plan, mocker):
    mock_cursor = get_mocked_cursor(mocker)
    mock_cursor.execute.side_effect = psycopg.extensions.QueryCanceledError('Timeout')

    result, query_output, _ = db.DB(DSN).run_query('SELECT 1', 0)
    assert result.status == db.Status.TIMEOUT
    assert (result.stop - result.start) > 0
    assert query_output is None
    mock_cursor.execute.assert_called_once_with('SELECT 1')


def test_db_run_query_error(no_plan, mocker):
    mock_cursor = get_mocked_cursor(mocker)
    mock_cursor.execute.side_effect = psycopg.InternalError('Error')

    result, query_output, _ = db.DB(DSN).run_query('SELECT 1', 0)
    assert result.status == db.Status.ERROR
    assert (result.stop - result.start) > 0
    assert query_output is None
    mock_cursor.execute.assert_called_once_with('SELECT 1')


def test_get_explain_output_json_error(mocker):
    mocker_conn = get_mocked_conn(mocker)
    mocker_json = mocker.patch('json.dumps')
    mocker_json.side_effect = json.decoder.JSONDecodeError('Test invalid explain plan', '', 255)
    plan = db.DB(DSN).get_explain_output(mocker_conn, 'EXPLAIN JSON SELECT 1')
    assert plan == f'Explain Output failed with a JSON Decode Error: Test invalid explain plan: line 1 column 256 (char 255)'
