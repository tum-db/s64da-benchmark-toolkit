import time

import psycopg
import pytest

from s64da_benchmark_toolkit import dbconn

DSN = 'postgresql://postgres@nowhere/foodb'


@pytest.fixture
def nosleep(monkeypatch):
    def sleep(seconds):
        pass

    monkeypatch.setattr(time, 'sleep', sleep)


def test_dbconn_connect_success(nosleep, mocker):
    psycopg_connect = mocker.patch('psycopg.connect')
    mock_conn = psycopg_connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    with dbconn.DBConn(DSN) as conn:
        assert conn.conn
        assert conn.cursor

    psycopg_connect.assert_called_once()
    mock_conn.close.assert_called_once()
    mock_cursor.close.assert_called_once()


def test_dbconn_connect_fail(nosleep, mocker):
    psycopg_connect = mocker.patch('psycopg.connect',
                                    side_effect=psycopg.Error('Just an error...'))

    num_retries = 10
    with pytest.raises(AssertionError):
        with dbconn.DBConn(DSN, num_retries=num_retries):
            pass

    assert psycopg_connect.call_count == num_retries
