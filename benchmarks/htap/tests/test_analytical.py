import pytest

from datetime import datetime
from dateutil.parser import isoparse

from benchmarks.htap.lib.helpers import TPCH_DATE_RANGE
from benchmarks.htap.lib.analytical import AnalyticalStream


class DateValue:
    value = isoparse('2198-12-31').timestamp()


class EmptyArgs:
    dsn = 'empty'


def test_tpch_date_to_benchmark_date():
    stream = AnalyticalStream(0, EmptyArgs, None, DateValue(), None)

    assert stream.tpch_date_to_benchmark_date(isoparse('1993-01-01')) == isoparse('2193-01-01')
    assert stream.tpch_date_to_benchmark_date(isoparse('1995-01-01')) == isoparse('2195-01-01')
    assert stream.tpch_date_to_benchmark_date(isoparse('2211-01-01')) == isoparse('2411-01-01')
    assert stream.tpch_date_to_benchmark_date(isoparse('1801-01-01')) == isoparse('2001-01-01')
