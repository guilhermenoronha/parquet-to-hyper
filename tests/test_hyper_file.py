from tests.test_hyper_utils import get_pyarrow_table
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode
from packages.hyper_file import HyperFile
import pyarrow as pa
import datetime as dt
import pytest
import os


@pytest.fixture
def create_hyper_file(get_pyarrow_table):
    def _method(hyper_filename):
        df = get_pyarrow_table
        filename = str(dt.datetime.today().strftime("%Y-%m-%d")) + '.parquet'
        pa.parquet.write_table(df, filename)
        hf = HyperFile('', 'parquet')
        hf.create_hyper_file(hyper_filename)
        return hf
    return _method


def test_create_hyper_file(create_hyper_file):
    filename = 'test.hyper'
    create_hyper_file(filename)
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, filename, CreateMode.NONE) as con:
            rows = con.execute_scalar_query(
                'SELECT COUNT(*) FROM "Extract"."Extract"')
    os.remove(filename)
    assert rows == 2


def test_delete_rows(create_hyper_file):
    filename = 'test.hyper'
    hf = create_hyper_file(filename)
    count = hf.delete_rows(filename, 'date32', 1)
    os.remove(filename)
    assert count == 1


def test_append_rows(create_hyper_file):
    filename = 'test.hyper'
    hf = create_hyper_file(filename)
    hf.append_rows(filename)
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, filename, CreateMode.NONE) as con:
            rows = con.execute_scalar_query(
                'SELECT COUNT(*) FROM "Extract"."Extract"')
    os.remove(filename)
    assert rows == 4
