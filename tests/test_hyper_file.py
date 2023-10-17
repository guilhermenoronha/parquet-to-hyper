from tests.test_hyper_utils import get_pyarrow_table
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode
from packages.hyper_file import HyperFile
import pyarrow as pa
import datetime as dt
import pytest
import os

FILENAME = "test.hyper"
QRY = 'SELECT COUNT(*) FROM "Extract"."Extract"'


@pytest.fixture
def create_hyper_file(get_pyarrow_table):
    def _method(hyper_filename):
        PARQUET = "parquet"
        df = get_pyarrow_table
        filename = str(dt.datetime.today().strftime("%Y-%m-%d")) + "." + PARQUET
        pa.parquet.write_table(df, filename)
        hf = HyperFile("", PARQUET)
        hf.create_hyper_file(hyper_filename)
        return hf

    return _method


def test_create_hyper_file(create_hyper_file):
    create_hyper_file(FILENAME)
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, FILENAME, CreateMode.NONE) as con:
            rows = con.execute_scalar_query(QRY)
    os.remove(FILENAME)
    assert rows == 2


def test_delete_rows(create_hyper_file):
    COLUMN_NAME = "date32"
    hf = create_hyper_file(FILENAME)
    count = hf.delete_rows(FILENAME, COLUMN_NAME, 1)
    os.remove(FILENAME)
    assert count == 1


def test_append_rows(create_hyper_file):
    hf = create_hyper_file(FILENAME)
    hf.append_rows(FILENAME)
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, FILENAME, CreateMode.NONE) as con:
            rows = con.execute_scalar_query(QRY)
    os.remove(FILENAME)
    assert rows == 4
