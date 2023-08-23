from tests.test_hyper_utils import get_pyarrow_table
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode
from packages.hyper_file import HyperFile
import pyarrow as pa
import datetime as dt
import glob
import pytest
import os

@pytest.fixture
def create_hyper_file(get_pyarrow_table):
    df = get_pyarrow_table
    now = str(dt.datetime.today().strftime("%Y-%m-%d"))
    extension = '.parquet'
    filename = now + extension
    pa.parquet.write_table(df, filename)
    hf = HyperFile('', 'parquet')
    hf.create_hyper_file('test.hyper')
    yield hf

def test_create_hyper_file(create_hyper_file):
    create_hyper_file
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, 'test.hyper', CreateMode.NONE) as connection:
            rows = connection.execute_scalar_query('SELECT COUNT(*) FROM "Extract"."Extract"')   
    os.remove('test.hyper')
    assert rows == 2

def test_delete_rows(create_hyper_file):
    hf = create_hyper_file
    count = hf.delete_rows('test.hyper', 'date32', 1)
    assert count == 1