from tests.test_hyper_utils import get_pyarrow_table
from packages.hyper_file import HyperFile
import pyarrow as pa
import datetime as dt
import glob
import pytest
import os

@pytest.mark.usefixtures('get_pyarrow_table')
def test_create_hyper_file(get_pyarrow_table):
    df = get_pyarrow_table
    now = str(dt.datetime.today().strftime("%Y-%m-%d"))
    extension = '.parquet'
    filename = now + extension
    pa.parquet.write_table(df, filename)
    hf = HyperFile('', 'parquet')
    hf.create_hyper_file('test.hyper')
    files = glob.glob('*.hyper')
    os.remove('test.hyper')
    assert len(files) == 1
