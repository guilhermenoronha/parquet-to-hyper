import packages.hyper_utils as hu
import pyarrow as pa
import pytest
import datetime as dt
import os
from tableauhyperapi import SqlType


@pytest.fixture
def get_pyarrow_table():
    array = [
        pa.array([1, 2], type=pa.int8()),
        pa.array([1, 2], type=pa.int16()),
        pa.array([1, 2], type=pa.int32()),
        pa.array([1, 2], type=pa.int64()),
        pa.array(['a', 'b'], type=pa.string()),
        pa.array([1.0, 1.5], type=pa.float32()),
        pa.array([1.0, 1.5], type=pa.float64()),
        pa.array([True, False], type=pa.bool_()),
        pa.array([dt.datetime(2023, 1, 1, 0, 0, 0), dt.datetime.now()],
                 type=pa.timestamp('us')),
        pa.array([dt.date(2023, 1, 1), dt.date.today()], type=pa.date32()),
        pa.array([dt.date(2023, 1, 1), dt.date.today()], type=pa.date64()),
        pa.array([b'a', b'b'], type=pa.binary()),
        pa.array([1234, 1234], type=pa.decimal128(7, 3))
    ]
    names = [
        'int8', 'int16', 'int32', 'int64', 'string', 'float32',
        'float64', 'bool', 'timestamp', 'date32', 'date64',
        'binary', 'decimal128'
    ]
    yield pa.table(array, names=names)


@pytest.fixture
def get_pyarrow_schema(get_pyarrow_table):
    yield get_pyarrow_table.schema


def test_convert_struct_field(get_pyarrow_schema):
    gps = get_pyarrow_schema
    assert hu._convert_struct_field(gps[0]).type == SqlType.small_int()
    assert hu._convert_struct_field(gps[1]).type == SqlType.small_int()
    assert hu._convert_struct_field(gps[2]).type == SqlType.int()
    assert hu._convert_struct_field(gps[3]).type == SqlType.big_int()
    assert hu._convert_struct_field(gps[4]).type == SqlType.text()
    assert hu._convert_struct_field(gps[5]).type == SqlType.double()
    assert hu._convert_struct_field(gps[6]).type == SqlType.double()
    assert hu._convert_struct_field(gps[7]).type == SqlType.bool()
    assert hu._convert_struct_field(gps[8]).type == SqlType.timestamp()
    assert hu._convert_struct_field(gps[9]).type == SqlType.date()
    assert hu._convert_struct_field(gps[10]).type == SqlType.date()
    assert hu._convert_struct_field(gps[11]).type == SqlType.bytes()
    assert hu._convert_struct_field(gps[12]).type == SqlType.numeric(7, 3)


def test_get_table_def(get_pyarrow_table):
    df = get_pyarrow_table
    now = str(dt.datetime.today().strftime("%Y-%m-%d"))
    pa.parquet.write_table(df, now)
    with pa.parquet.ParquetFile(now) as file:
        table_def = hu.get_table_def(file)
    os.remove(now)
    assert table_def.columns[0].type == SqlType.small_int()
    assert table_def.columns[1].type == SqlType.small_int()
    assert table_def.columns[2].type == SqlType.int()
    assert table_def.columns[3].type == SqlType.big_int()
    assert table_def.columns[4].type == SqlType.text()
    assert table_def.columns[5].type == SqlType.double()
    assert table_def.columns[6].type == SqlType.double()
    assert table_def.columns[7].type == SqlType.bool()
    assert table_def.columns[8].type == SqlType.timestamp()
    assert table_def.columns[9].type == SqlType.date()
    assert table_def.columns[10].type == SqlType.date()
    assert table_def.columns[11].type == SqlType.bytes()
    assert table_def.columns[12].type == SqlType.numeric(7, 3)


def test_get_parquet_files(get_pyarrow_table):
    df = get_pyarrow_table
    now = str(dt.datetime.today().strftime("%Y-%m-%d"))
    extension = '.parquet'
    filename = now + extension
    pa.parquet.write_table(df, filename)
    files = hu.get_parquet_files('', extension.replace('.', ''))
    os.remove(filename)
    assert len(files) == 1
