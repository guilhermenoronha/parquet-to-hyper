    
from pyarrow import ChunkedArray
from tableauhyperapi import SqlType, TableDefinition, NULLABLE, NOT_NULLABLE, TableName
from pyarrow.parquet import ParquetFile
import glob
import pyarrow as pa


def _convert_struct_field(column : ChunkedArray) -> TableDefinition.Column:
    """Converts a Pyarrow Column type to a Tableau Hyper SqlType

    Args:
        column (ChunkedArray): Pyarrow column

    Raises:
        ValueError: raises error if the column type is not found.

    Returns:
        Column: Column with Hyper SqlType
    """
    if column.type == pa.string():
        sql_type = SqlType.text()
    elif column.type in [pa.date32(), pa.date64()]:
        sql_type = SqlType.date()
    elif column.type in [pa.float32(), pa.float64()]:
        sql_type = SqlType.double()
    elif column.type in [pa.int8(), pa.int16(), pa.int32()]:
        sql_type = SqlType.int()
    elif column.type == pa.int64():
        sql_type = SqlType.big_int()
    elif column.type == pa.bool_():
        sql_type = SqlType.bool()
    elif column.type in [pa.timestamp('s'), pa.timestamp('ms'), pa.timestamp('us'), pa.timestamp('ns')]:
        sql_type = SqlType.timestamp()
    elif column.type == pa.binary():
        sql_type = SqlType.bytes()
    else:
        raise ValueError(f'Invalid StructField datatype for column `{column.name}` : {column.type}')
    nullable = NULLABLE if column.nullable else NOT_NULLABLE
    return TableDefinition.Column(name=column.name, type=sql_type, nullability=nullable)

def get_table_def(df : ParquetFile) -> TableDefinition:
    """Returns a Tableau TableDefintion given a Parquet file

    Args:
        df (ParquetFile): Pyarrow dataframe

    Returns:
        TableDefinition: Hyper Table Definition with schema
    """
    schema = df.schema_arrow
    cols = list(map(_convert_struct_field, schema))
    return TableDefinition(
        table_name=TableName("Extract", "Extract"),
        columns=cols
    )

def get_parquet_files(parquet_folder : str, parquet_extension : str = None) -> list[str]:
    """Get list of parquet files in a folder

    Args:
        parquet_folder (str): path where the parquet files are
        parquet_extension (str, optional): If the parquet files has some extension in its files. Eg.: parquet. Defaults to None.

    Raises:
        ValueError: raises error if the folder has no parquet files

    Returns:
        list[str]: list of filenames
    """
    extension =  f"*.{parquet_extension}" if parquet_extension is not None else '*'
    files = glob.glob(parquet_folder + extension)
    if len(files) == 0:
        raise ValueError(f'Error! The parquet_folder: {parquet_folder} returned no files!')
    return files