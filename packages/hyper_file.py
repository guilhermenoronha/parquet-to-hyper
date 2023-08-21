from tableauhyperapi import NULLABLE, HyperProcess, Telemetry, Connection, CreateMode
from packages.time_decorator import timeit
from pyarrow.parquet import ParquetFile
import os
import logging
import packages.hyper_utils as hu

class HyperFile():

    def __init__(self, parquet_folder : str, file_extension : str = None) -> None:
        self.parquet_folder = parquet_folder
        self.file_extension = file_extension

    @timeit
    def create_hyper_file(self, hyper_path : str):
        if os.path.exists(hyper_path):
            os.remove(hyper_path)
        files = hu.get_parquet_files(self.parquet_folder, self.file_extension)
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(endpoint=hp.endpoint,
                            database=hyper_path,
                            create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                table_definition = hu.get_table_def(ParquetFile(files[0]))
                connection.catalog.create_schema(schema=table_definition.table_name.schema_name)
                connection.catalog.create_table(table_definition=table_definition)
                total_rows = 0
                for parquet_path in files:
                    try:
                        copy_command = f"COPY \"Extract\".\"Extract\" from '{parquet_path}' with (format parquet)"
                        count = connection.execute_command(copy_command)
                        total_rows += count
                    except Exception:
                        logging.warning(f'File {os.path.basename(parquet_path)} could not be processed.')
                logging.info(f'Process completed with {total_rows} rows added.')
                return total_rows 
        
