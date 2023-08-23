from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, escape_name
from packages.time_decorator import timeit
from pyarrow.parquet import ParquetFile
import os
import logging
import packages.hyper_utils as hu

class HyperFile():

    def __init__(self, parquet_folder : str, file_extension : str = None) -> None:
        """Class constructor. Requires parquet folder and parquet file extension if any

        Args:
            parquet_folder (str): path to folder with parquet files
            file_extension (str, optional): parquet file extension without the dot. Defaults to None.
        """
        self.parquet_folder = parquet_folder
        self.file_extension = file_extension

    @timeit
    def create_hyper_file(self, hyper_path : str) -> int:
        """Create hyper file based on files within parquet_folder

        Args:
            hyper_path (str): hyper destination with file name. Eg: /path/file.hyper

        Returns:
            int: number of affected rows
        """
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
                    except Exception as e:
                        logging.warning(f'File {os.path.basename(parquet_path)} could not be processed. {e}')
                        logging.info(f'Error message: {e}')
                logging.info(f'Process completed with {total_rows} rows added.')
                return total_rows

    @timeit
    def delete_rows(self, hyper_path: str, date_column : str, days_to_delete : int) -> int:
        """Function that copies data from a Parquet file to a .hyper file.

        Args:
            hyper_path (str): hyper file path. Eg: path/hyper.file 
            days_to_delete (int): the window in days to be deleted from the database
            date_column (str): name of date column to be used in the incremental strategy
        """
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(endpoint=hp.endpoint,
                            database=hyper_path,
                            create_mode=CreateMode.NONE) as connection:
                delete_command = f'DELETE FROM \"Extract\".\"Extract\" WHERE {escape_name(date_column)} >= CURRENT_DATE - {days_to_delete}'
                count = connection.execute_command(delete_command)
                logging.info(f'Process completed with {count} rows deleted.')        
        return count
        
