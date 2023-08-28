from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, \
    escape_name
from pyarrow.parquet import ParquetFile
import os
import logging as log
import packages.hyper_utils as hu


class HyperFile():

    def __init__(self, parquet_folder: str,
                 file_extension: str = None) -> None:
        """Requires parquet folder and parquet file extension if any

        Args:
            parquet_folder (str): path to folder with parquet files
            file_extension (str, optional): parquet file extension
                without the dot. Defaults to None.
        """
        self.parquet_folder = parquet_folder
        self.file_extension = file_extension

    def create_hyper_file(self, hyper_path: str) -> int:
        """Create hyper file based on files within parquet_folder

        Args:
            hyper_path (str): hyper destination with file name.
                Eg: /path/file.hyper

        Returns:
            int: number of affected rows
        """
        if os.path.exists(hyper_path):
            os.remove(hyper_path)
        files = hu.get_parquet_files(self.parquet_folder, self.file_extension)
        telemetry = Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
        create_mode = CreateMode.CREATE_AND_REPLACE
        with HyperProcess(telemetry=telemetry) as hp:
            with Connection(endpoint=hp.endpoint,
                            database=hyper_path,
                            create_mode=create_mode) as conn:
                table_definition = hu.get_table_def(ParquetFile(files[0]))
                schema = table_definition.table_name.schema_name
                conn.catalog.create_schema(schema=schema)
                conn.catalog.create_table(table_definition=table_definition)
                total_rows = 0
                for file in files:
                    try:
                        copy_command = f"COPY \"Extract\".\"Extract\" from '{file}' with (format parquet)"
                        count = conn.execute_command(copy_command)
                        total_rows += count
                    except Exception as e:
                        log.warning(f'File {os.path.basename(file)} \
                                        could not be processed. {e}')
                        log.info(f'Error message: {e}')
                log.info(f'Process completed with {total_rows} rows added.')
                return total_rows

    def delete_rows(self, hyper_path: str, date_column: str,
                    days_to_delete: int) -> int:
        """Delete rows from a hyper based on days before a date to delete.

        Args:
            hyper_path (str): hyper file path. Eg: path/hyper.file
            days_to_delete (int): the window in days to delete in the database
            date_column (str): date column to use in the incremental strategy
        Returns:
            int: number of deleted rows
        """
        telemetry = Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
        with HyperProcess(telemetry=telemetry) as hp:
            with Connection(endpoint=hp.endpoint,
                            database=hyper_path,
                            create_mode=CreateMode.NONE) as connection:
                qry = f'DELETE FROM \"Extract\".\"Extract\" WHERE {escape_name(date_column)} >= CURRENT_DATE - {days_to_delete}'
                count = connection.execute_command(qry)
                log.info(f'Process completed with {count} rows deleted.')
        return count

    def append_rows(self, hyper_path: str) -> int:
        """Append rows from parquet files into an existing hyper file

        Args:
            hyper_path (str): hyper file path. Eg: path/hyper.file

        Returns:
            int: number of appended rows
        """
        telemetry = Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
        with HyperProcess(telemetry=telemetry) as hp:
            with Connection(endpoint=hp.endpoint,
                            database=hyper_path,
                            create_mode=CreateMode.NONE) as connection:
                total_rows = 0
                files = hu.get_parquet_files(self.parquet_folder,
                                             self.file_extension)
                for parquet_path in files:
                    try:
                        copy_command = f"COPY \"Extract\".\"Extract\" from '{parquet_path}' with (format parquet)"
                        count = connection.execute_command(copy_command)
                        total_rows += count
                    except Exception as e:
                        log.warning(f'File {os.path.basename(parquet_path)}\
                                     could not be processed. {e}')
                        log.info(f'Error message: {e}')
                log.info(f'Process completed with {total_rows} rows added.')
        return total_rows
