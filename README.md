# Parquet to Hyper

Package to convert parquet files into a single hyper file. 

## Benchmarking

To be announced soon.

## How to use

### Initializing object

```python
from packages.hyper_file import HyperFile

parquet_folder = '/path/to/your/folder'             # The folder where the parquet files are
parquet_extension = 'parquet'                       # Optional. Don't use it if the parquet files has no extension
hf = HyperFile(parquet_folder, parquet_extension)
```

### Create a single file

```python
hyper_filename = 'path/to/your/db.hyper'            # Path to save hyper file with filename
rows = hf.create_hyper_file(hyper_file_name)
print(f'Hyper created with {rows} rows.')
```

### Deleting rows from an existing hyper file

This function deletes rows based on a control column (date column) and the days to delete from current day.

```python
hyper_filename = 'path/to/your/db.hyper'            # Path to load hyper file with filename
control_column = 'date_column'
days = 7
hf.delete_rows(hyper_filename)
print(f'{rows} rows were deleted.')
```

### Appending rows from parquet into an existing hyper file

```python
hyper_filename = 'path/to/your/db.hyper'            # Path to load hyper file with filename
rows  = hf.append_rows(hyper_filename)
print(f'{rows} were appended.')
```

