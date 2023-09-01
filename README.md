# Parquet to Hyper
[![](https://img.shields.io/static/v1?label=linter&message=flake8&color=green&logo=flake8)](https://flake8.pycqa.org/en/latest/)
[![](https://img.shields.io/static/v1?label=unit-tests&message=pytest&color=green&logo=pytest)](https://docs.pytest.org/en/latest/)
![GitHub Workflow Status (with event)](https://img.shields.io/github/actions/workflow/status/guilhermenoronha/parquet-to-hyper/python-package.yml?label=tests)

Package to convert parquet files into a single hyper file. 

## Benchmarking

To be announced soon.

## How to use

### Installation

```shell
pip install parquet-to-hyper
```

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

### Publishing hyper file into Tableau server

```python
from packages.hyper_file import HyperFile

tsu = TableauServerUtils(tableau_address, token_name, token_value)
project_id = tsu.get_project_id(project_name)
tsu.publish_hyper(project_id, 'test.hyper')
```

