# dfconv

This is a CLI tool to convert between different DataFrame types. It supports both Polars and Pandas I/O tools.

# Install

It can be installed through `pip`:

`$ pip install git+https://github.com/monego/dfconv.git`

Alternatively, dfconv.py can be run manually from the command line:

`python dfconv/dfconv.py -h`

# Usage

Convert from xlsx to parquet:

`$ dfconv -if xlsx -of parquet -i data.xlsx -o data.parquet`

Force the conversion through Pandas:

`$ dfconv -if xlsx -of parquet -i data.xlsx -o data.parquet --force-pandas`

# Dependencies

A pyproject.toml file is available with the suggested environment. At least Pandas should be available. On-demand dependencies are Polars, pyarrow, openpyxl and xlsx2csv.
