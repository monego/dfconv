import argparse
import sys

try:
    import polars as pl
    use_polars = True
except ImportError:
    import pandas as pd
except ImportError:
    print("Neither Pandas nor Polars was found."
          "At least one of them must be installed.",
          file=sys.stderr)
    
parser = argparse.ArgumentParser(
    prog='dfconv',
    description='Convert between DataFrame formats'
)

parser.add_argument('-if', '--input-format', required=True,
                    choices=('csv', 'ipc', 'parquet', 'xlsx'),
                    type=str, nargs=1, help="Input format")
parser.add_argument('-of', '--output-format', required=True,
                    choices=('csv', 'ipc', 'parquet', 'xlsx'),
                    type=str, nargs=1, help="Output format")
parser.add_argument('-i', '--input-file', required=True,
                    type=str, nargs=1, help="Input filename")
parser.add_argument('-o', '--output-file', required=True,
                    type=str, nargs=1, help="Output filename")
parser.add_argument('-fp', '--force-pandas', action='store_true',
                    help="Force Pandas even is Polars is installed")


args = parser.parse_args()

input_format = args.input_format[0]
output_format = args.output_format[0]
input_file = args.input_file[0]
output_file = args.output_file[0]

if args.force_pandas:
    import pandas as pd
    use_polars = False
    
if use_polars:
    if input_format == "csv":
        df = pl.read_csv(input_file)
    elif input_format == "ipc":
        df = pl.read_ipc(input_file)
    elif input_format == "parquet":
        df = pl.read_parquet(input_file)
    elif input_format == "xlsx":
        df = pl.read_excel(input_file)

    if output_format == "csv":
        df.write_csv(output_file)
    elif output_format == "ipc":
        df.write_ipc(output_file)
    elif output_format == "parquet":
        df.write_parquet(output_file)
    elif output_format == "xlsx":
        df.write_excel(output_file)

else:
    if input_format == "csv":
        df = pd.read_csv(input_file)
    elif input_format == "ipc":
        df = pd.read_feather(input_file)
    elif input_format == "parquet":
        df = pd.read_parquet(input_file)
    elif input_format == "xlsx":
        df = pd.read_excel(input_file)

    if output_format == "csv":
        df.to_csv(output_file)
    elif output_format == "ipc":
        df.to_feather(output_file)
    elif output_format == "parquet":
        df.to_parquet(output_file)
    elif output_format == "xlsx":
        df.to_excel(output_file)
