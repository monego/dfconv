import argparse
import mimetypes
import sys


try:
    import polars as pl
    use_polars = True
except ImportError:
    try:
        import pandas as pd
    except ImportError:
        use_polars = False
        raise ImportError("Neither Pandas nor Polars was found."
                          "At least one of them must be installed.")

parser = argparse.ArgumentParser(
    prog='dfconv',
    description='Convert between DataFrame formats'
)

parser.add_argument('-i', '--input-file', required=True,
                    type=str, nargs=1, help="Input filename")
parser.add_argument('-o', '--output-file', required=True,
                    type=str, nargs=1, help="Output filename")
parser.add_argument('-fp', '--force-pandas', action='store_true',
                    help="Force Pandas even when Polars is installed")


def main(use_polars):

    args = parser.parse_args()

    mimetypes.init()

    # Add Apache Parquet and Feather MIMEs to the types.
    mimetypes.add_type('application/vnd.apache.parquet', '.parquet')
    for ext in ['.arrow', '.feather']:
        mimetypes.add_type('application/vnd.apache.arrow.feather', ext)

    input_file = args.input_file[0]
    output_file = args.output_file[0]

    input_mime = mimetypes.guess_type(input_file)[0]

    # Make sure that the extension was correctly identified, otherwise raise RuntimeError.
    if input_mime is not None:
        input_format = mimetypes.guess_extension(input_mime).replace('.', '', 1)
        output_format = output_file.split('.')[-1]
    else:
        raise RuntimeError("Input file format couldn't be identified."
                           "Maybe the MIME type is wrong?")

    if args.force_pandas:
        import pandas as pd
        use_polars = False

    read_map = {
        "csv": pl.read_csv if use_polars else pd.read_csv,
        "arrow": pl.read_ipc if use_polars else pd.read_feather,
        "feather": pl.read_ipc if use_polars else pd.read_feather,
        "parquet": pl.read_parquet if use_polars else pd.read_parquet,
        "xlsx": pl.read_excel if use_polars else pd.read_excel
    }

    write_map = {
        "csv": lambda df, file: df.write_csv(file),
        "arrow": lambda df, file: df.write_ipc(file),
        "feather": lambda df, file: df.write_ipc(file),
        "parquet": lambda df, file: df.write_parquet(file),
        "xlsx": lambda df, file: df.write_excel(file)
    }

    df = read_map[input_format](input_file)

    write_map[output_format](df, output_file)

if __name__=='__main__':
    main(use_polars)
