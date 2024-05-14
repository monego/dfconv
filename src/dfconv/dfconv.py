import argparse
import logging
import mimetypes
import time

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

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

def main():

    try:
        import polars as pl
        use_polars = True
    except ImportError:
        try:
            import pandas as pd
            use_polars = False
        except ImportError:
            raise ImportError("Neither Pandas nor Polars was found."
                              "At least one of them must be installed.")
    
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
        logging.info("Using the Pandas processor.")
    else:
        logging.info("Using the Polars processor.")
        
    if use_polars:
        read_map = {
            "csv": lambda in_file: pl.read_csv(in_file),
            "arrow": lambda in_file: pl.read_ipc(in_file),
            "feather": lambda in_file: pl.read_ipc(in_file),
            "parquet": lambda in_file: pl.read_parquet(in_file),
            "xlsx": lambda in_file: pl.read_excel(in_file)
        }
        write_map = {
            "arrow": lambda df, out_file: df.write_ipc(out_file),
            "csv": lambda df, out_file: df.write_csv(out_file),
            "feather": lambda df, out_file: df.write_ipc(out_file),
            "parquet": lambda df, out_file: df.write_parquet(out_file),
            "xlsx": lambda df, out_file: df.write_excel(out_file),
        }
    else:
        read_map = {
            "csv": lambda in_file: pd.read_csv(in_file),
            "arrow": lambda in_file: pd.read_feather(in_file),
            "feather": lambda in_file: pd.read_feather(in_file),
            "parquet": lambda in_file: pd.read_parquet(in_file),
            "xlsx": lambda in_file: pd.read_excel(in_file)
        }
        write_map = {
            "arrow": lambda df, out_file: df.write_feather(out_file),
            "csv": lambda df, out_file: df.write_csv(out_file),
            "feather": lambda df, out_file: df.write_feather(out_file),
            "parquet": lambda df, out_file: df.write_parquet(out_file),
            "xlsx": lambda df, out_file: df.write_excel(out_file),
        }

    logging.info("Reading input file")

    start_read = time.time()
    df = read_map[input_format](input_file)
    end_read = time.time()

    logging.info("Writing output file")

    start_write = time.time()
    write_map[output_format](df, output_file)
    end_write = time.time()
    
    read_time = end_read - start_read
    write_time = end_write - start_write

    logging.info(f"Read execution time: {read_time} seconds")
    logging.info(f"Write execution time: {write_time} seconds")
    logging.info(f"Total execution time: {read_time + write_time} seconds")

if __name__=='__main__':
    main()
