import pandas as pd
import argparse
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_csv(input_file_path, output_file_path, expected_columns=7):
    try:
        df = pd.read_csv(input_file_path, dtype=str, keep_default_na=False, on_bad_lines='skip')

        # Drop unnamed or empty columns
        df = df.loc[:, ~df.columns.str.match(r'^Unnamed|^$', na=False)]

        # Drop rows where the first column equals its column name (i.e. repeated headers)
        df = df[df[df.columns[0]] != df.columns[0]]

        # Truncate extra columns if necessary
        if df.shape[1] > expected_columns:
            df = df.iloc[:, :expected_columns]

        df.to_csv(output_file_path, index=False)
        logger.info(f"✅ Cleaned CSV saved to: {output_file_path}")
    except FileNotFoundError:
        logger.error(f"❌ File not found: {input_file_path}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"❌ An error occurred while cleaning CSV: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean a CSV file by removing invalid rows/columns.")
    parser.add_argument("input_file", help="Path to the input CSV file")
    parser.add_argument("output_file", help="Path to the output cleaned CSV file")
    args = parser.parse_args()

    clean_csv(args.input_file, args.output_file)
