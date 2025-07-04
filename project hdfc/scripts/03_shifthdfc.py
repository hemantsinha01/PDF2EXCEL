import pandas as pd
import argparse
import logging
import sys
import re

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_csv_data_shift(input_file, output_file):
    """
    Fix shifted CSV rows where 'Chq/Ref No' is empty and data is pushed right.
    """
    try:
        df = pd.read_csv(input_file, dtype=str, keep_default_na=False)

        chq_ref_col = None
        for col in df.columns:
            if 'chq' in col.lower() or 'ref' in col.lower():
                chq_ref_col = col
                break

        if chq_ref_col is None:
            logger.error("❌ 'Chq/Ref No' column not found in the file.")
            sys.exit(1)

        chq_ref_pos = df.columns.get_loc(chq_ref_col)
        df_corrected = df.copy()

        # Detect empty Chq/Ref No
        empty_chq_mask = (df[chq_ref_col].str.strip() == '') | df[chq_ref_col].isna()
        problematic_indices = df[empty_chq_mask].index.tolist()

        logger.info(f"🔍 Found {len(problematic_indices)} rows with empty '{chq_ref_col}' to fix.")

        for idx in problematic_indices:
            actual_data = df.iloc[idx, chq_ref_pos + 1] if chq_ref_pos + 1 < len(df.columns) else ""
            df_corrected.iat[idx, chq_ref_pos] = actual_data

            # Shift left
            for col_pos in range(chq_ref_pos + 1, len(df.columns) - 1):
                df_corrected.iat[idx, col_pos] = df.iat[idx, col_pos + 1]

            # Clear last column
            df_corrected.iat[idx, -1] = ""

        # Optional numeric cleaning
        numeric_columns = ["Withdrawal Amt.", "Deposit Amt.", "Closing Balance"]
        for col in numeric_columns:
            if col in df_corrected.columns:
                df_corrected[col] = (
                    df_corrected[col]
                    .str.replace(",", "", regex=False)
                    .replace("", "0")
                )
                df_corrected[col] = pd.to_numeric(df_corrected[col], errors="coerce")

        df_corrected.to_csv(output_file, index=False)
        logger.info(f"✅ Corrected file saved to: {output_file}")

    except FileNotFoundError:
        logger.error(f"❌ Input file not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"❌ An unexpected error occurred while processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix shifted CSV rows when 'Chq/Ref No' is missing.")
    parser.add_argument("input_file", help="Input CSV file path")
    parser.add_argument("output_file", help="Output CSV file path")
    args = parser.parse_args()

    fix_csv_data_shift(args.input_file, args.output_file)
