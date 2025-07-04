# scripts/delete_eispt_row.py

import pandas as pd
import numpy as np
import sys
import os
import traceback

def remove_empty_rows_advanced(input_file, output_file, min_filled_columns=2):
    try:
        # Load the file
        if input_file.endswith('.csv'):
            df = pd.read_csv(input_file, dtype=str, keep_default_na=False)
        elif input_file.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(input_file, dtype=str)
        else:
            raise ValueError("Unsupported file format. Use CSV or Excel.")

        print(f"✅ Loaded file: {input_file} with shape {df.shape}")

        # Fallback if columns are not as expected
        if df.empty or df.shape[1] == 0:
            raise ValueError("Input file has no valid data or columns.")

        # Columns that might exist
        important_columns = ['Date', 'Narration', 'Chq/Ref No', 'Withdrawal(Dr)', 'Deposit(Cr)', 'Balance']
        existing_columns = [col for col in important_columns if col in df.columns]

        if not existing_columns:
            print("⚠️ No standard important columns found. Checking all columns.")
            existing_columns = df.columns.tolist()

        # Count non-empty and non-whitespace cells
        non_empty_count = df[existing_columns].notna().sum(axis=1)
        non_whitespace_count = df[existing_columns].apply(
            lambda x: x.astype(str).str.strip().replace('', np.nan)
        ).notna().sum(axis=1)

        # Keep rows that have at least min_filled_columns
        rows_to_keep = (non_empty_count >= min_filled_columns) & (non_whitespace_count >= min_filled_columns)
        cleaned_df = df[rows_to_keep].copy().reset_index(drop=True)

        print(f"✅ Rows removed: {len(df) - len(cleaned_df)}")
        print(f"✅ Cleaned shape: {cleaned_df.shape}")

        # Save output
        if output_file.endswith('.csv'):
            cleaned_df.to_csv(output_file, index=False)
        elif output_file.endswith(('.xlsx', '.xls')):
            cleaned_df.to_excel(output_file, index=False)
        print(f"✅ Output saved to: {output_file}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("❌ Usage: python delete_eispt_row.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    if not os.path.exists(input_file):
        print(f"❌ Input file does not exist: {input_file}")
        sys.exit(1)

    print(f"🔧 Running delete_eispt_row.py on: {input_file}")
    remove_empty_rows_advanced(input_file, output_file)
