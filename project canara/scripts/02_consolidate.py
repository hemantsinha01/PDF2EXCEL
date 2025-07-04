import pandas as pd
import numpy as np
import re
import sys
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = ['Txn Date', 'Value Date', 'Cheque No.', 'Description', 
                    'Branch Code', 'Debit', 'Credit', 'Balance']

def is_date_like(value):
    if pd.isna(value) or value == '':
        return False
    value_str = str(value).strip()
    date_patterns = [
        r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
        r'\d{2,4}[/-]\d{1,2}[/-]\d{1,2}',
        r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{2,4}',
    ]
    return any(re.match(pattern, value_str, re.IGNORECASE) for pattern in date_patterns)

def clean_bank_statement_csv(input_file, output_file):
    try:
        logger.info(f"Reading input file: {input_file}")
        df = pd.read_csv(input_file, header=None)

        logger.info("Step 1: Removing duplicate headers...")
        cleaned_rows = []
        header_found = False

        for _, row in df.iterrows():
            is_header = False
            if len(row) >= len(EXPECTED_COLUMNS):
                row_values = [str(val).strip() for val in row[:len(EXPECTED_COLUMNS)]]
                if all(col in row_values for col in EXPECTED_COLUMNS[:4]):
                    is_header = True

            if is_header:
                if not header_found:
                    cleaned_rows.append(row)
                    header_found = True
                continue  # Skip duplicate headers
            else:
                cleaned_rows.append(row)

        cleaned_df = pd.DataFrame(cleaned_rows)

        if len(cleaned_df) == 0:
            logger.error("No data left after removing duplicate headers.")
            sys.exit(1)

        # Set column headers
        cleaned_df.columns = cleaned_df.iloc[0]
        cleaned_df = cleaned_df.drop(cleaned_df.index[0]).reset_index(drop=True)

        logger.info("Step 2: Consolidating fragmented rows...")
        consolidated_rows = []
        current_row = None

        for _, row in cleaned_df.iterrows():
            txn_date = row.iloc[0] if len(row) > 0 else None

            if is_date_like(txn_date):
                if current_row is not None:
                    consolidated_rows.append(current_row)
                current_row = row.copy()
            else:
                if current_row is not None:
                    for i, cell in enumerate(row):
                        if pd.notna(cell) and str(cell).strip() != '':
                            if i == 3 and pd.notna(current_row.iloc[3]):  # Description
                                current_row.iloc[3] = str(current_row.iloc[3]) + ' ' + str(cell)
                            elif pd.isna(current_row.iloc[i]) or str(current_row.iloc[i]).strip() == '':
                                current_row.iloc[i] = cell

        if current_row is not None:
            consolidated_rows.append(current_row)

        if not consolidated_rows:
            logger.error("No valid transactions found after consolidation.")
            sys.exit(1)

        final_df = pd.DataFrame(consolidated_rows)

        logger.info("Step 3: Final cleanup...")

        if len(final_df.columns) < len(EXPECTED_COLUMNS):
            for i in range(len(final_df.columns), len(EXPECTED_COLUMNS)):
                final_df[EXPECTED_COLUMNS[i]] = ''

        final_df.columns = EXPECTED_COLUMNS[:len(final_df.columns)]

        if 'Description' in final_df.columns:
            final_df['Description'] = (
                final_df['Description'].astype(str)
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip()
            )

        final_df = final_df[final_df['Txn Date'].apply(is_date_like)].reset_index(drop=True)

        logger.info(f"✅ Cleaned and consolidated {len(final_df)} transactions.")

        final_df.to_csv(output_file, index=False)
        logger.info(f"✅ Output saved to: {output_file}")

    except FileNotFoundError:
        logger.error(f"File not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error during consolidation: {e}")
        sys.exit(2)

def main():
    if len(sys.argv) != 3:
        logger.error("Usage: python 02_consolidate.py <input_csv> <output_csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    clean_bank_statement_csv(input_file, output_file)

if __name__ == "__main__":
    main()
