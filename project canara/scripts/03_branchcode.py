import pandas as pd
import re
import sys
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def clean_branch_code(input_file, output_file):
    try:
        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            sys.exit(1)

        logger.info(f"Reading file: {input_file}")
        df = pd.read_csv(input_file)

        # Normalize column names to find 'Branch Code'
        def normalize(col):
            return re.sub(r'\s+', '', col).lower()

        normalized_cols = {normalize(col): col for col in df.columns}
        possible_names = ['branchcode']
        branch_col = next((normalized_cols[name] for name in possible_names if name in normalized_cols), None)

        if not branch_col:
            logger.error("❌ 'Branch Code' or similar column not found!")
            logger.error(f"Available columns: {list(df.columns)}")
            sys.exit(2)

        # Define pattern to clean noise after '33'
        pattern = r'^33[\s\n]*[/\\-]?\s*[\dA-Za-z]+.*$'

        def clean_entry(entry):
            if pd.isna(entry):
                return entry
            entry_str = str(entry).strip()
            if re.match(pattern, entry_str, re.DOTALL):
                return '33'
            return entry_str

        logger.info("Cleaning Branch Code entries...")
        df[branch_col] = df[branch_col].apply(clean_entry)

        # Rename to standard form
        df.rename(columns={branch_col: 'Branch Code'}, inplace=True)

        df.to_csv(output_file, index=False)
        logger.info(f"✅ Branch Code column cleaned and saved to: {output_file}")

    except Exception as e:
        logger.exception(f"Unexpected error during branch code cleaning: {e}")
        sys.exit(3)

def main():
    if len(sys.argv) != 3:
        logger.error("Usage: python 03_branchcode.py <input_csv> <output_csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    clean_branch_code(input_file, output_file)

if __name__ == "__main__":
    main()
