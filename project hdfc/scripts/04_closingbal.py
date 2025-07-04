import pandas as pd
import argparse
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_balance_column_shift(input_file, output_file):
    try:
        df = pd.read_csv(input_file, dtype=str, keep_default_na=False)

        # Detect column names
        withdrawal_col = deposit_col = balance_col = None
        for col in df.columns:
            col_lower = col.lower()
            if 'withdrawal' in col_lower and 'amt' in col_lower:
                withdrawal_col = col
            elif 'deposit' in col_lower and 'amt' in col_lower:
                deposit_col = col
            elif 'closing' in col_lower and 'balance' in col_lower:
                balance_col = col

        if not all([withdrawal_col, deposit_col, balance_col]):
            logger.error("❌ Required columns not found.")
            logger.error(f"Withdrawal: {withdrawal_col}, Deposit: {deposit_col}, Balance: {balance_col}")
            sys.exit(1)

        logger.info(f"✅ Columns detected: Withdrawal='{withdrawal_col}', Deposit='{deposit_col}', Balance='{balance_col}'")

        df_corrected = df.copy()

        def is_empty_or_zero(val):
            if pd.isna(val) or str(val).strip().lower() in ['', 'nan']:
                return True
            try:
                return float(str(val).replace(",", "")) == 0.0
            except Exception:
                return False

        # Find rows with balance issues
        mask = df[balance_col].apply(is_empty_or_zero)
        indices_to_fix = df[mask].index.tolist()

        logger.info(f"🔧 Found {len(indices_to_fix)} rows with empty or zero Closing Balance")

        for idx in indices_to_fix:
            w = df.loc[idx, withdrawal_col]
            d = df.loc[idx, deposit_col]
            df_corrected.loc[idx, balance_col] = d
            df_corrected.loc[idx, deposit_col] = w
            df_corrected.loc[idx, withdrawal_col] = ""

        # Optional: Clean numeric values
        for col in [withdrawal_col, deposit_col, balance_col]:
            df_corrected[col] = (
                df_corrected[col]
                .str.replace(",", "", regex=False)
                .replace("", "0")
            )
            df_corrected[col] = pd.to_numeric(df_corrected[col], errors='coerce').fillna(0)

        df_corrected.to_csv(output_file, index=False)
        logger.info(f"✅ Output saved to: {output_file}")

    except FileNotFoundError:
        logger.error(f"❌ File not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"❌ Unexpected error while processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix shifted rows where Closing Balance is empty or zero.")
    parser.add_argument("input_file", help="Path to input CSV file")
    parser.add_argument("output_file", help="Path to output corrected CSV file")
    args = parser.parse_args()

    fix_balance_column_shift(args.input_file, args.output_file)
