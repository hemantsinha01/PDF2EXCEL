import csv
import re
import sys
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def is_valid_txn_row(row):
    """Check if row has a valid Txn Date and Description (indicates it's a transaction row)."""
    if len(row) < 7:
        return False
    date_pattern = r'^\d{2}-\d{2}-\d{4}'  # Txn Date: DD-MM-YYYY
    return bool(re.match(date_pattern, row[0]))

def clean_transaction_csv(input_file, output_file):
    try:
        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            sys.exit(1)

        with open(input_file, newline='', encoding='utf-8') as infile, \
             open(output_file, 'w', newline='', encoding='utf-8') as outfile:

            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            seen_headers = False
            row_count = 0

            for row in reader:
                if not any(cell.strip() for cell in row):
                    continue  # skip blank rows

                if row[0].strip().lower() == "txn date":
                    if not seen_headers:
                        writer.writerow(row)  # write header once
                        seen_headers = True
                    continue  # skip subsequent headers

                if is_valid_txn_row(row):
                    writer.writerow(row)
                    row_count += 1

            logger.info(f"✅ Cleaned {row_count} transaction rows.")
            logger.info(f"✅ Output written to: {output_file}")

    except Exception as e:
        logger.exception(f"❌ Error during cleaning: {str(e)}")
        sys.exit(2)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.error("Usage: python 01_clean.py <input_csv> <output_csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    clean_transaction_csv(input_file, output_file)
