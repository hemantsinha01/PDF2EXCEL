import pandas as pd
import argparse
import logging
import sys
import os
from pathlib import Path

# Configure logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def merge_narration_rows(input_file, output_file):
    """
    Merge narration rows where empty date rows are continuation of previous transaction.
    Common in bank statements where long descriptions span multiple lines.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output merged CSV file
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Validate input file exists
        if not os.path.exists(input_file):
            logger.error(f"❌ Input file does not exist: {input_file}")
            return False
        
        # Check file size
        file_size = os.path.getsize(input_file)
        if file_size == 0:
            logger.error(f"❌ Input file is empty: {input_file}")
            return False
        
        logger.info(f"📥 Reading: {input_file}")
        
        # Read CSV with error handling
        try:
            df = pd.read_csv(input_file, dtype=str, keep_default_na=False)
        except pd.errors.EmptyDataError:
            logger.error(f"❌ The CSV file contains no data: {input_file}")
            return False
        except pd.errors.ParserError as e:
            logger.error(f"❌ Error parsing CSV file: {e}")
            return False
        
        if len(df) == 0:
            logger.warning(f"⚠️  CSV file contains no rows: {input_file}")
            return False
        
        logger.info(f"📊 Loaded {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"📋 Original columns: {list(df.columns)}")

        # Clean column names (remove extra spaces)
        df.columns = [col.strip() for col in df.columns]
        logger.info(f"📋 Cleaned columns: {list(df.columns)}")

        # Find Date and Narration columns
        date_col = narration_col = None
        
        for col in df.columns:
            col_lower = col.lower().strip()
            if 'date' in col_lower and not 'value' in col_lower:
                date_col = col
            elif 'narration' in col_lower or 'description' in col_lower or 'particulars' in col_lower:
                narration_col = col

        # Validate required columns exist
        if not date_col:
            logger.error("❌ Date column not found")
            logger.error(f"Available columns: {list(df.columns)}")
            return False
        
        if not narration_col:
            logger.error("❌ Narration column not found")
            logger.error(f"Available columns: {list(df.columns)}")
            return False

        logger.info(f"✅ Using Date column: '{date_col}'")
        logger.info(f"✅ Using Narration column: '{narration_col}'")

        # Show sample data for debugging
        logger.info(f"📄 First 3 rows of data:")
        for i in range(min(3, len(df))):
            date_val = df.iloc[i][date_col]
            narr_val = df.iloc[i][narration_col]
            logger.info(f"  Row {i}: Date='{date_val}', Narration='{narr_val}'")

        # Create processed dataframe
        processed_rows = []
        current_row = None
        rows_merged = 0
        
        def is_empty_date(date_val):
            """Check if date value is empty or invalid"""
            if pd.isna(date_val):
                return True
            date_str = str(date_val).strip()
            return date_str == '' or date_str.lower() in ['nan', 'null', 'none']
        
        def clean_narration(narr_val):
            """Clean narration text"""
            if pd.isna(narr_val):
                return ""
            return str(narr_val).strip()

        logger.info("🔄 Processing rows for narration merging...")
        
        for idx, row in df.iterrows():
            date_val = row[date_col]
            narr_val = clean_narration(row[narration_col])
            
            if is_empty_date(date_val):
                # This is a continuation row
                if current_row is not None and narr_val:
                    # Merge with previous row's narration
                    current_narration = clean_narration(current_row[narration_col])
                    if current_narration:
                        current_row[narration_col] = current_narration + " " + narr_val
                    else:
                        current_row[narration_col] = narr_val
                    rows_merged += 1
                    logger.debug(f"Merged row {idx} narration: '{narr_val}' with previous")
                # Skip this row as it's been merged
                continue
            else:
                # This is a new transaction row
                if current_row is not None:
                    # Save the previous complete row
                    processed_rows.append(current_row.copy())
                
                # Start new transaction
                current_row = row.copy()
        
        # Don't forget the last row
        if current_row is not None:
            processed_rows.append(current_row.copy())
        
        if not processed_rows:
            logger.error("❌ No valid rows found after processing")
            logger.error("This might indicate an issue with date column detection or data format")
            
            # Debug: Show unique date values
            unique_dates = df[date_col].unique()[:10]  # First 10 unique values
            logger.error(f"Sample date values: {list(unique_dates)}")
            return False
        
        # Create new dataframe from processed rows
        df_merged = pd.DataFrame(processed_rows)
        
        # Reset index
        df_merged.reset_index(drop=True, inplace=True)
        
        logger.info(f"✅ Processing completed:")
        logger.info(f"   Original rows: {len(df)}")
        logger.info(f"   Merged rows: {rows_merged}")
        logger.info(f"   Final rows: {len(df_merged)}")
        
        # Show sample of merged data
        if len(df_merged) > 0:
            logger.info("📄 Sample of processed data:")
            for i in range(min(3, len(df_merged))):
                date_val = df_merged.iloc[i][date_col]
                narr_val = df_merged.iloc[i][narration_col]
                # Truncate long narrations for display
                display_narr = (narr_val[:50] + '...') if len(narr_val) > 50 else narr_val
                logger.info(f"  Row {i}: Date='{date_val}', Narration='{display_narr}'")

        # Create output directory if needed
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save merged data
        logger.info(f"📤 Writing merged data to: {output_file}")
        df_merged.to_csv(output_file, index=False)
        
        # Verify output file
        if not os.path.exists(output_file):
            logger.error(f"❌ Failed to create output file: {output_file}")
            return False
            
        output_size = os.path.getsize(output_file)
        logger.info(f"✅ Output saved successfully ({output_size:,} bytes)")
        
        return True

    except FileNotFoundError:
        logger.error(f"❌ File not found: {input_file}")
        return False
    except PermissionError:
        logger.error(f"❌ Permission denied accessing file: {input_file}")
        return False
    except Exception as e:
        logger.exception(f"❌ Unexpected error while processing: {e}")
        return False

def main():
    """Main function to handle command line arguments and execute the script."""
    parser = argparse.ArgumentParser(
        description="Merge narration rows where empty date rows are continuation of previous transaction.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 02_mergenarration.py input.csv output.csv
  python 02_mergenarration.py input.csv output.csv --debug

This script handles bank statement formats where:
- Row 1: Date='01/04/24', Narration='UPI-ASHOK B S-914817752897@PAYTM'
- Row 2: Date='', Narration='0033-445806747361-NA'  (continuation)
- Row 3: Date='01/04/24', Narration='UPI-VIRUPAKSHI-8073663066@YBL'

Result:
- Row 1: Date='01/04/24', Narration='UPI-ASHOK B S-914817752897@PAYTM 0033-445806747361-NA'
- Row 2: Date='01/04/24', Narration='UPI-VIRUPAKSHI-8073663066@YBL'
        """
    )
    parser.add_argument("input_file", help="Path to input CSV file")
    parser.add_argument("output_file", help="Path to output merged CSV file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--version", action="version", version="%(prog)s 1.0")
    
    args = parser.parse_args()

    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("🐛 Debug logging enabled")

    # Validate input arguments
    if not args.input_file or not args.output_file:
        logger.error("❌ Both input and output file paths are required")
        parser.print_help()
        sys.exit(1)

    # Check if input and output are the same file
    if os.path.abspath(args.input_file) == os.path.abspath(args.output_file):
        logger.error("❌ Input and output files cannot be the same")
        sys.exit(1)

    logger.info("🚀 Starting narration merge process...")
    logger.info(f"📥 Input: {args.input_file}")
    logger.info(f"📤 Output: {args.output_file}")

    # Execute the main function
    success = merge_narration_rows(args.input_file, args.output_file)
    
    if success:
        logger.info("🎉 Script completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Script failed - check the error messages above")
        sys.exit(1)

if __name__ == "__main__":
    main()