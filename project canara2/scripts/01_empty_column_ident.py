#!/usr/bin/env python3
"""
Empty Column Identification and Processing Script
Pipeline Stage 1: Identifies and removes empty columns from CSV files

Usage:
    python 01_empty_column_ident.py <input_file> <output_file>
    
Example:
    python 01_empty_column_ident.py extracted.csv empty_cols_removed.csv
"""

import pandas as pd
import sys
import os
import argparse
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_logging(log_file=None):
    """Setup logging configuration"""
    if log_file:
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)

def validate_input_file(input_file):
    """Validate input file exists and is readable"""
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    if not os.path.isfile(input_file):
        raise ValueError(f"Input path is not a file: {input_file}")
    
    if not input_file.lower().endswith('.csv'):
        logger.warning(f"Input file doesn't have .csv extension: {input_file}")
    
    try:
        # Test if file is readable
        with open(input_file, 'r') as f:
            f.read(1)
    except Exception as e:
        raise ValueError(f"Cannot read input file: {e}")

def ensure_output_directory(output_file):
    """Ensure output directory exists"""
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")

def detect_empty_leading_cells_with_indices(df):
    """
    Detect empty columns with leading empty cells
    
    Args:
        df: pandas DataFrame
        
    Returns:
        tuple: (results list, processed dataframe)
    """
    results = []
    
    try:
        # Clean column names
        df.columns = [str(col).strip() for col in df.columns]
        original_headers = df.columns.tolist()
        
        logger.info(f"Analyzing {len(df.columns)} columns for empty leading cells...")
        
        for col_idx, col_name in enumerate(df.columns):
            # Check if column is unnamed or empty
            if col_name == "" or col_name.startswith("Unnamed") or col_name.lower() in ['nan', 'none']:
                col_data = df.iloc[:, col_idx]
                
                # Count consecutive empty cells starting from row 0
                empty_count = 0
                for val in col_data:
                    if str(val).strip() == "" or str(val).strip().lower() in ['nan', 'none']:
                        empty_count += 1
                    else:
                        break
                
                if empty_count > 0:
                    results.append({
                        "col_idx": col_idx,
                        "col_name": col_name,
                        "start_row": 0,
                        "end_row": empty_count,
                        "total_rows": len(df)
                    })
                    
                    logger.debug(f"Found empty column at index {col_idx}: '{col_name}' "
                               f"with {empty_count} leading empty cells")
        
        logger.info(f"Found {len(results)} unnamed columns with leading empty cells")
        return results, df, original_headers
        
    except Exception as e:
        logger.error(f"Error detecting empty columns: {e}")
        raise

def shift_left_selected_block(df, col_idx, start_row, end_row):
    """
    Shift cells left for specified block
    
    Args:
        df: pandas DataFrame
        col_idx: column index to remove
        start_row: starting row index
        end_row: ending row index (exclusive)
        
    Returns:
        pandas DataFrame: modified dataframe
    """
    try:
        # Adjust end_row to be exclusive (Python style)
        for i in range(start_row, min(end_row, len(df))):
            row = df.iloc[i].tolist()
            # Remove the cell at target column, shift left, add empty cell at end
            row = row[:col_idx] + row[col_idx+1:] + [""]
            df.iloc[i] = row
        
        return df
        
    except Exception as e:
        logger.error(f"Error shifting cells: {e}")
        raise

def process_bank_statement(input_file, output_file):
    """
    Main processing function
    
    Args:
        input_file: path to input CSV file
        output_file: path to output CSV file
        
    Returns:
        dict: processing results
    """
    try:
        # Validate inputs
        validate_input_file(input_file)
        ensure_output_directory(output_file)
        
        logger.info(f"Processing file: {input_file}")
        logger.info(f"Output file: {output_file}")
        
        # Read CSV file
        try:
            df = pd.read_csv(input_file, dtype=str, keep_default_na=False)
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise ValueError(f"Cannot read CSV file: {e}")
        
        if df.empty:
            logger.warning("Input CSV file is empty")
            df.to_csv(output_file, index=False)
            return {"status": "success", "message": "Empty file processed", "changes": 0}
        
        logger.info(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        
        # Step 1: Detect empty columns
        results, df, original_headers = detect_empty_leading_cells_with_indices(df)
        
        if not results:
            logger.info("No unnamed columns with leading empty values found")
            df.to_csv(output_file, index=False)
            return {
                "status": "success", 
                "message": "No changes needed", 
                "changes": 0,
                "input_rows": len(df),
                "output_rows": len(df),
                "input_columns": len(df.columns),
                "output_columns": len(df.columns)
            }
        
        logger.info(f"Found {len(results)} columns to process:")
        for r in results:
            logger.info(f"  Column {r['col_idx']} ('{r['col_name']}'): "
                       f"{r['end_row']} leading empty cells")
        
        # Step 2: Process each detected empty column
        changes_made = 0
        for r in results:
            try:
                df = shift_left_selected_block(
                    df=df,
                    col_idx=r['col_idx'],
                    start_row=r['start_row'],
                    end_row=r['end_row']
                )
                changes_made += 1
                logger.debug(f"Processed column {r['col_idx']}")
                
            except Exception as e:
                logger.error(f"Error processing column {r['col_idx']}: {e}")
                continue
        
        # Step 3: Fix headers
        if len(original_headers) > 0:
            try:
                new_headers = original_headers.copy()
                
                # Remove empty column headers and shift left
                for r in sorted(results, key=lambda x: x['col_idx'], reverse=True):
                    col_idx = r['col_idx']
                    if col_idx < len(new_headers):
                        new_headers.pop(col_idx)
                        new_headers.append("")
                
                df.columns = new_headers
                logger.debug("Headers updated successfully")
                
            except Exception as e:
                logger.error(f"Error updating headers: {e}")
        
        # Step 4: Save output
        try:
            df.to_csv(output_file, index=False)
            logger.info(f"Successfully saved processed data to: {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving output file: {e}")
            raise
        
        # Return processing results
        return {
            "status": "success",
            "message": f"Processed {changes_made} empty columns successfully",
            "changes": changes_made,
            "input_rows": len(df),
            "output_rows": len(df),
            "input_columns": len(original_headers),
            "output_columns": len(df.columns),
            "processed_columns": [r['col_name'] for r in results]
        }
        
    except Exception as e:
        logger.error(f"Error in process_bank_statement: {e}")
        raise

def main():
    """Main function for command line usage"""
    parser = argparse.ArgumentParser(
        description='Process CSV files to remove empty columns',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python 01_empty_column_ident.py input.csv output.csv
    python 01_empty_column_ident.py --verbose input.csv output.csv
    python 01_empty_column_ident.py --log-file process.log input.csv output.csv
        """
    )
    
    parser.add_argument('input_file', help='Input CSV file path')
    parser.add_argument('output_file', help='Output CSV file path')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    parser.add_argument('--log-file', help='Log file path')
    
    args = parser.parse_args()
    
    # Setup logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    if args.log_file:
        setup_logging(args.log_file)
    
    try:
        # Process the file
        result = process_bank_statement(args.input_file, args.output_file)
        
        # Print results
        print(f"\n✅ Processing complete!")
        print(f"Status: {result['status']}")
        print(f"Message: {result['message']}")
        print(f"Changes made: {result['changes']}")
        print(f"Input: {result['input_rows']} rows, {result['input_columns']} columns")
        print(f"Output: {result['output_rows']} rows, {result['output_columns']} columns")
        
        if result['processed_columns']:
            print(f"Processed columns: {', '.join(result['processed_columns'])}")
        
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()