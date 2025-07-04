import pandas as pd
import numpy as np
import re
from datetime import datetime

def clean_bank_statement_csv(input_file, output_file):
    """
    Clean bank statement CSV by:
    1. Removing duplicate headers
    2. Consolidating rows that belong together based on transaction dates
    3. Working with columns: Date, Particulars, Withdrawals, Deposits, Balance
    """
    
    # Read the CSV file
    print("Reading CSV file...")
    df = pd.read_csv(input_file, header=None)
    
    # Define the expected column names for your dataset
    expected_columns = ['Date', 'Particulars', 'Withdrawals', 'Deposits', 'Balance']
    
    print("Step 1: Removing duplicate headers...")
    
    # Find and remove duplicate header rows
    cleaned_rows = []
    header_found = False
    
    for index, row in df.iterrows():
        # Check if current row is a header row
        is_header = False
        if len(row) >= len(expected_columns):
            # Check if the row contains expected column names
            row_values = [str(val).strip().lower() for val in row[:len(expected_columns)]]
            # Check for key column names (case insensitive)
            header_keywords = ['date', 'particular', 'withdrawal', 'deposit', 'balance']
            matching_keywords = sum(1 for keyword in header_keywords 
                                  if any(keyword in val for val in row_values))
            
            if matching_keywords >= 3:  # If at least 3 keywords match, it's likely a header
                is_header = True
        
        if is_header:
            if not header_found:
                # Keep the first header
                cleaned_rows.append(row)
                header_found = True
                print(f"Header found at row {index}: {row.tolist()}")
            else:
                print(f"Duplicate header removed at row {index}")
            # Skip subsequent headers
        else:
            # Keep non-header rows
            cleaned_rows.append(row)
    
    # Create DataFrame from cleaned rows
    cleaned_df = pd.DataFrame(cleaned_rows)
    
    # Set the first row as header if we found one
    if len(cleaned_df) > 0 and header_found:
        # Clean up column names
        header_row = cleaned_df.iloc[0]
        clean_columns = []
        for col in header_row:
            col_str = str(col).strip()
            if 'date' in col_str.lower():
                clean_columns.append('Date')
            elif 'particular' in col_str.lower():
                clean_columns.append('Particulars')
            elif 'withdrawal' in col_str.lower():
                clean_columns.append('Withdrawals')
            elif 'deposit' in col_str.lower():
                clean_columns.append('Deposits')
            elif 'balance' in col_str.lower():
                clean_columns.append('Balance')
            else:
                clean_columns.append(col_str)
        
        cleaned_df.columns = clean_columns
        cleaned_df = cleaned_df.drop(cleaned_df.index[0]).reset_index(drop=True)
    else:
        # No header found, use expected columns
        print("No header found, using default column names")
        if len(cleaned_df.columns) >= len(expected_columns):
            cleaned_df.columns = expected_columns + [f'Extra_{i}' for i in range(len(expected_columns), len(cleaned_df.columns))]
        else:
            cleaned_df.columns = expected_columns[:len(cleaned_df.columns)]
    
    print("Step 2: Consolidating fragmented rows...")
    
    # Function to check if a value looks like a date
    def is_date_like(value):
        if pd.isna(value) or value == '':
            return False
        
        value_str = str(value).strip()
        if value_str == '' or value_str.lower() in ['nan', 'none']:
            return False
            
        # Common date patterns for Indian bank statements
        date_patterns = [
            r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # DD/MM/YYYY or DD-MM-YYYY
            r'\d{2,4}[/-]\d{1,2}[/-]\d{1,2}',  # YYYY/MM/DD or YYYY-MM-DD
            r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{2,4}',  # DD Mon YYYY
            r'\d{1,2}[/-]\d{1,2}[/-]\d{4}',    # DD/MM/YYYY
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, value_str, re.IGNORECASE):
                return True
        return False
    
    # Function to check if a value looks like an amount (number)
    def is_amount_like(value):
        if pd.isna(value) or value == '':
            return False
        
        value_str = str(value).strip().replace(',', '').replace(' ', '')
        if value_str == '' or value_str.lower() in ['nan', 'none']:
            return False
        
        # Check if it's a number (including negative numbers and decimals)
        try:
            float(value_str)
            return True
        except ValueError:
            return False
    
    # Consolidate rows
    consolidated_rows = []
    current_row = None
    
    for index, row in cleaned_df.iterrows():
        # Check if this row starts a new transaction (has a date)
        date_value = row.iloc[0] if len(row) > 0 else None
        
        if is_date_like(date_value):
            # This is a new transaction row
            if current_row is not None:
                # Save the previous consolidated row
                consolidated_rows.append(current_row)
            
            # Start a new consolidated row
            current_row = row.copy()
            print(f"New transaction found at row {index}: {date_value}")
        else:
            # This row is part of the previous transaction or contains additional data
            if current_row is not None:
                # Consolidate this row with the current transaction
                for i, cell in enumerate(row):
                    if pd.notna(cell) and str(cell).strip() != '' and str(cell).strip().lower() != 'nan':
                        if i < len(current_row):
                            current_cell_value = str(cell).strip()
                            
                            # Handle different columns appropriately
                            if i == 1:  # Particulars column
                                if pd.notna(current_row.iloc[1]) and str(current_row.iloc[1]).strip() != '':
                                    # Append to existing particulars
                                    current_row.iloc[1] = str(current_row.iloc[1]).strip() + ' ' + current_cell_value
                                else:
                                    current_row.iloc[1] = current_cell_value
                            elif i >= 2:  # Amount columns (Withdrawals, Deposits, Balance)
                                if is_amount_like(cell):
                                    if pd.isna(current_row.iloc[i]) or str(current_row.iloc[i]).strip() == '':
                                        current_row.iloc[i] = current_cell_value
                            else:
                                # For other columns, fill if empty
                                if pd.isna(current_row.iloc[i]) or str(current_row.iloc[i]).strip() == '':
                                    current_row.iloc[i] = current_cell_value
            else:
                # If we don't have a current transaction but this row has amounts, 
                # it might be the continuation of data from previous pages
                has_amounts = any(is_amount_like(cell) for cell in row[2:] if len(row) > 2)
                if has_amounts:
                    print(f"Found orphaned amounts at row {index}, skipping: {row.tolist()}")
    
    # Don't forget the last row
    if current_row is not None:
        consolidated_rows.append(current_row)
    
    # Create final DataFrame
    if consolidated_rows:
        final_df = pd.DataFrame(consolidated_rows)
        
        # Clean up the data
        print("Step 3: Final cleanup...")
        
        # DEBUG: Print current DataFrame info
        print(f"DEBUG: final_df has {len(final_df.columns)} columns: {list(final_df.columns)}")
        print(f"DEBUG: expected_columns has {len(expected_columns)} columns: {expected_columns}")
        
        # Handle column count mismatch
        if len(final_df.columns) > len(expected_columns):
            # More columns than expected - first assign names, then remove extra columns
            column_names = expected_columns + [f'Extra_{i}' for i in range(len(expected_columns), len(final_df.columns))]
            final_df.columns = column_names
            print(f"DEBUG: Found {len(final_df.columns)} columns: {list(final_df.columns)}")
            
            # Check if extra columns are mostly empty and remove them
            extra_columns = [col for col in final_df.columns if col.startswith('Extra_')]
            for col in extra_columns:
                non_empty_count = final_df[col].notna().sum()
                empty_count = len(final_df) - non_empty_count
                print(f"DEBUG: Column '{col}' - Non-empty: {non_empty_count}, Empty: {empty_count}")
                
                # If the column is mostly empty (more than 80% empty), remove it
                if empty_count > len(final_df) * 0.8:
                    print(f"DEBUG: Removing mostly empty column '{col}'")
                    final_df = final_df.drop(columns=[col])
                else:
                    print(f"DEBUG: Keeping column '{col}' as it has significant data")
            
            print(f"DEBUG: Final columns after cleanup: {list(final_df.columns)}")
            
        elif len(final_df.columns) < len(expected_columns):
            # Fewer columns than expected - use only the available ones
            final_df.columns = expected_columns[:len(final_df.columns)]
            print(f"DEBUG: Used subset of expected columns: {list(final_df.columns)}")
        else:
            # Exact match
            final_df.columns = expected_columns
            print(f"DEBUG: Perfect match, column names: {list(final_df.columns)}")
        
        # Clean up particulars column (remove extra spaces)
        if 'Particulars' in final_df.columns:
            final_df['Particulars'] = final_df['Particulars'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
        
        # Clean up amount columns
        for col in ['Withdrawals', 'Deposits', 'Balance']:
            if col in final_df.columns:
                # Remove commas and clean up number formatting
                final_df[col] = final_df[col].astype(str).str.replace(',', '').str.strip()
                # Replace empty strings with NaN for better handling
                final_df[col] = final_df[col].replace(['', 'nan', 'None'], np.nan)
        
        # Remove rows that don't have valid dates
        if 'Date' in final_df.columns:
            final_df = final_df[final_df['Date'].apply(is_date_like)]
        
        # Reset index
        final_df = final_df.reset_index(drop=True)
        
        print(f"Cleaned data: {len(final_df)} transactions found")
        
        # Save to output file
        final_df.to_csv(output_file, index=False)
        print(f"Cleaned data saved to: {output_file}")
        
        # Display first few rows as preview
        print("\nPreview of cleaned data:")
        print(final_df.head())
        
        # Show column info
        print(f"\nColumn information:")
        for col in final_df.columns:
            non_null_count = final_df[col].notna().sum()
            print(f"  {col}: {non_null_count} non-null values")
        
        return final_df
    
    else:
        print("No valid transaction data found!")
        return None

def main():
    # File paths - update these to match your files
    input_file = "cleancol.csv"  # Change this to your input file path
    output_file = "cleaned_output2.csv"  # Change this to your desired output file path
    
    try:
        cleaned_data = clean_bank_statement_csv(input_file, output_file)
        
        if cleaned_data is not None:
            print(f"\n✅ Success! Cleaned data has been saved to '{output_file}'")
            print(f"Total transactions: {len(cleaned_data)}")
            print(f"Columns: {list(cleaned_data.columns)}")
        else:
            print("❌ Failed to process the CSV file")
            
    except FileNotFoundError:
        print(f"❌ Error: Could not find the input file '{input_file}'")
        print("Please make sure the file exists and the path is correct.")
    except Exception as e:
        print(f"❌ Error processing the file: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()