# scripts/split.py

import pandas as pd
import re
import sys
import os

def clean_bank_statement_csv(input_file, output_file):
    try:
        # Load CSV
        df = pd.read_csv(input_file, dtype=str, keep_default_na=False)
        print(f"✅ Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        print(f"📋 Columns: {list(df.columns)}")

        # 1. Remove Statement Summary section and everything after it
        summary_keywords = ['Statement Summary', 'Opening Balance', 'Total Withdrawal Amount', 
                          'Total Deposit Amount', 'Closing Balance', 'Withdrawal Count', 
                          'Deposit Count', 'Any discrepancy']
        
        # Find the first row that contains any summary keyword
        summary_start_idx = None
        for idx, row in df.iterrows():
            row_text = ' '.join(str(val) for val in row.values).lower()
            if any(keyword.lower() in row_text for keyword in summary_keywords):
                summary_start_idx = idx
                break
        
        if summary_start_idx is not None:
            df = df.iloc[:summary_start_idx].copy()
            print(f"✅ Removed Statement Summary section from row {summary_start_idx}")

        # 2. Find and fix Date column issues
        date_col = None
        for col in df.columns:
            if 'date' in col.lower():
                date_col = col
                break
        
        # 3. Find Chq/Ref No column
        chq_ref_col = None
        for col in df.columns:
            if 'chq' in col.lower() or 'ref' in col.lower():
                chq_ref_col = col
                break

        if date_col:
            print(f"✅ Found date column: '{date_col}'")
            if chq_ref_col:
                print(f"✅ Found Chq/Ref column: '{chq_ref_col}'")
            
            # Process the date column to separate merged date-narration entries
            # Only process entries that have merged data (date + text)
            date_pattern = r'^(\d{2}-\d{2}-\d{4})\s+'  # Date followed by space and more text
            
            i = 0
            while i < len(df):
                entry_str = str(df.at[i, date_col]).strip()
                
                # Check if this entry contains merged date-narration data
                match = re.match(date_pattern, entry_str)
                if match and len(entry_str) > 10:  # More than just a date
                    date = match.group(1)
                    remaining_text = entry_str[match.end():].strip()
                    
                    # Split the remaining text to extract Chq/Ref and Narration
                    # Look for patterns that might indicate Chq/Ref (numbers, codes, etc.)
                    chq_ref_value = ""
                    narration_parts = []
                    
                    # Split by spaces and analyze parts
                    parts = remaining_text.split()
                    if parts:
                        # Check if first part looks like a Chq/Ref (contains numbers/codes)
                        first_part = parts[0]
                        if (re.search(r'\d', first_part) or 
                            len(first_part) <= 15 and 
                            not any(word in first_part.lower() for word in ['to', 'from', 'by', 'for', 'the', 'and', 'of', 'in', 'on', 'at'])):
                            chq_ref_value = first_part
                            narration_parts = parts[1:]
                        else:
                            narration_parts = parts
                    
                    # Look ahead for continuation of narration in next rows
                    j = i + 1
                    while j < len(df):
                        next_entry = str(df.at[j, date_col]).strip()
                        
                        # If next row doesn't have a date pattern and isn't empty, it's continuation
                        if (not re.match(r'^\d{2}-\d{2}-\d{4}', next_entry) and 
                            next_entry != '' and 
                            next_entry.lower() not in ['date', 'narration', 'withdrawal', 'deposit', 'balance', 'chq/ref']):
                            
                            narration_parts.append(next_entry)
                            # Clear this row's date column as we're merging it
                            df.at[j, date_col] = ''
                            j += 1
                        else:
                            break
                    
                    # Merge all narration parts
                    full_narration = ' '.join(narration_parts).strip()
                    
                    # Update the date column with clean date
                    df.at[i, date_col] = date
                    
                    # Update Chq/Ref column if it exists and we found a value
                    if chq_ref_col and chq_ref_value:
                        df.at[i, chq_ref_col] = chq_ref_value
                    
                    # Update narration column if it exists
                    if 'Narration' in df.columns:
                        df.at[i, 'Narration'] = full_narration
                
                i += 1
            
            print("✅ Processed merged Date-Narration entries and preserved Chq/Ref data")

        # 4. Find and fix Withdrawal/Deposit column issues
        withdrawal_col = None
        for col in df.columns:
            if 'withdrawal' in col.lower() and 'deposit' in col.lower():
                withdrawal_col = col
                break

        if withdrawal_col:
            print(f"✅ Found withdrawal/deposit column: '{withdrawal_col}'")
            
            # Pattern to match amount with (Cr) or (Dr)
            amount_pattern = r'([0-9,]+\.?[0-9]*)\s*\((Cr|Dr)\)'
            
            for idx, entry in enumerate(df[withdrawal_col]):
                entry_str = str(entry).strip()
                
                # Find all amounts with (Cr) or (Dr)
                matches = re.findall(amount_pattern, entry_str)
                
                if len(matches) >= 2:
                    # This entry has merged data - separate it
                    first_amount = f"{matches[0][0]}({matches[0][1]})"
                    second_amount = f"{matches[1][0]}({matches[1][1]})"
                    
                    # Update withdrawal column with first amount
                    df.at[idx, withdrawal_col] = first_amount
                    
                    # Update balance column with second amount
                    if 'Balance' in df.columns:
                        df.at[idx, 'Balance'] = second_amount
            
            print("✅ Processed merged Withdrawal-Balance entries while preserving existing data")

        # 5. Clean up header rows (remove rows that are just column headers)
        header_keywords = ['withdrawal', 'deposit', 'balance', 'date', 'narration', 'chq/ref']
        rows_to_remove = []
        
        for idx, row in df.iterrows():
            row_text = ' '.join(str(val) for val in row.values).lower()
            # If a row contains mostly header keywords and little other content
            keyword_count = sum(1 for keyword in header_keywords if keyword in row_text)
            if keyword_count >= 2 and len(row_text.replace(' ', '')) < 50:
                rows_to_remove.append(idx)
        
        if rows_to_remove:
            df = df.drop(rows_to_remove).reset_index(drop=True)
            print(f"✅ Removed {len(rows_to_remove)} header rows")

        # 6. Remove empty rows
        df = df.dropna(how='all').reset_index(drop=True)
        df = df[~df.apply(lambda row: all(str(val).strip() == '' for val in row), axis=1)].reset_index(drop=True)
        print(f"✅ Final dataset has {len(df)} rows")

        # Save output
        df.to_csv(output_file, index=False)
        print(f"\n✅ Final cleaned CSV saved to: {output_file}")

    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("❌ Usage: python split.py <input_csv> <output_csv>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_csv = sys.argv[2]

    if not os.path.exists(input_csv):
        print(f"❌ Input file not found: {input_csv}")
        sys.exit(1)

    print(f"🔧 Running split.py on: {input_csv}")
    clean_bank_statement_csv(input_csv, output_csv)