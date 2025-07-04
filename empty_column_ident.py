import pandas as pd

def detect_empty_leading_cells_with_indices(input_file):
    df = pd.read_csv(input_file, dtype=str, keep_default_na=False)
    df.columns = [col.strip() for col in df.columns]
    original_headers = df.columns.tolist()
    results = []

    for col_idx, col_name in enumerate(df.columns):
        if col_name == "" or col_name.startswith("Unnamed"):
            col_data = df.iloc[:, col_idx]
            
            # Count consecutive empty cells starting from row 0 (header is row 0)
            empty_count = 0
            for val in col_data:
                if str(val).strip() == "":
                    empty_count += 1
                else:
                    break

            if empty_count > 0:
                results.append({
                    "col_idx": col_idx,
                    "start_row": 0,  # Always start from header row
                    "end_row": empty_count  # Number of empty rows (header + data rows)
                })

    return results, df, original_headers

def shift_left_selected_block(df, col_idx, start_row, end_row):
    # Adjust end_row to be exclusive (Python style)
    for i in range(start_row, end_row):
        row = df.iloc[i].tolist()
        # Remove the cell at target column, shift left, add empty cell at end
        row = row[:col_idx] + row[col_idx+1:] + [""]
        df.iloc[i] = row
    return df

def process_bank_statement(input_file, output_file):
    # Step 1: Detect empty columns
    results, df, original_headers = detect_empty_leading_cells_with_indices(input_file)
    
    if not results:
        print("\n✅ No unnamed columns with leading empty values found.")
        df.to_csv(output_file, index=False)
        return
    
    print("\n🔍 Found unnamed columns with leading empty cells:")
    for r in results:
        print(f"➡️ col_idx = {r['col_idx']}")
        print(f"   start_row = {r['start_row']}, end_row = {r['end_row']} (affects rows {r['start_row']} to {r['end_row']-1})")

    # Step 2: Process each detected empty column
    for r in results:
        df = shift_left_selected_block(
            df=df,
            col_idx=r['col_idx'],
            start_row=r['start_row'],
            end_row=r['end_row']  # end_row is already correct (exclusive)
        )
    
    # Step 3: Fix only the specific header that was shifted
    if len(original_headers) > 0:
        # Create new headers list
        new_headers = original_headers.copy()
        
        # For each empty column found, shift its header left
        for r in results:
            col_idx = r['col_idx']
            if col_idx < len(new_headers):
                # Remove the empty column's header and shift left
                new_headers.pop(col_idx)
                # Add empty string at end to maintain length
                new_headers.append("")
        
        df.columns = new_headers
    
    # Step 4: Save with headers
    df.to_csv(output_file, index=False)
    print(f"\n✅ Processing complete. Output saved to {output_file} with headers preserved.")

if __name__ == "__main__":
    input_path = input("Enter path to extracted CSV from Tabula: ").strip()
    output_path = input("Enter output CSV path: ").strip()
    process_bank_statement(input_path, output_path)