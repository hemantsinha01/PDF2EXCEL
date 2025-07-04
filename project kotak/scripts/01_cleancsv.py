# scripts/01_cleancsv.py

import pandas as pd
import sys
import os

def remove_split_repeating_headers(input_file, output_file):
    try:
        # Load entire file as raw (no header)
        df_raw = pd.read_csv(input_file, header=None, dtype=str, keep_default_na=False)

        # Helper function to identify a 3-row header block
        def is_split_header_block(start_idx):
            try:
                row0 = df_raw.iloc[start_idx].fillna("").tolist()
                row1 = df_raw.iloc[start_idx + 1].fillna("").tolist()
                row2 = df_raw.iloc[start_idx + 2].fillna("").tolist()
                return (
                    "Withdrawal(Dr)/" in row0 or "Withdrawal" in row0
                ) and (
                    "Date" in row1 and "Narration" in row1
                ) and (
                    "Deposit(Cr)" in row2 or "Deposit" in row2
                )
            except:
                return False

        # Step 1: Find first header block and build the actual header
        for i in range(len(df_raw) - 2):
            if is_split_header_block(i):
                row0 = df_raw.iloc[i].fillna("")
                row1 = df_raw.iloc[i + 1].fillna("")
                row2 = df_raw.iloc[i + 2].fillna("")
                merged_header = []
                for j in range(max(len(row0), len(row1), len(row2))):
                    parts = [
                        row0[j] if j < len(row0) else "",
                        row1[j] if j < len(row1) else "",
                        row2[j] if j < len(row2) else ""
                    ]
                    col = " ".join([p.strip() for p in parts if p.strip()])
                    merged_header.append(col)
                header_start_idx = i
                break
        else:
            print("❌ Could not find any 3-row header block.")
            return

        # Step 2: Remove all repeating header blocks (in 3-row chunks)
        rows_to_remove = set()
        for i in range(len(df_raw) - 2):
            if is_split_header_block(i):
                rows_to_remove.update([i, i + 1, i + 2])

        df_cleaned = df_raw.drop(index=list(rows_to_remove)).reset_index(drop=True)

        # Step 3: Apply the merged header
        df_cleaned.columns = merged_header

        # Step 4: Save the final cleaned file
        df_cleaned.to_csv(output_file, index=False)
        #print(f"\n✅ Cleaned file saved successfully to: {output_file}")

    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()

# Entry point for production use
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("❌ Usage: python 01_cleancsv.py <input_csv> <output_csv>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_csv = sys.argv[2]

    if not os.path.exists(input_csv):
        print(f"❌ Input file not found: {input_csv}")
        sys.exit(1)

    print(f"🔧 Running 01_cleancsv.py on: {input_csv}")
    remove_split_repeating_headers(input_csv, output_csv)
