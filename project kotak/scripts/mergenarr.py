# scripts/mergenarr.py

import pandas as pd
import sys
import os

def merge_narration_and_remove_empty_rows(input_file, output_file):
    try:
        # Load CSV
        df = pd.read_csv(input_file, dtype=str, keep_default_na=False)

        cleaned_rows = []
        for i in range(len(df)):
            row = df.iloc[i]

            # Skip if row is completely empty
            if all(cell.strip() == "" for cell in row.values):
                continue

            # Merge Narration if Date is missing
            if i > 0 and row.get("Date", "").strip() == "" and row.get("Narration", "").strip():
                prev_row = cleaned_rows[-1]
                prev_row["Narration"] = (prev_row["Narration"] + " " + row["Narration"]).strip()
            else:
                cleaned_rows.append(row)

        # Create cleaned DataFrame
        df_cleaned = pd.DataFrame(cleaned_rows)

        # Save to CSV
        df_cleaned.to_csv(output_file, index=False)
        #print(f"\n✅ Narration merged & empty rows removed. Saved to: {output_file}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

# Entry point
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("❌ Usage: python mergenarr.py <input_csv> <output_csv>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_csv = sys.argv[2]

    if not os.path.exists(input_csv):
        print(f"❌ Input file not found: {input_csv}")
        sys.exit(1)

    print(f"🔧 Running mergenarr.py on: {input_csv}")
    merge_narration_and_remove_empty_rows(input_csv, output_csv)
