import pandas as pd

file_path = "canara_new.csv"
output_file = "excel_like_shift_left_Csv.csv"

# Read the file (assume Excel, no header)
df = pd.read_csv(file_path, header=None, dtype=str).fillna("")

# The selected block is C1:C26 (Excel), which is [row 0-25, col 2] in pandas
start_row, end_row = 0, 26    # python index: 0 to 25 (end is exclusive, so 26)
col_idx = 2                   # column C is index 2

for i in range(start_row, end_row):
    row = df.iloc[i].tolist()
    # Remove the cell at column C, shift left, add empty cell at the end to keep length
    row = row[:col_idx] + row[col_idx+1:] + [""]
    df.iloc[i] = row

df.to_csv(output_file, index=False, header=False)
print("Done! Your selected block C1:C26 was deleted and shifted left (just like Excel).")
 