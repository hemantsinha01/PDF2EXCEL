import csv
import sys
from typing import List, Tuple, Set

class CSVCleaner:
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.target_headers = ['date', 'particulars', 'withdrawals', 'deposits', 'balance']
        self.original_data = []
        self.cleaned_data = []
        self.removed_columns = []
        
    def read_csv(self) -> List[List[str]]:
        """Read CSV file and return all rows as list of lists"""
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8', newline='') as file:
                reader = csv.reader(file)
                data = []
                for row in reader:
                    data.append(row)
                self.original_data = data
                return data
        except FileNotFoundError:
            print(f"Error: File '{self.csv_file_path}' not found.")
            sys.exit(1)
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            sys.exit(1)
    
    def normalize_header(self, header: str) -> str:
        """Normalize header text for comparison"""
        return str(header).strip().lower().replace(' ', '').replace('_', '')
    
    def is_header_row(self, row: List[str]) -> Tuple[bool, List[int]]:
        """Check if a row contains target headers and return positions"""
        if not row or len(row) < 4:
            return False, []
        
        # Normalize row headers
        normalized_row = [self.normalize_header(cell) for cell in row]
        
        # Find positions of target headers
        header_positions = []
        for target_header in self.target_headers:
            for i, cell in enumerate(normalized_row):
                if target_header in cell or cell in target_header:
                    header_positions.append(i)
                    break
        
        # Consider it a header row if at least 4 out of 5 headers are found
        is_header = len(header_positions) >= 4
        return is_header, header_positions
    
    def find_header_sections(self, data: List[List[str]]) -> List[Tuple[int, List[int]]]:
        """Find all header rows and their column positions"""
        header_sections = []
        
        for i, row in enumerate(data):
            is_header, positions = self.is_header_row(row)
            if is_header:
                header_sections.append((i, positions))
                
        return header_sections
    
    def find_expected_column_positions(self, header_sections: List[Tuple[int, List[int]]]) -> List[int]:
        """Find the expected column positions based on the most common pattern"""
        if not header_sections:
            return []
        
        # Find the header section with exactly 5 columns (the correct one)
        correct_positions = None
        for row_idx, positions in header_sections:
            if len(positions) == 5:
                correct_positions = positions
                break
        
        # If no perfect match, use the first one
        if correct_positions is None:
            correct_positions = header_sections[0][1]
        
        return correct_positions
    
    def find_empty_columns_in_section(self, data: List[List[str]], start_row: int, end_row: int, expected_positions: List[int]) -> Set[int]:
        """Find empty columns in a specific section"""
        empty_columns = set()
        
        if not data or start_row >= len(data):
            return empty_columns
        
        # Get the actual column count in this section
        max_cols = 0
        for i in range(start_row, min(end_row, len(data))):
            if data[i]:
                max_cols = max(max_cols, len(data[i]))
        
        # Check if there are extra columns compared to expected
        if max_cols > len(expected_positions):
            # Check each column to see if it's empty
            for col_idx in range(max_cols):
                if col_idx not in expected_positions:
                    # Check if this column is empty throughout the section
                    is_empty = True
                    for row_idx in range(start_row, min(end_row, len(data))):
                        if row_idx < len(data) and col_idx < len(data[row_idx]):
                            cell_value = str(data[row_idx][col_idx]).strip()
                            if cell_value and cell_value != '0' and cell_value.lower() not in ['null', '']:
                                is_empty = False
                                break
                    
                    if is_empty:
                        empty_columns.add(col_idx)
        
        return empty_columns
    
    def find_all_empty_columns(self, data: List[List[str]], header_sections: List[Tuple[int, List[int]]]) -> Set[int]:
        """Find all empty columns across all sections"""
        if not header_sections:
            return set()
        
        # Get expected column positions from the correct header
        expected_positions = self.find_expected_column_positions(header_sections)
        all_empty_columns = set()
        
        # Check each section between headers
        for i in range(len(header_sections)):
            start_row = header_sections[i][0]
            end_row = header_sections[i + 1][0] if i + 1 < len(header_sections) else len(data)
            
            # Check current section header row for extra columns
            header_row = data[start_row]
            current_positions = header_sections[i][1]
            
            # If this section has more columns than expected, find the empty ones
            if len(header_row) > len(expected_positions):
                for col_idx in range(len(header_row)):
                    # Check if this column index is not in the expected positions
                    header_cell = str(header_row[col_idx]).strip().lower()
                    
                    # If the header cell is empty or not one of our target headers
                    if not header_cell or header_cell not in [h.lower() for h in ['date', 'particulars', 'withdrawals', 'deposits', 'balance']]:
                        # Verify it's empty in the data rows too
                        is_empty = True
                        for row_idx in range(start_row + 1, end_row):
                            if row_idx < len(data) and col_idx < len(data[row_idx]):
                                cell_value = str(data[row_idx][col_idx]).strip()
                                if cell_value and cell_value != '0' and cell_value.lower() not in ['null', '']:
                                    is_empty = False
                                    break
                        
                        if is_empty:
                            all_empty_columns.add(col_idx)
        
        return all_empty_columns
    
    def remove_empty_columns(self, data: List[List[str]], empty_columns: Set[int]) -> List[List[str]]:
        """Remove empty columns from the data"""
        if not empty_columns:
            return data
        
        cleaned_data = []
        sorted_empty_cols = sorted(empty_columns, reverse=True)  # Remove from right to left
        
        for row in data:
            new_row = row.copy()
            # Remove empty columns (from right to left to maintain indices)
            for col_idx in sorted_empty_cols:
                if col_idx < len(new_row):
                    del new_row[col_idx]
            cleaned_data.append(new_row)
        
        return cleaned_data
    
    def clean_csv(self) -> Tuple[List[List[str]], Set[int]]:
        """Main method to clean the CSV file"""
        print(f"Processing CSV file: {self.csv_file_path}")
        data = self.read_csv()
        
        if not data:
            print("No data found in CSV file.")
            return [], set()
        
        print(f"Original data has {len(data)} rows")
        
        # Find header sections
        header_sections = self.find_header_sections(data)
        print(f"Found {len(header_sections)} header sections:")
        for i, (row_idx, positions) in enumerate(header_sections):
            print(f"  Section {i+1}: Row {row_idx+1}, Columns: {len(data[row_idx])}")
            print(f"    Headers: {[data[row_idx][pos] if pos < len(data[row_idx]) else 'N/A' for pos in positions]}")
        
        if not header_sections:
            print("Warning: No header rows found.")
            return data, set()
        
        # Find empty columns
        empty_columns = self.find_all_empty_columns(data, header_sections)
        print(f"Found {len(empty_columns)} empty columns to remove: {sorted(empty_columns)}")
        
        if not empty_columns:
            print("No empty columns found. Data is already clean.")
            self.cleaned_data = data
            return data, empty_columns
        
        # Show which columns will be removed
        if data and empty_columns:
            print("Columns to be removed:")
            for col_idx in sorted(empty_columns):
                sample_values = []
                for row_idx in range(min(5, len(data))):
                    if col_idx < len(data[row_idx]):
                        sample_values.append(f"'{data[row_idx][col_idx]}'")
                    else:
                        sample_values.append("'N/A'")
                print(f"  Column {col_idx}: {', '.join(sample_values)}")
        
        # Remove empty columns
        cleaned_data = self.remove_empty_columns(data, empty_columns)
        
        self.cleaned_data = cleaned_data
        self.removed_columns = sorted(empty_columns)
        
        print(f"Cleaned data has {len(cleaned_data)} rows")
        print(f"Removed {len(empty_columns)} empty columns")
        
        return cleaned_data, empty_columns
    
    def save_cleaned_csv(self, output_path: str = None):
        """Save the cleaned data to a new CSV file"""
        if not self.cleaned_data:
            print("No cleaned data to save. Run clean_csv() first.")
            return
        
        if output_path is None:
            # Generate output filename
            base_name = self.csv_file_path.rsplit('.', 1)[0]
            output_path = f"{base_name}_cleaned.csv"
        
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerows(self.cleaned_data)
            
            print(f"Cleaned CSV saved to: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"Error saving cleaned CSV: {e}")
            return None
    
    def print_summary(self):
        """Print a summary of the cleaning process"""
        print("\n" + "="*60)
        print("CSV CLEANING SUMMARY")
        print("="*60)
        print(f"Original file: {self.csv_file_path}")
        print(f"Original rows: {len(self.original_data)}")
        print(f"Cleaned rows: {len(self.cleaned_data)}")
        
        if self.removed_columns:
            print(f"Removed empty columns at positions: {self.removed_columns}")
        else:
            print("No columns were removed")
        
        # Show before/after structure
        if self.original_data and self.cleaned_data:
            print("\nBefore/After comparison:")
            print("Original structure:")
            for i, row in enumerate(self.original_data[:3]):  # Show first 3 rows
                print(f"  Row {i+1}: {len(row)} columns - {row}")
            
            print("Cleaned structure:")
            for i, row in enumerate(self.cleaned_data[:3]):  # Show first 3 rows
                print(f"  Row {i+1}: {len(row)} columns - {row}")
        
        print("="*60)

def main():
    """Main function to run the CSV cleaner"""
    
    if len(sys.argv) > 1:
        csv_file_path = sys.argv[1]
    else:
        csv_file_path = input("Enter the path to your CSV file: ").strip()
        
    # Remove quotes if present
    csv_file_path = csv_file_path.strip('"\'')
    
    # Create cleaner instance
    cleaner = CSVCleaner(csv_file_path)
    
    # Clean the CSV
    cleaned_data, removed_columns = cleaner.clean_csv()
    
    # Print summary
    cleaner.print_summary()
    
    # Automatically save cleaned CSV
    output_path = cleaner.save_cleaned_csv()
    
    if output_path:
        print(f"\nProcess completed successfully!")
        print(f"Cleaned file saved as: {output_path}")
    else:
        print("\nProcess completed but there was an error saving the file.")

if __name__ == "__main__":
    main()