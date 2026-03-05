import csv
import os

def explore_csv_structure(filepath: str, num_lines: int = 5):
    """
    Read first few lines of CSV to understand structure.
    Memory-efficient: only loads requested lines.
    """
    print(f"Exploring file: {filepath}")
    print(f"File size: {os.path.getsize(filepath) / (1024**3):.2f} GB")
    print("-" * 80)
    
    with open(filepath, 'r', encoding='utf-8') as f:
        # Read header
        reader = csv.reader(f)
        header = next(reader)
        
        print(f"Number of columns: {len(header)}")
        print("\nColumn names:")
        for i, col in enumerate(header):
            print(f"  {i}: {col}")
        
        print("\n" + "-" * 80)
        print(f"First {num_lines} data rows:\n")
        
        # Read first few data rows
        for i, row in enumerate(reader):
            if i >= num_lines:
                break
            print(f"Row {i + 1}:")
            for col_name, value in zip(header, row):
                print(f"  {col_name}: {value}")
            print()

def count_lines_streaming(filepath: str, sample_every: int = 100000):
    """
    Count total lines in file without loading into memory.
    Prints progress every sample_every lines.
    """
    print(f"Counting lines in {filepath}...")
    count = 0
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            count += 1
            if count % sample_every == 0:
                print(f"  Processed {count:,} lines...")
    print(f"Total lines: {count:,}")
    return count


if __name__ == "__main__":
    # UPDATE THIS PATH to your actual CSV file location
    # Example: "/Users/jekaterinasergejeva/Desktop/Masters/Big Data/assignment1/aisdk-2024-01-15.csv"
    
    DATA_DIR = "/Users/jekaterinasergejeva/Desktop/Masters/Big Data/assignment1"
    
    # List available CSV files in directory
    print("Available CSV files in directory:")
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    for f in csv_files:
        filepath = os.path.join(DATA_DIR, f)
        size_gb = os.path.getsize(filepath) / (1024**3)
        print(f"  {f} - {size_gb:.2f} GB")
    
    print("\n" + "=" * 80 + "\n")
    
    # Explore first CSV file found (or specify manually)
    if csv_files:
        first_file = os.path.join(DATA_DIR, csv_files[0])
        explore_csv_structure(first_file, num_lines=5)
        
        # Uncomment below to count total lines (takes a few minutes for large files)
        # count_lines_streaming(first_file)
    else:
        print("No CSV files found. Please update DATA_DIR path.")