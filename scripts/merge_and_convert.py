# scripts/merge_and_convert.py

import os
import pandas as pd
from pathlib import Path
from collections import defaultdict

# --- Configuration: UPDATE THESE PATHS ---
# Use Path for OS compatibility (Windows, macOS, Linux)
# This script assumes it is in a 'scripts' folder, so '../' goes up to the project root.
PROJECT_ROOT = Path(__file__).parent.parent
# IMPORTANT: Create a folder named 'source_folders' inside 'data/raw'
# and put all your bimonthly folders (01.Comportamiento..., etc.) there.
SOURCE_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "source_folders"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

# List of city folder names as they appear in the directories
# The script will look for these specific folder names.
CITY_NAMES = [
    "Albacete Municipio",
    "Ciudad Real Municipio",
    "Cuenca Municipio",
    "Guadalajara Municipio",
    "Toledo Municipio"
]

def get_file_type_from_name(filename):
    """
    Extracts a generic file type from a filename by removing the variable
    _Lx_ part and the city-specific codes. This version is more robust.
    Example: 'Diario_2H_Municipio_L1_Toled9.csv' -> 'Diario_2H_Municipio.csv'
    Example: 'Diario_2H_L6_Ciuda3.csv' -> 'Diario_2H.csv'
    """
    # Splitting the filename by '_L' is a more reliable way to separate the
    # base name from the variable parts (_L*, city code, etc.).
    base_name = filename.split('_L')[0]
    return f"{base_name}.csv"

def process_city_data():
    """
    Main function to discover, merge, and convert data for all cities.
    """
    if not SOURCE_DATA_DIR.is_dir():
        print(f"Error: Source directory not found at '{SOURCE_DATA_DIR}'")
        print("Please create it and place your bimonthly data folders inside.")
        return

    print("Starting data processing workflow...")
    print(f"Source data directory: {SOURCE_DATA_DIR}")
    print(f"Processed data directory: {PROCESSED_DATA_DIR}")
    print("-" * 50)

    # Dictionary to hold all file paths, grouped by city and then by file type.
    # e.g., {'Albacete Municipio': {'Diario_2H.csv': [path1, path2, ...]}}
    all_files = defaultdict(lambda: defaultdict(list))

    # --- Step 1: Discover all files and group them ---
    print("Step 1: Discovering and grouping all CSV files...")
    bimonthly_folders = [d for d in SOURCE_DATA_DIR.iterdir() if d.is_dir()]
    if not bimonthly_folders:
        print(f"Warning: No bimonthly data folders found in '{SOURCE_DATA_DIR}'.")
        return

    for bimonth_dir in bimonthly_folders:
        for city_name in CITY_NAMES:
            city_path = bimonth_dir / city_name
            if city_path.is_dir():
                for csv_file in city_path.glob("*.csv"):
                    file_type = get_file_type_from_name(csv_file.name)
                    all_files[city_name][file_type].append(csv_file)
    print("File discovery complete.\n")


    # --- Step 2: Process files for each city ---
    print("Step 2: Merging files and converting to Parquet for each city...")
    for city_name, file_types in all_files.items():
        city_output_dir = PROCESSED_DATA_DIR / city_name
        city_output_dir.mkdir(parents=True, exist_ok=True)
        print(f"\nProcessing city: {city_name}")
        print(f"Output directory: {city_output_dir}")

        for file_type, file_paths in file_types.items():
            print(f"  - Merging {len(file_paths)} files for type: {file_type}")
            list_of_dfs = []
            total_source_rows = 0

            # Read each CSV and append its DataFrame to the list
            for file_path in file_paths:
                try:
                    df = pd.read_csv(file_path, sep=';', decimal=',')
                    list_of_dfs.append(df)
                    total_source_rows += len(df)
                except Exception as e:
                    print(f"    - Could not read file {file_path}. Error: {e}")
                    continue

            if not list_of_dfs:
                print(f"    - No dataframes to merge for {file_type}. Skipping.")
                continue

            # Concatenate all DataFrames for this type into one
            try:
                merged_df = pd.concat(list_of_dfs, ignore_index=True)
                final_merged_rows = len(merged_df)

                # --- Validation Check ---
                print(f"    - Validation: Source rows = {total_source_rows}, Merged rows = {final_merged_rows}")
                if total_source_rows == final_merged_rows:
                    print("    - Row count validation: SUCCESS")
                else:
                    print(f"    - Row count validation: FAILED. Expected {total_source_rows} rows, but got {final_merged_rows}.")


                # Define the output path for the Parquet file
                output_filename = Path(file_type).stem + ".parquet"
                output_path = city_output_dir / output_filename

                # Save the merged DataFrame to a Parquet file
                merged_df.to_parquet(output_path, engine='pyarrow')
                print(f"    -> Saved {output_path} with {final_merged_rows} rows.")

            except Exception as e:
                print(f"    - Could not merge or save {file_type}. Error: {e}")

    print("-" * 50)
    print("Data processing workflow finished.")


if __name__ == "__main__":
    process_city_data()