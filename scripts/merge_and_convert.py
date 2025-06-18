# scripts/merge_and_convert.py

import os
import pandas as pd
from pathlib import Path
import re
from collections import defaultdict

# --- Configuration: UPDATE THESE PATHS ---
# Use Path for OS compatibility (Windows, macOS, Linux)
# This script assumes it is in a 'scripts' folder, so '../' goes up to the project root.
PROJECT_ROOT = Path(__file__).parent.parent
SOURCE_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "source_folders"  # <-- IMPORTANT: Create a folder named 'source_folders' inside 'data/raw' and put all your bimonthly folders (01.Comportamiento..., etc.) there.
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
    _Lx_ part and the city-specific codes.
    Example: 'Diario_2H_L1_Albac0.csv' -> 'Diario_2H.csv'
    Example: 'Diario_2H_L6_Ciuda3.csv' -> 'Diario_2H.csv'
    This helps group all 'Diario_2H' files together, regardless of
    the bimonthly period (L1, L6) or city.
    """
    # This regex looks for the pattern _L[number]_[CityCode][number] and removes it.
    # e.g., it will find and remove '_L1_Albac0' or '_L6_Ciuda3'.
    # The \d+ matches one or more digits.
    pattern = r'_L\d+_(Albac|Ciuda|Cuenc|Guada|Tole)[0-9]*'
    base_name = re.sub(pattern, '', filename, flags=re.IGNORECASE)
    return base_name


def process_city_data(city_name):
    """
    Finds all files for a given city, merges them by type, and saves them as Parquet files.
    """
    print(f"\n--- Processing city: {city_name} ---")

    # A dictionary to hold lists of file paths, keyed by their type
    # e.g., {'Diario_2H.csv': [path1, path2, ...], 'Nocturno.csv': [path_a, path_b, ...]}
    files_to_merge = defaultdict(list)

    # Walk through all the bimonthly source folders to find files for the current city
    city_folder_path_pattern = f"**/{city_name}/*.csv"

    print(f"Scanning for files in {SOURCE_DATA_DIR} matching pattern...")

    # Use rglob to recursively find all CSV files for the city
    for file_path in SOURCE_DATA_DIR.rglob(city_folder_path_pattern):
        file_type = get_file_type_from_name(file_path.name)
        files_to_merge[file_type].append(file_path)

    if not files_to_merge:
        print(f"Warning: No files found for city '{city_name}'. Check folder names and paths.")
        return

    # Create the output directory for the city if it doesn't exist
    city_output_dir = PROCESSED_DATA_DIR / city_name.replace(" Municipio", "")
    city_output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output will be saved in: {city_output_dir}")

    # Now, merge the files for each type
    for file_type, file_list in files_to_merge.items():
        print(f"  Merging {len(file_list)} files for type: {file_type}")

        list_of_dfs = []
        for file_path in file_list:
            try:
                # Read each CSV file into a DataFrame
                df = pd.read_csv(file_path, sep=';', decimal=',')  # Adjusted for typical Spanish CSV format
                list_of_dfs.append(df)
            except Exception as e:
                print(f"    - Could not read file {file_path}. Error: {e}")
                continue

        if not list_of_dfs:
            print(f"    - No dataframes to merge for {file_type}. Skipping.")
            continue

        # Concatenate all DataFrames for this type into one
        try:
            merged_df = pd.concat(list_of_dfs, ignore_index=True)

            # Define the output path for the Parquet file
            output_filename = Path(file_type).stem + ".parquet"
            output_path = city_output_dir / output_filename

            # Save the merged DataFrame to a Parquet file
            merged_df.to_parquet(output_path, engine='pyarrow')
            print(f"    -> Saved {output_path} with {len(merged_df)} rows.")

        except Exception as e:
            print(f"    - Could not merge or save {file_type}. Error: {e}")


def main():
    """Main function to run the data processing workflow."""
    if not SOURCE_DATA_DIR.is_dir():
        print(f"Error: Source directory not found at '{SOURCE_DATA_DIR}'")
        print("Please create it and place your bimonthly data folders inside.")
        return

    for city in CITY_NAMES:
        process_city_data(city)

    print("\n--- Data merging process completed! ---")


if __name__ == "__main__":
    main()