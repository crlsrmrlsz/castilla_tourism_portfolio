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

# List of locations (cities and community) to process.
# The script will look for these specific folder names.
LOCATIONS_TO_PROCESS = [
    "Albacete Municipio",
    "Ciudad Real Municipio",
    "Cuenca Municipio",
    "Guadalajara Municipio",
    "Toledo Municipio",
    "CCAA Castilla-La Mancha"  # Added the community-level data folder
]


def get_file_type_from_name(filename):
    """
    Extracts a generic file type from a filename by removing the variable
    _Lx_ part and the city/community-specific codes.
    Example: 'Diario_2H_L1_Albac0.csv' -> 'Diario_2H.csv'
    Example: 'Diario_2H_L6_Ciuda3.csv' -> 'Diario_2H.csv'
    Example: 'Diario_2H_Municipio_L1_Albac0.csv' -> 'Diario_2H_Municipio.csv'
    """
    # Remove the _Lx_ part and the city/community-specific code (e.g., _L1_Albac0)
    # Broadening the city/community-specific code part to include alphanumeric and underscore characters
    # to better handle variations like 'Toledod9' and community-level file naming conventions.
    cleaned_filename = re.sub(r'_L\d+_[A-Za-z0-9_]+', '', filename)
    return cleaned_filename


def process_location_data(location_name, source_dir, processed_dir):
    """
    Processes data for a single location (city or community): reads CSVs, merges them by type, and saves as Parquet.
    """
    print(f"Processing data for {location_name}...")
    location_output_dir = processed_dir / location_name
    location_output_dir.mkdir(parents=True, exist_ok=True)

    # Use defaultdict to group files by their generic type (e.g., 'Diario_2H.csv')
    files_by_type = defaultdict(list)

    # Walk through all bimonthly folders for the current location
    for bimonthly_folder in source_dir.iterdir():
        if bimonthly_folder.is_dir():
            location_folder_path = bimonthly_folder / location_name
            if location_folder_path.is_dir():
                for file_path in location_folder_path.iterdir():
                    if file_path.suffix == '.csv':
                        file_type = get_file_type_from_name(file_path.name)
                        files_by_type[file_type].append(file_path)

    for file_type, file_paths in files_by_type.items():
        list_of_dfs = []
        print(f"  - Merging files for type: {file_type} ({len(file_paths)} files)")

        for file_path in file_paths:
            try:
                # Read CSV, typically semicolon-separated with comma as decimal in Spanish data
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

            # --- Validation Check 1: Check if merged_df is empty ---
            if merged_df.empty:
                print(f"    - WARNING: Merged DataFrame for {file_type} is empty. Skipping save.")
                continue
            else:
                print(f"    - Successfully merged {len(merged_df)} rows for {file_type}.")

            # Define the output path for the Parquet file
            output_filename = Path(file_type).stem + ".parquet"
            output_path = location_output_dir / output_filename

            # Save the merged DataFrame to a Parquet file
            merged_df.to_parquet(output_path, engine='pyarrow')
            print(f"    -> Saved {output_path} with {len(merged_df)} rows.")

            # --- Validation Check 2: Verify saved file exists and is not empty ---
            if output_path.exists() and output_path.stat().st_size > 0:
                print(f"    -> Validation: {output_path} successfully created and is not empty.")
            else:
                print(f"    -> Validation: WARNING! {output_path} was not created or is empty.")


        except Exception as e:
            print(f"    - Could not merge or save {file_type}. Error: {e}")


def main():
    """Main function to run the data processing workflow."""
    if not SOURCE_DATA_DIR.is_dir():
        print(f"Error: Source directory not found at '{SOURCE_DATA_DIR}'")
        print("Please create it and place your bimonthly data folders inside.")
        return

    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Source data directory: {SOURCE_DATA_DIR}")
    print(f"Processed data will be saved to: {PROCESSED_DATA_DIR}")
    print("-" * 30)

    for location_name in LOCATIONS_TO_PROCESS:
        process_location_data(location_name, SOURCE_DATA_DIR, PROCESSED_DATA_DIR)
        print("-" * 30)

    print("Data processing complete for all specified locations.")


if __name__ == "__main__":
    main()