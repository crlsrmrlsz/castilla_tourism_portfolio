# scripts/01_merge_and_convert.py

import pandas as pd
from pathlib import Path
import re
from collections import defaultdict
import logging

# --- Configuration ---
# Set up professional logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Use Path for OS compatibility
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# --- THIS LINE HAS BEEN CORRECTED ---
SOURCE_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "source_folders"
# ------------------------------------
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

LOCATIONS_TO_PROCESS = [
    "Albacete Municipio", "Ciudad Real Municipio", "Cuenca Municipio",
    "Guadalajara Municipio", "Toledo Municipio", "CCAA Castilla-La Mancha"
]

def get_file_type_from_name(filename):
    """Extracts a generic file type from a filename."""
    cleaned_filename = re.sub(r'_L\d+_[A-Za-z0-9_]+\.csv', '.csv', filename)
    return cleaned_filename

def process_location_data(location_name, source_dir, processed_dir):
    """Processes data for a single location: reads, merges by type, and saves as Parquet."""
    logging.info(f"Processing data for: {location_name}...")
    location_output_dir = processed_dir / location_name
    location_output_dir.mkdir(parents=True, exist_ok=True)

    files_by_type = defaultdict(list)
    for bimonthly_folder in source_dir.iterdir():
        if bimonthly_folder.is_dir():
            location_folder_path = bimonthly_folder / location_name
            if location_folder_path.is_dir():
                for file_path in location_folder_path.glob('*.csv'):
                    file_type = get_file_type_from_name(file_path.name)
                    files_by_type[file_type].append(file_path)

    if not files_by_type:
        logging.info(f"  - No CSV files found for {location_name}. Skipping.")
        return

    for file_type, file_paths in files_by_type.items():
        list_of_dfs = []
        logging.info(f"  - Merging {len(file_paths)} files for type: {file_type}")

        for file_path in file_paths:
            try:
                df = pd.read_csv(file_path, sep=';', decimal=',')
                list_of_dfs.append(df)
            except Exception as e:
                logging.warning(f"    - Could not read file {file_path}. Error: {e}")
                continue

        if not list_of_dfs:
            logging.warning(f"    - No dataframes were successfully read for {file_type}. Skipping merge.")
            continue

        try:
            merged_df = pd.concat(list_of_dfs, ignore_index=True)
            if merged_df.empty:
                logging.warning(f"    - Merged DataFrame for {file_type} is empty. Skipping save.")
                continue

            output_filename = Path(file_type).stem + ".parquet"
            output_path = location_output_dir / output_filename
            merged_df.to_parquet(output_path, engine='pyarrow')
            logging.info(f"    -> Saved {output_path} with {len(merged_df)} rows.")

            if output_path.exists() and output_path.stat().st_size > 0:
                logging.info(f"    -> Validation successful: File exists and is not empty.")
            else:
                logging.warning(f"    -> VALIDATION FAILED: File was not created or is empty.")
        except Exception as e:
            logging.error(f"    - Could not merge or save {file_type}. Error: {e}")

def main():
    """Main function to run the data processing workflow."""
    if not SOURCE_DATA_DIR.is_dir():
        logging.error(f"Source directory not found at '{SOURCE_DATA_DIR}'")
        logging.error("Please ensure your raw data folders are located there.")
        return

    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    logging.info(f"Source data directory: {SOURCE_DATA_DIR}")
    logging.info(f"Processed data will be saved to: {PROCESSED_DATA_DIR}")
    logging.info("-" * 50)

    for location in LOCATIONS_TO_PROCESS:
        process_location_data(location, SOURCE_DATA_DIR, PROCESSED_DATA_DIR)
        logging.info("-" * 50)

    logging.info("Data processing complete for all specified locations.")

if __name__ == "__main__":
    main()