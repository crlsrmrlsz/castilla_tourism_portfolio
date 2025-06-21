# scripts/03_validate_data.py

import pandas as pd
from pathlib import Path
import numpy as np
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ANALYTICS_DATA_DIR = PROJECT_ROOT / "data" / "analytics"

# Tolerance for float comparisons. We check if the sum of parts is > total.
# A small tolerance prevents warnings for minor floating point differences.
TOLERANCE = 1.01  # Allows sum of parts to be 1% greater than total before warning


def validate_origin_file(df, file_name):
    """Checks that the sum of origin volumes is not greater than the total volume."""
    logging.info(f"  - Performing Origin Sanity Check on {file_name}...")

    grouping_cols = [col for col in df.columns if
                     not col.startswith('volumen_') and col not in ['origen_detalle', 'provincia_detalle', 'pais',
                                                                    'nombremunicipio', 'nombreprovincia', 'year',
                                                                    'month', 'weekday']]
    if not grouping_cols or 'volumen_total' not in df.columns:
        logging.warning("    - Could not determine grouping columns or find 'volumen_total'. Skipping.")
        return

    df['volumen_origen_sum'] = df.get('volumen_municipio', 0).fillna(0) + df.get('volumen_nacionalidad', 0).fillna(0)

    grouped = df.groupby(grouping_cols, dropna=False)
    sum_of_origins = grouped['volumen_origen_sum'].sum()
    total_volume = grouped['volumen_total'].first()

    # Check only for cases where the sum of parts is GREATER than the total
    discrepancies = sum_of_origins > total_volume * TOLERANCE

    if not discrepancies.any():
        logging.info("    -> SUCCESS: Origin sanity check passed. No sums exceeded the total.")
    else:
        logging.warning(
            f"    -> WARNING: Found {discrepancies.sum()} GROUPS where the sum of details is GREATER than the total volume.")


def validate_demographics_file(df, file_name):
    """Checks that the sums of pivoted demographic volumes are not greater than the total."""
    logging.info(f"  - Performing Demographics Sanity Check on {file_name}...")

    # Check Age
    age_cols = [col for col in df.columns if col.startswith('volumen_edad_')]
    if age_cols:
        df['volumen_edad_sum'] = df[age_cols].sum(axis=1)
        discrepancies = df[df['volumen_edad_sum'] > df['volumen_total'].fillna(0) * TOLERANCE]
        if discrepancies.empty:
            logging.info("    -> SUCCESS: Age sanity check passed.")
        else:
            logging.warning(
                f"    -> WARNING: Found {len(discrepancies)} rows where sum of age volumes is GREATER than the total.")

    # Check Gender
    gender_cols = [col for col in df.columns if col.startswith('volumen_genero_')]
    if gender_cols:
        df['volumen_genero_sum'] = df[gender_cols].sum(axis=1)
        discrepancies = df[df['volumen_genero_sum'] > df['volumen_total'].fillna(0) * TOLERANCE]
        if discrepancies.empty:
            logging.info("    -> SUCCESS: Gender sanity check passed.")
        else:
            logging.warning(
                f"    -> WARNING: Found {len(discrepancies)} rows where sum of gender volumes is GREATER than the total.")


def main():
    """Finds all analysis files and runs the appropriate validation checks."""
    logging.info("--- Starting Data Validation Script ---")
    if not ANALYTICS_DATA_DIR.is_dir():
        logging.error(f"Analytics directory not found at {ANALYTICS_DATA_DIR}")
        return

    for location_path in sorted(list(ANALYTICS_DATA_DIR.iterdir())):
        if not location_path.is_dir(): continue

        logging.info(f"--- Validating Location: {location_path.name} ---")
        for file_path in sorted(list(location_path.glob('*_analysis.parquet'))):
            try:
                df = pd.read_parquet(file_path)
                if file_path.name.endswith("_origin_analysis.parquet"):
                    validate_origin_file(df, file_path.name)
                elif file_path.name.endswith("_demographics_analysis.parquet"):
                    validate_demographics_file(df, file_path.name)
            except Exception as e:
                logging.error(f"Could not process file {file_path.name}. Error: {e}")

    logging.info("--- Data Validation Script Finished ---")


if __name__ == '__main__':
    main()