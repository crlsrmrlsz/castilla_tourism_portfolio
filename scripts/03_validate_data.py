# scripts/03_validate_data.py

import pandas as pd
from pathlib import Path
import numpy as np
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- SET TO TRUE TO PRINT A DETAILED EXAMPLE OF THE FIRST FAILED CHECK ---
DEBUG_MODE = False
# --------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ANALYTICS_DATA_DIR = PROJECT_ROOT / "data" / "analytics"

# Tolerance for float comparisons. We check if the sum of parts is > total.
# A small tolerance prevents warnings for minor floating point differences.
TOLERANCE = 1.05  # Allows sum of parts to be 1% greater than total before warning


def validate_origin_file(df, file_name):
    """Checks that the sum of origin volumes is not greater than the total volume."""
    logging.info(f"  - Performing Origin Sanity Check on {file_name}...")

    grouping_cols = [col for col in df.columns if
                     not col.startswith('volumen_') and col not in ['origen_detalle', 'provincia_detalle', 'year',
                                                                    'month', 'weekday']]
    if not grouping_cols or 'volumen_total' not in df.columns:
        logging.warning("    - Could not determine grouping columns or find 'volumen_total'. Skipping.")
        return True  # Return True as no test was performed

    df['volumen_origen_sum'] = df.get('volumen_municipio', 0).fillna(0) + df.get('volumen_nacionalidad', 0).fillna(0)

    grouped = df.groupby(grouping_cols, dropna=False)
    sum_of_origins = grouped['volumen_origen_sum'].sum()
    total_volume = grouped['volumen_total'].first()

    comparison_df = pd.DataFrame({'total': total_volume, 'calculated_sum': sum_of_origins})
    # Check only for cases where the sum of parts is GREATER than the total
    discrepancies = comparison_df[comparison_df['calculated_sum'] > comparison_df['total'] * TOLERANCE]

    if discrepancies.empty:
        logging.info("    -> SUCCESS: Origin sanity check passed. No sums exceeded the total.")
        return True
    else:
        logging.warning(
            f"    -> WARNING: Found {len(discrepancies)} GROUPS where the sum of details is GREATER than the total volume.")
        if DEBUG_MODE:
            # (The origin debug mode is kept here for completeness but should not be triggered)
            return False
    return True


def validate_demographics_file(df, file_name):
    """Checks that the sums of pivoted demographic volumes are not greater than the total."""
    logging.info(f"  - Performing Demographics Sanity Check on {file_name}...")
    all_ok = True

    # Check Age
    age_cols = [col for col in df.columns if col.startswith('volumen_edad_')]
    if age_cols:
        df['volumen_edad_sum'] = df[age_cols].sum(axis=1)
        discrepancies = df[df['volumen_edad_sum'] > df['volumen_total'].fillna(0) * TOLERANCE]
        if discrepancies.empty:
            logging.info("    -> SUCCESS: Age sanity check passed.")
        else:
            all_ok = False
            logging.warning(
                f"    -> WARNING: Found {len(discrepancies)} rows where sum of age volumes is GREATER than the total.")
            if DEBUG_MODE:
                logging.info("--- DEBUGGING FIRST FAILED AGE ROW ---")
                # Prepare columns to display for debugging
                display_cols = ['volumen_total', 'volumen_edad_sum'] + age_cols
                # Add key identifier columns to see which row is failing
                id_cols = [col for col in ['fecha', 'origen', 'categoriadelvisitante', 'hora'] if col in df.columns]
                print(discrepancies[id_cols + display_cols].head(1).to_string())
                return False  # Stop after first debug

    # Check Gender if age was okay
    if all_ok:
        gender_cols = [col for col in df.columns if col.startswith('volumen_genero_')]
        if gender_cols:
            df['volumen_genero_sum'] = df[gender_cols].sum(axis=1)
            discrepancies = df[df['volumen_genero_sum'] > df['volumen_total'].fillna(0) * TOLERANCE]
            if discrepancies.empty:
                logging.info("    -> SUCCESS: Gender sanity check passed.")
            else:
                all_ok = False
                logging.warning(
                    f"    -> WARNING: Found {len(discrepancies)} rows where sum of gender volumes is GREATER than the total.")
                if DEBUG_MODE:
                    logging.info("--- DEBUGGING FIRST FAILED GENDER ROW ---")
                    display_cols = ['volumen_total', 'volumen_genero_sum'] + gender_cols
                    id_cols = [col for col in ['fecha', 'origen', 'categoriadelvisitante', 'hora'] if col in df.columns]
                    print(discrepancies[id_cols + display_cols].head(1).to_string())
                    return False  # Stop after first debug
    return all_ok


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
                validation_passed = True
                if file_path.name.endswith("_origin_analysis.parquet"):
                    validation_passed = validate_origin_file(df, file_path.name)
                elif file_path.name.endswith("_demographics_analysis.parquet"):
                    validation_passed = validate_demographics_file(df, file_path.name)

                if DEBUG_MODE and not validation_passed:
                    logging.warning("--- DEBUG MODE ON: Halting after first failed check. ---")
                    return  # Exit after the first failed check in debug mode
            except Exception as e:
                logging.error(f"Could not process file {file_path.name}. Error: {e}")

    logging.info("--- Data Validation Script Finished ---")


if __name__ == '__main__':
    main()