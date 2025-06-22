# scripts/02_create_analysis_datasets.py

import pandas as pd
from pathlib import Path
from collections import defaultdict
import logging
import re
import unicodedata

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
ANALYTICS_DATA_DIR = PROJECT_ROOT / "data" / "analytics"

# --- Constants ---
LOCATIONS_TO_PROCESS = [
    "Albacete Municipio", "Ciudad Real Municipio", "Cuenca Municipio",
    "Guadalajara Municipio", "Toledo Municipio", "CCAA Castilla-La Mancha"
]
COLUMNS_TO_DROP = ['diadelasemana', 'zonaobservacion']
ORIGIN_FILES = ['Municipio', 'Nacionalidad']
DEMOGRAPHIC_FILES = ['Edad', 'Genero']


def clean_dataframe(df):
    """Applies standard cleaning steps to a dataframe."""
    normalized_cols = [
        ''.join(c for c in unicodedata.normalize('NFD', col) if unicodedata.category(c) != 'Mn').lower()
        for col in df.columns
    ]
    df.columns = normalized_cols
    for col in COLUMNS_TO_DROP:
        if col in df.columns:
            df = df.drop(columns=[col])
    return df


def process_file_group(base_name: str, file_paths: list, output_dir: Path):
    """Creates two analysis files for a group: one for origin, one for demographics."""
    logging.info(f"  - Processing file group: '{base_name}'")

    base_file_path = next((p for p in file_paths if p.name == f"{base_name}.parquet"), None)

    if not base_file_path:
        logging.warning(f"    - Base file for group '{base_name}' not found. Skipping.")
        return

    try:
        base_df = pd.read_parquet(base_file_path)
        base_df = clean_dataframe(base_df)
        base_df = base_df.rename(columns={'volumen': 'volumen_total'})
    except Exception as e:
        logging.error(f"    - Could not read base file {base_file_path.name}. Error: {e}")
        return

    # --- PATH A: Generate the Origin Analysis File ---
    logging.info(f"    - Building Origin dataset for '{base_name}'...")
    origin_df = base_df.copy()
    for suffix in ORIGIN_FILES:
        detail_path = next((p for p in file_paths if p.stem.endswith(f"_{suffix}")), None)
        if not detail_path: continue

        try:
            detail_df = pd.read_parquet(detail_path)
            detail_df = clean_dataframe(detail_df)
            if 'pais' in detail_df.columns:
                detail_df = detail_df[detail_df['pais'].str.lower() != 'acumulado']
            if 'nombremunicipio' in detail_df.columns:
                detail_df = detail_df[detail_df['nombremunicipio'].str.lower() != 'acumulado']

            detail_df = detail_df.rename(columns={'volumen': f'volumen_{suffix.lower()}'})
            join_keys = [col for col in origin_df.columns if col in detail_df.columns and col != 'volumen_total']
            origin_df = pd.merge(origin_df, detail_df, on=join_keys, how='left')
        except Exception as e:
            logging.error(f"      - Failed to merge {detail_path.name}. Error: {e}")

    origin_df['origen_detalle'] = origin_df['pais'] if 'pais' in origin_df.columns else pd.Series(dtype='object')
    if 'nombremunicipio' in origin_df.columns:
        origin_df['origen_detalle'] = origin_df['origen_detalle'].fillna(origin_df['nombremunicipio'])
    origin_df['provincia_detalle'] = origin_df[
        'nombreprovincia'] if 'nombreprovincia' in origin_df.columns else pd.Series(dtype='object')

    columns_to_drop_after_unification = ['pais', 'nombremunicipio', 'nombreprovincia']
    origin_df = origin_df.drop(columns=[col for col in columns_to_drop_after_unification if col in origin_df.columns])

    date_col = next((col for col in origin_df.columns if 'fecha' in col), None)
    if date_col:
        origin_df[date_col] = pd.to_datetime(origin_df[date_col])
        origin_df['year'] = origin_df[date_col].dt.year
        origin_df['month'] = origin_df[date_col].dt.month
        origin_df['weekday'] = origin_df[date_col].dt.day_name()

    output_path_origin = output_dir / f"{base_name}_origin_analysis.parquet"
    origin_df.to_parquet(output_path_origin, index=False)
    logging.info(f"    -> Saved Origin dataset: {output_path_origin.name}")

    # --- PATH B: Generate the Demographics Analysis File ---
    logging.info(f"    - Building Demographics dataset for '{base_name}'...")
    demographics_df = base_df.copy()
    for suffix in DEMOGRAPHIC_FILES:
        detail_path = next((p for p in file_paths if p.stem.endswith(f"_{suffix}")), None)
        if not detail_path: continue

        try:
            detail_df = pd.read_parquet(detail_path)
            detail_df = clean_dataframe(detail_df)

            pivot_col = suffix.lower()
            if pivot_col not in detail_df.columns: continue

            pivot_keys = [k for k in detail_df.columns if k not in [pivot_col, 'volumen']]
            pivoted = detail_df.pivot_table(index=pivot_keys, columns=pivot_col, values='volumen').reset_index()
            pivoted.columns.name = None
            pivoted = pivoted.rename(
                columns={str(col): f'volumen_{pivot_col}_{str(col).lower()}' for col in pivoted.columns if
                         col not in pivot_keys})

            join_keys = [col for col in demographics_df.columns if col in pivoted.columns and col != 'volumen_total']
            demographics_df = pd.merge(demographics_df, pivoted, on=join_keys, how='left')
        except Exception as e:
            logging.error(f"      - Failed to pivot/merge {detail_path.name}. Error: {e}")

    # --- NEW: Filter out 'Extranjero' rows as they have no demographic data ---
    if 'origen' in demographics_df.columns:
        logging.info("    - Filtering out 'Extranjero' rows from demographics dataset.")
        rows_to_keep = demographics_df['origen'].isin(['Local', 'NoLocal'])
        demographics_df = demographics_df[rows_to_keep].copy()
    # --- END OF NEW BLOCK ---

    date_col_demo = next((col for col in demographics_df.columns if 'fecha'