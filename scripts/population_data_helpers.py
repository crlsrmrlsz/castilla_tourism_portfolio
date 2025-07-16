import pandas as pd
import os
import re


def load_ine_community_population(file_paths, debug=False):
    """
    Loads and processes INE community-level population data from ECP_CLM files.
    This version now correctly extracts year and month from filenames.

    Args:
        file_paths (list): A list of paths to the INE ECP_CLM_*.csv files.
        debug (bool): If True, prints debugging information.

    Returns:
        pandas.DataFrame: A DataFrame with INE population data for Castilla-La Mancha.
    """
    if debug:
        print("--- Loading INE Community Population Data ---")

    all_data = []
    month_map = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
        'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }

    for file_path in file_paths:
        try:
            filename = os.path.basename(file_path)
            year_match = re.search(r'(\d{4})', filename)
            month_match = re.search(
                r'_(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)',
                filename, re.IGNORECASE)

            if not (year_match and month_match):
                print(f"Warning: Could not extract year/month from {filename}. Skipping.")
                continue

            year = int(year_match.group(1))
            month_name = month_match.group(1).lower()
            month = month_map[month_name]

            # Load data with multi-level header
            df = pd.read_csv(file_path, sep=';', header=[0, 1], index_col=0, thousands='.')

            # --- Robust Header Cleaning ---
            new_cols = []
            last_valid_top = ''
            for top, sub in df.columns:
                if 'Unnamed:' not in top:
                    last_valid_top = top.strip()
                new_cols.append((last_valid_top, sub.strip()))
            df.columns = pd.MultiIndex.from_tuples(new_cols)

            # --- Process Age Data ---
            df_age = df.drop(df.tail(1).index)  # Drop 'Total' row
            df_age.index = df_age.index.str.extract(r'(\d+)', expand=False).fillna(0).astype(int)

            bins = [-1, 17, 24, 34, 44, 54, 64, 150]
            labels = ['<18', '18-24', '25-34', '35-44', '45-54', '55-64', '65 o más']
            df_age['age_group'] = pd.cut(df_age.index, bins=bins, labels=labels, right=True)

            clm_cols = {
                'population_ine': ('CASTILLA-LA MANCHA', 'Total'),
                'hombres_ine': ('CASTILLA-LA MANCHA', 'Hombres'),
                'mujeres_ine': ('CASTILLA-LA MANCHA', 'Mujeres')
            }

            df_age_grouped = df_age.groupby('age_group')[list(clm_cols.values())].sum()
            df_age_grouped.columns = clm_cols.keys()
            df_age_grouped['fecha'] = pd.to_datetime(f'{year}-{month}-01')

            all_data.append(df_age_grouped.reset_index())

            if debug:
                print(f"Successfully processed {filename} for {year}-{month}")

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


def load_ine_city_population(file_paths, debug=False):
    """
    Loads and processes INE city-level annual population data, including gender.
    (This function remains unchanged as city data is annual)
    """
    if debug:
        print("--- Loading INE City Population Data (with Gender) ---")
    # ... (rest of the function is the same as before)
    all_city_data = []
    for file_path in file_paths:
        try:
            df