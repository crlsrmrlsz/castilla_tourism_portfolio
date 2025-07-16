import pandas as pd
import os
import re


def load_ine_community_population(file_paths, debug=False):
    """
    Loads and processes INE community-level TOTAL population data from ECP_CLM files.
    This version now correctly handles whitespace in the index labels.
    """
    if debug:
        print("--- Loading INE Community Population Data (Totals Only) ---")

    all_data = []
    month_map = {
        'enero': 1, 'julio': 7, 'octubre': 10
    }

    for file_path in file_paths:
        try:
            filename = os.path.basename(file_path)
            year_match = re.search(r'(\d{4})', filename)
            month_match = re.search(r'_(enero|julio|octubre)', filename, re.IGNORECASE)

            if not (year_match and month_match):
                continue

            year = int(year_match.group(1))
            month = month_map[month_match.group(1).lower()]

            df = pd.read_csv(file_path, sep=';', header=[0, 1], index_col=0, thousands='.')

            # --- FIX: STRIP WHITESPACE FROM THE INDEX ---
            df.index = df.index.str.strip()
            # -------------------------------------------

            # --- Robust Header Cleaning ---
            new_cols = []
            last_valid_top = ''
            for top, sub in df.columns:
                if 'Unnamed:' not in top:
                    last_valid_top = top.strip()
                new_cols.append((last_valid_top, sub.strip()))
            df.columns = pd.MultiIndex.from_tuples(new_cols)

            # This line will now work correctly because ' Total' has become 'Total'
            total_population = df.loc['Total', ('CASTILLA-LA MANCHA', 'Total')]

            record = {
                'fecha': pd.to_datetime(f'{year}-{month}-01'),
                'population_ine': total_population
            }
            all_data.append(record)

            if debug:
                print(f"Successfully processed {filename} for {year}-{month}. Total INE Pop: {total_population}")

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    if not all_data:
        return pd.DataFrame()

    return pd.DataFrame(all_data)


def load_ine_city_population(file_paths, debug=False):
    """
    Loads and processes INE city-level TOTAL annual population data.
    This version correctly handles multi-word city names.
    """
    if debug:
        print("--- Loading INE City Population Data (Totals Only) ---")

    all_city_data = []
    for file_path in file_paths:
        try:
            df = pd.read_csv(file_path, sep=';', thousands='.')
            # Filter for rows where 'Sexo' is 'Total'
            df_total = df[df['Sexo'] == 'Total'].copy()

            # --- THIS IS THE CORRECTED LINE ---
            # It now takes all words after the first one, correctly handling multi-word names.
            df_total['municipio'] = df_total['Municipios'].str.split(' ').str[1:].str.join(' ')

            df_total.rename(columns={'Periodo': 'year', 'Total': 'population_ine'}, inplace=True)
            all_city_data.append(df_total[['year', 'municipio', 'population_ine']])
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    if not all_city_data:
        return pd.DataFrame()

    final_df = pd.concat(all_city_data, ignore_index=True)

    if debug:
        print("\n--- Final Combined INE City DataFrame ---")
        print(final_df.head())

    return final_df


def load_mobile_population(file_path, debug=False):
    """
    Loads and processes DAILY mobile population data, taking the first Monday
    of each month as a snapshot.
    (This function remains unchanged)
    """
    if debug:
        print(f"--- Loading DAILY Mobile Population Data from: {file_path} ---")

    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"Error loading mobile data: {e}")
        return pd.DataFrame()

    df['fecha'] = pd.to_datetime(df['fecha'])
    df['year_month'] = df['fecha'].dt.to_period('M')

    mondays = df[df['fecha'].dt.dayofweek == 0].copy()

    first_monday_dates = mondays.groupby('year_month')['fecha'].min()

    df_snapshots = df[df['fecha'].isin(first_monday_dates)].copy()

    if debug:
        print(f"Found and filtered for {len(df_snapshots)} monthly snapshots (first Monday of each month).")

    population_categories = ['Residente', 'Habitualmente presente']
    df_pop = df_snapshots[df_snapshots['categoriadelvisitante'].isin(population_categories)].copy()

    df_agg = df_pop.groupby(['fecha', 'categoriadelvisitante'])['volumen_total'].sum().reset_index()

    df_pivot = df_agg.pivot_table(
        index='fecha',
        columns='categoriadelvisitante',
        values='volumen_total',
        fill_value=0
    ).reset_index()

    df_pivot.rename_axis(columns=None, inplace=True)

    if debug:
        print("\n--- Final Mobile Population DataFrame (Pivoted Snapshots) ---")
        print(df_pivot.head())

    return df_pivot