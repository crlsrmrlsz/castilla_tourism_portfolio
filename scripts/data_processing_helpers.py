import pandas as pd
import os


def load_ine_data(file_path, debug=False):
    """
    Loads and processes the INE hotel occupancy data with corrected logic.

    Args:
        file_path (str): The path to the INE CSV file.
        debug (bool): If True, prints debugging information.

    Returns:
        pandas.DataFrame: A DataFrame with processed INE data.
    """
    if debug:
        print(f"--- Loading INE Data from: {file_path} ---")

    try:
        df = pd.read_csv(
            file_path,
            encoding='utf-8',
            thousands='.',
            delimiter=';'
        )
        if debug:
            print("Initial INE data loaded successfully.")
    except Exception as e:
        print(f"Error loading INE data: {e}")
        return pd.DataFrame()

    # Rename columns for easier access
    df.rename(columns={
        'Viajeros y pernoctaciones': 'metric',
        'Residencia: Nivel 2': 'residencia', # Simplified: this is our main residency column now
        'Periodo': 'periodo',
        'Total': 'total'
    }, inplace=True)

    if debug:
        print("\n--- Columns renamed ---")
        print("Relevant INE DataFrame columns:", df[['metric', 'residencia', 'periodo', 'total']].columns.tolist())

    # 1. Filter for 'Viajeros' (travelers) metric
    df_viajeros = df[df['metric'] == 'Viajero'].copy()

    # 2. Filter directly for the required residency categories. This is the corrected logic.
    residencies_to_keep = ['Residentes en España', 'Residentes en el Extranjero']
    df_filtered = df_viajeros[df_viajeros['residencia'].isin(residencies_to_keep)].copy()

    if debug:
        print("\n--- Filtering for 'Viajeros' and specific residencies ---")
        print(f"Data shape after filtering: {df_filtered.shape}")
        print("Residency categories found:", df_filtered['residencia'].unique())

    # 3. Convert 'periodo' to datetime
    df_filtered['fecha'] = pd.to_datetime(df_filtered['periodo'].str.replace('M', ''), format='%Y%m')

    # 4. Create the final, clean DataFrame
    df_final = df_filtered[['fecha', 'residencia', 'total']].copy()
    df_final.rename(columns={'total': 'viajeros_ine'}, inplace=True)

    if debug:
        print("\n--- Final INE DataFrame ---")
        print(df_final.head())
        print("\nData types:")
        df_final.info()

    return df_final

def load_mobile_data(file_path, debug=False):
    """
    Loads and processes the mobile phone (Kido Dynamics) data.

    Args:
        file_path (str): The path to the Parquet file.
        debug (bool): If True, prints debugging information.

    Returns:
        pandas.DataFrame: A DataFrame with processed mobile data.
    """
    if debug:
        print(f"--- Loading Mobile Data from: {file_path} ---")

    try:
        df = pd.read_parquet(file_path)
        if debug:
            print("Mobile data loaded successfully.")
            print("Initial Mobile DataFrame columns:", df.columns.tolist())
    except Exception as e:
        print(f"Error loading mobile data: {e}")
        return pd.DataFrame()

    # Filter for the relevant visitor categories
    categories_to_include = ['Turista', 'Habitualmente presente']
    df_filtered = df[df['categoriadelvisitante'].isin(categories_to_include)].copy()

    if debug:
        print("\n--- Filtering for 'Turista' and 'Habitualmente presente' ---")
        print(f"Data shape after filtering: {df_filtered.shape}")

    # Map mobile data origin to INE residency categories
    def map_residencia(origen):
        if origen == 'Extranjero':
            return 'Residentes en el Extranjero'
        else:  # 'Local' and 'NoLocal'
            return 'Residentes en España'

    df_filtered['residencia'] = df_filtered['origen'].apply(map_residencia)

    # Convert 'mes' to datetime
    df_filtered['fecha'] = pd.to_datetime(df_filtered['mes'], format='%Y%m')

    # Pivot the data to have categories as columns
    df_pivot = df_filtered.pivot_table(
        index=['fecha', 'residencia'],
        columns='categoriadelvisitante',
        values='volumen_total',
        fill_value=0
    ).reset_index()

    # Rename columns for clarity
    df_pivot.rename(columns={
        'Turista': 'turistas_mobile',
        'Habitualmente presente': 'frecuentes_mobile'
    }, inplace=True)

    if debug:
        print("\n--- Final Mobile DataFrame (Pivoted) ---")
        print(df_pivot.head())
        print("\nData types:")
        print(df_pivot.info())

    return df_pivot


def merge_and_prepare_data(df_ine, df_mobile, debug=False):
    """
    Merges INE and mobile data and prepares it for plotting with the correct extrapolation factor.

    Args:
        df_ine (pandas.DataFrame): The processed INE data.
        df_mobile (pandas.DataFrame): The processed mobile data.
        debug (bool): If True, prints debugging information.

    Returns:
        pandas.DataFrame: A merged DataFrame ready for visualization.
    """
    if debug:
        print("\n--- Merging INE and Mobile Data ---")

    # Merge the two dataframes on 'fecha' and 'residencia'
    df_merged = pd.merge(df_ine, df_mobile, on=['fecha', 'residencia'], how='inner')

    # Create total mobile visitors column
    df_merged['total_mobile'] = df_merged['turistas_mobile'] + df_merged['frecuentes_mobile']

    # Apply the CORRECT extrapolation factor.
    # Hotel stays are ~65% of total stays. (from INE FRONTUR data of Spain)
    # So, we extrapolate INE data by dividing by 0.65.
    extrapolation_factor = 1 / 0.65
    df_merged['viajeros_ine_extrapolado'] = df_merged['viajeros_ine'] * extrapolation_factor

    if debug:
        print("\n--- Merged and Finalized DataFrame ---")
        print(f"Data shape after merging: {df_merged.shape}")
        print(f"Using extrapolation factor: {extrapolation_factor:.4f} (1 / 0.65)")
        print(df_merged.head())
        print("\nData types:")
        df_merged.info()

    return df_merged