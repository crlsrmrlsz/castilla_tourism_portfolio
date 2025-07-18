# scripts/tourist_data_helpers.py

import pandas as pd
import os


def load_ine_frontur_data(file_path, debug=False):
    """
    Loads and processes the annual INE FRONTUR data for international tourists.

    Args:
        file_path (str): The path to the INE FRONTUR CSV file.
        debug (bool): If True, prints debugging information.

    Returns:
        pandas.DataFrame: A DataFrame with processed INE data (Year, Tourists).
    """
    if debug:
        print(f"--- Loading INE FRONTUR Data from: {file_path} ---")

    try:
        df = pd.read_csv(
            file_path,
            delimiter=';',
            thousands='.',
            encoding='utf-8'
        )
        if debug:
            print("Initial INE data loaded successfully.")
    except Exception as e:
        print(f"Error loading INE FRONTUR data: {e}")
        return pd.DataFrame()

    # Clean up and rename columns for consistency
    df.rename(columns={
        'Periodo': 'year',
        'Total': 'tourists_ine'
    }, inplace=True)

    # Ensure 'tourists_ine' is a numeric type
    df['tourists_ine'] = pd.to_numeric(df['tourists_ine'], errors='coerce')

    # Filter for the relevant region and columns
    df_clm = df[df['Comunidades autónomas'].str.contains("Castilla - La Mancha", na=False)].copy()

    final_df = df_clm[['year', 'tourists_ine']].reset_index(drop=True)

    if debug:
        print("\n--- Processed INE FRONTUR DataFrame ---")
        print(final_df.head())
        print(final_df.info())

    return final_df


def load_mobile_tourist_data(file_path, debug=False):
    """
    Loads and processes mobile data for international tourists and frequently present visitors.

    Args:
        file_path (str): The path to the mobile data Parquet file.
        debug (bool): If True, prints debugging information.

    Returns:
        pandas.DataFrame: A DataFrame with annually aggregated mobile data.
    """
    if debug:
        print(f"--- Loading Mobile Tourist Data from: {file_path} ---")

    try:
        df = pd.read_parquet(file_path)
        if debug:
            print("Mobile data Parquet loaded successfully.")
    except Exception as e:
        print(f"Error loading mobile data: {e}")
        return pd.DataFrame()

    # Ensure fecha is datetime
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['year'] = df['fecha'].dt.year

    # Filter for international visitors who stay overnight
    # This includes 'Turista' and 'Habitualmente presente' (Frequently Present)
    overnight_categories = ['Turista', 'Habitualmente presente']
    df_filtered = df[
        (df['origen'] == 'Extranjero') &
        (df['categoriadelvisitante'].isin(overnight_categories))
        ].copy()

    if debug:
        print(f"\nFiltered for 'Extranjero' origin and categories: {overnight_categories}")

    # Aggregate daily data to get annual totals
    df_agg = df_filtered.groupby(['year', 'categoriadelvisitante'])['volumen_total'].sum().reset_index()

    # Pivot the table to have visitor categories as columns
    df_pivot = df_agg.pivot_table(
        index='year',
        columns='categoriadelvisitante',
        values='volumen_total',
        fill_value=0
    ).reset_index()

    # Rename columns for clarity
    df_pivot.rename(columns={
        'Turista': 'tourists_mobile',
        'Habitualmente presente': 'frequent_present_mobile'
    }, inplace=True)

    # Calculate total mobile visitors for comparison
    df_pivot['total_tourists_mobile'] = df_pivot['tourists_mobile'] + df_pivot['frequent_present_mobile']

    if debug:
        print("\n--- Processed Mobile Tourist DataFrame (Annual) ---")
        print(df_pivot.head())
        print(df_pivot.info())

    return df_pivot


def merge_tourist_data(df_ine, df_mobile, debug=False):
    """
    Merges the processed INE and mobile tourist dataframes on the 'year' column.

    Args:
        df_ine (pandas.DataFrame): The processed INE data.
        df_mobile (pandas.DataFrame): The processed mobile data.
        debug (bool): If True, prints debugging information.

    Returns:
        pandas.DataFrame: A merged DataFrame ready for visualization.
    """
    if not df_ine.empty and not df_mobile.empty:
        df_merged = pd.merge(df_ine, df_mobile, on='year', how='inner')
        if debug:
            print("\n--- Merged Comparison DataFrame ---")
            print(df_merged.head())
        return df_merged
    else:
        print("Could not merge dataframes, one or both are empty.")
        return pd.DataFrame()