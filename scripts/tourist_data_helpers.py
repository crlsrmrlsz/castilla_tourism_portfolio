# scripts/tourist_data_helpers.py

import pandas as pd
import os


def load_ine_frontur_data(file_path, debug=False):
    """
    Loads and processes the annual INE FRONTUR data for international tourists.
    (This function remains unchanged)
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

    df.rename(columns={'Periodo': 'year', 'Total': 'tourists_ine'}, inplace=True)
    df['tourists_ine'] = pd.to_numeric(df['tourists_ine'], errors='coerce')
    df_clm = df[df['Comunidades autónomas'].str.contains("Castilla - La Mancha", na=False)].copy()
    final_df = df_clm[['year', 'tourists_ine']].reset_index(drop=True)

    if debug:
        print("\n--- Processed INE FRONTUR DataFrame ---")
        print(final_df.head())
    return final_df


def load_mobile_tourist_data(file_path, debug=False):
    """
    Loads and processes mobile data for international tourists,
    extrapolating partial years based on 2023's seasonality.
    """
    if debug:
        print(f"--- Loading Mobile Tourist Data from: {file_path} ---")

    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"Error loading mobile data: {e}")
        return pd.DataFrame()

    df['fecha'] = pd.to_datetime(df['fecha'])
    df['year'] = df['fecha'].dt.year
    df['month'] = df['fecha'].dt.month

    overnight_categories = ['Turista', 'Habitualmente presente']
    df_filtered = df[
        (df['origen'] == 'Extranjero') &
        (df['categoriadelvisitante'].isin(overnight_categories))
        ].copy()

    # Get monthly totals for all years
    monthly_totals = df_filtered.groupby(['year', 'month', 'categoriadelvisitante'])['volumen_total'].sum().unstack(
        fill_value=0).reset_index()
    monthly_totals['total'] = monthly_totals['Turista'] + monthly_totals.get('Habitualmente presente', 0)

    # --- Step 1: Calculate Seasonality Weights from 2023 data ---
    df_2023 = monthly_totals[monthly_totals['year'] == 2023]
    if df_2023.empty or len(df_2023) < 12:
        print("Warning: Full 2023 data not available to create seasonal profile. Extrapolation may be inaccurate.")
        seasonality_weights = pd.Series([1 / 12] * 12, index=range(1, 13))  # Fallback to equal weights
    else:
        total_2023 = df_2023['total'].sum()
        seasonality_weights = df_2023.groupby('month')['total'].sum() / total_2023

    if debug:
        print("\n--- 2023 Seasonality Weights ---")
        print(seasonality_weights)

    # --- Step 2: Process each year, extrapolating if data is incomplete ---
    annual_results = []
    for year, group in monthly_totals.groupby('year'):
        available_months = group['month'].unique()
        actual_sum_tourists = group['Turista'].sum()
        actual_sum_frequent = group.get('Habitualmente presente', 0).sum()

        # An incomplete year is one that doesn't have all 12 months of data
        is_complete = (len(available_months) == 12)

        if is_complete or year == 2023:
            extrapolated_tourists = actual_sum_tourists
            extrapolated_frequent = actual_sum_frequent
            note = "Complete Year"
        else:
            # Sum the weights for the months we have data for
            weight_of_available_months = seasonality_weights.loc[available_months].sum()

            if weight_of_available_months > 0:
                extrapolation_factor = 1 / weight_of_available_months
                extrapolated_tourists = actual_sum_tourists * extrapolation_factor
                extrapolated_frequent = actual_sum_frequent * extrapolation_factor
                note = f"Extrapolated from {len(available_months)} months"
            else:
                extrapolated_tourists = actual_sum_tourists
                extrapolated_frequent = actual_sum_frequent
                note = "Warning: No seasonal data for months"

        annual_results.append({
            'year': year,
            'tourists_mobile': extrapolated_tourists,
            'frequent_present_mobile': extrapolated_frequent,
            'note': note
        })

    final_df = pd.DataFrame(annual_results)
    final_df['total_tourists_mobile'] = final_df['tourists_mobile'] + final_df['frequent_present_mobile']

    if debug:
        print("\n--- Processed & Extrapolated Mobile Tourist DataFrame (Annual) ---")
        print(final_df)

    return final_df


def merge_tourist_data(df_ine, df_mobile, debug=False):
    """
    Merges the processed INE and mobile tourist dataframes on the 'year' column.
    (This function remains unchanged)
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