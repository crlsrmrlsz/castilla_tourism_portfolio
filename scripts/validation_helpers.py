# scripts/validation_helpers.py

import pandas as pd
import os


def load_and_clean_mobile_data(path_mobile):
    """
    Loads and performs initial cleaning on the mobile datasets.
    """
    nocturno_df = pd.read_parquet(os.path.join(path_mobile, "Nocturno_Mes_demographics_analysis.parquet"))
    noche_estancia_df = pd.read_parquet(os.path.join(path_mobile, "NocheEstancia_demographics_analysis.parquet"))
    nocturno_df['date'] = pd.to_datetime(nocturno_df['mes'], format='%Y%m')
    noche_estancia_df['month_date'] = pd.to_datetime(noche_estancia_df['fecha']).dt.to_period('M').dt.to_timestamp()
    return nocturno_df, noche_estancia_df


def load_and_clean_public_data(path_public):
    """
    Loads and performs initial cleaning on the public INE datasets.
    """
    # FIX APPLIED HERE: Changed encoding to 'utf-8' to match your file format
    eoh_df = pd.read_csv(
        os.path.join(path_public, "INE_EOH_Viajeros_pernoctaciones_mensual.csv"),
        sep=';',
        encoding='utf-8',
        thousands='.'
    )
    # FIX APPLIED HERE: Changed encoding to 'utf-8'
    frontur_df = pd.read_csv(
        os.path.join(path_public, "INE_FRONTUR_Total_anual_CLM.csv"),
        sep=';',
        encoding='utf-8',
        thousands='.'
    )

    # Whitespace stripping is still a good defensive practice
    eoh_df['Residencia: Nivel 2'] = eoh_df['Residencia: Nivel 2'].str.strip()
    eoh_df['Viajeros y pernoctaciones'] = eoh_df['Viajeros y pernoctaciones'].str.strip()

    eoh_df['date'] = pd.to_datetime(eoh_df['Periodo'], format='%YM%m')
    eoh_df['Total'] = pd.to_numeric(eoh_df['Total'], errors='coerce')
    return {'eoh': eoh_df, 'frontur': frontur_df}


def prepare_monthly_tourist_comparison(nocturno_df, eoh_df):
    """
    Prepares a monthly comparison DataFrame including mobile data (separated by role)
    and official EOH data.

    Changes from previous version:
    - Domestic mobile data now INCLUDES 'local' users to better match EOH criteria.
    - Mobile data is split into 'Turista' and 'Habitualmente_Presente' roles.
    """
    # Make a copy to avoid SettingWithCopyWarning
    df = nocturno_df.copy()

    # Convert 'mes' to datetime and extract the month start date
    df['date'] = pd.to_datetime(df['mes'], format='%Y%m').dt.to_period('M').dt.start_time

    # --- New Logic: Define domestic and foreign categories ---
    # Domestic now includes 'local' and 'non-local'
    is_domestic = df['origen'] == 'NoLocal'
    is_foreign = df['origen'] == 'Extranjero'

    # --- New Logic: Separate counts by role ---
    is_turista = (df['categoriadelvisitante'] == 'turista')
    is_habitual = (df['categoriadelvisitante'] == 'habitualmente_presente')

    # Calculate aggregates for each category and role
    domestic_turista = df[is_domestic & is_turista].groupby('date')['volumen_total'].sum()
    domestic_habitual = df[is_domestic & is_habitual].groupby('date')['volumen_total'].sum()
    foreign_turista = df[is_foreign & is_turista].groupby('date')['volumen_total'].sum()
    foreign_habitual = df[is_foreign & is_habitual].groupby('date')['volumen_total'].sum()

    # Combine into a single mobile data DataFrame
    mobile_df = pd.DataFrame({
        'Mobile_Domestic_Turista': domestic_turista,
        'Mobile_Domestic_Habitual': domestic_habitual,
        'Mobile_Foreign_Turista': foreign_turista,
        'Mobile_Foreign_Habitual': foreign_habitual,
    }).fillna(0) # Fill months with no data with 0

    # Prepare EOH data
    eoh_df['date'] = pd.to_datetime(eoh_df['date'])
    eoh_df.set_index('date', inplace=True)

    # Merge mobile and EOH data
    comparison_df = mobile_df.join(eoh_df, how='inner').reset_index()

    return comparison_df


def prepare_average_stay_comparison(noche_estancia_df, eoh_df):
    """
    Prepares a merged DataFrame for comparing the average length of stay.
    """
    mobile_tourist_stay = noche_estancia_df[
        noche_estancia_df['categoriadelvisitante'].isin(['Turista', 'Habitualmente presente'])
    ].copy()
    mobile_tourist_stay['total_nights'] = mobile_tourist_stay['duracionestancianum'] * mobile_tourist_stay[
        'volumen_total']
    mobile_stay_grouped = mobile_tourist_stay.groupby('month_date').agg(
        total_nights_sum=('total_nights', 'sum'),
        total_volume_sum=('volumen_total', 'sum')
    )
    mobile_stay_grouped['Mobile_Avg_Stay'] = mobile_stay_grouped['total_nights_sum'] / mobile_stay_grouped[
        'total_volume_sum']
    mobile_stay = mobile_stay_grouped.reset_index().rename(columns={'month_date': 'date'})

    total_pernoctaciones = eoh_df[
        (eoh_df['Provincias'].isnull()) &
        (eoh_df['Viajeros y pernoctaciones'] == 'Pernoctaciones') &
        (eoh_df['Residencia: Nivel 2'] == 'Total')
        ][['date', 'Total']].rename(columns={'Total': 'Pernoctaciones'})

    total_viajeros = eoh_df[
        (eoh_df['Provincias'].isnull()) &
        (eoh_df['Viajeros y pernoctaciones'] == 'Viajero') &
        (eoh_df['Residencia: Nivel 2'] == 'Total')
        ][['date', 'Total']].rename(columns={'Total': 'Viajero'})

    eoh_avg_stay_df = pd.merge(total_pernoctaciones, total_viajeros, on='date', how='inner')
    eoh_avg_stay_df['EOH_Avg_Stay'] = eoh_avg_stay_df['Pernoctaciones'] / eoh_avg_stay_df['Viajero']

    comparison_df = pd.merge(
        mobile_stay[['date', 'Mobile_Avg_Stay']],
        eoh_avg_stay_df[['date', 'EOH_Avg_Stay']],
        on='date',
        how='inner'
    )
    return comparison_df


def prepare_city_population_comparison(path_analytics, path_public):
    """
    Prepares a DataFrame comparing resident population for each provincial capital
    between mobile data and official Padrón files.
    """
    cities = {
        'Albacete': 'AB', 'Ciudad Real': 'CR', 'Cuenca': 'CU',
        'Guadalajara': 'GU', 'Toledo': 'TO'
    }
    results = []

    for city_name, city_code in cities.items():
        mobile_path = os.path.join(path_analytics, f"{city_name} Municipio",
                                   "Nocturno_Mes_demographics_analysis.parquet")
        mobile_df = pd.read_parquet(mobile_path)
        mobile_df['year'] = pd.to_datetime(mobile_df['mes'], format='%Y%m').dt.year
        mobile_residents = mobile_df[
            (mobile_df['categoriadelvisitante'] == 'Residente') &
            (mobile_df['origen'] == 'Local')
            ]
        mobile_pop = mobile_residents.groupby('year')['volumen_total'].mean().reset_index()
        mobile_pop.rename(columns={'volumen_total': 'Mobile_Population'}, inplace=True)
        mobile_pop['City'] = city_name

        public_path = os.path.join(path_public, f"INE_{city_code}_padron_genero_anual.csv")
        # FIX APPLIED HERE: Changed encoding to 'utf-8'
        public_df = pd.read_csv(public_path, sep=';', encoding='utf-8')
        public_pop = public_df[public_df['Sexo'] == 'Total'][['Periodo', 'Total']]
        public_pop.rename(columns={'Periodo': 'year', 'Total': 'Padron_Population'}, inplace=True)
        public_pop['City'] = city_name

        comparison_df = pd.merge(mobile_pop, public_pop, on=['year', 'City'], how='inner')
        results.append(comparison_df)

    return pd.concat(results, ignore_index=True)