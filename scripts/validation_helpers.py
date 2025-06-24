# scripts/validation_helpers.py

import pandas as pd
import os


# (The first two functions: load_and_clean_mobile_data and load_and_clean_public_data are correct)
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
    eoh_df = pd.read_csv(
        os.path.join(path_public, "INE_EOH_Viajeros_pernoctaciones_mensual.csv"),
        sep=';',
        encoding='latin1'
    )
    frontur_df = pd.read_csv(
        os.path.join(path_public, "INE_FRONTUR_Total_anual_CLM.csv"),
        sep=';',
        encoding='latin1'
    )
    eoh_df['date'] = pd.to_datetime(eoh_df['Periodo'], format='%YM%m')
    eoh_df['Total'] = pd.to_numeric(eoh_df['Total'], errors='coerce')
    return {'eoh': eoh_df, 'frontur': frontur_df}


# (Function prepare_monthly_tourist_comparison is correct)
def prepare_monthly_tourist_comparison(nocturno_df, eoh_df):
    """
    Prepares a merged DataFrame for comparing monthly tourist volumes.
    """
    mobile_tourists = nocturno_df[
        nocturno_df['categoriadelvisitante'].isin(['Turista', 'Habitualmente presente'])
    ].copy()
    mobile_pivot = mobile_tourists.pivot_table(
        index='date', columns='origen', values='volumen_total', aggfunc='sum'
    ).reset_index()
    mobile_pivot = mobile_pivot[['date', 'NoLocal', 'Extranjero']]
    mobile_pivot.columns = ['date', 'Mobile_Domestic', 'Mobile_Foreign']
    eoh_viajeros = eoh_df[
        (eoh_df['Viajeros y pernoctaciones'] == 'Viajero') &
        (eoh_df['Provincias'].isnull())
        ].copy()
    eoh_pivot = eoh_viajeros.pivot_table(
        index='date', columns='Residencia: Nivel 2', values='Total'
    ).reset_index()
    eoh_pivot = eoh_pivot[['date', 'Residentes en España', 'Residentes en el Extranjero']]
    eoh_pivot.columns = ['date', 'EOH_Domestic', 'EOH_Foreign']
    comparison_df = pd.merge(mobile_pivot, eoh_pivot, on='date', how='inner')
    return comparison_df


##### vvvvvv THIS IS THE FUNCTION TO REPLACE vvvvvv #####
def prepare_average_stay_comparison(noche_estancia_df, eoh_df):
    """
    Prepares a merged DataFrame for comparing the average length of stay.
    CORRECTED: This version is more robust and avoids the pivot_table KeyError.
    """
    # 1. Calculate Mobile Average Stay for tourists (this part is correct)
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

    # 2. Calculate EOH Average Stay (Robust Method)
    # Get total Pernoctaciones per month
    total_pernoctaciones = eoh_df[
        (eoh_df['Provincias'].isnull()) &
        (eoh_df['Viajeros y pernoctaciones'] == 'Pernoctaciones') &
        (eoh_df['Residencia: Nivel 2'] == 'Total')
        ][['date', 'Total']].rename(columns={'Total': 'Pernoctaciones'})

    # Get total Viajeros per month
    total_viajeros = eoh_df[
        (eoh_df['Provincias'].isnull()) &
        (eoh_df['Viajeros y pernoctaciones'] == 'Viajero') &
        (eoh_df['Residencia: Nivel 2'] == 'Total')
        ][['date', 'Total']].rename(columns={'Total': 'Viajero'})

    # Merge the two tables and calculate the average stay
    eoh_avg_stay_df = pd.merge(total_pernoctaciones, total_viajeros, on='date', how='inner')
    eoh_avg_stay_df['EOH_Avg_Stay'] = eoh_avg_stay_df['Pernoctaciones'] / eoh_avg_stay_df['Viajero']

    # 3. Merge mobile and EOH results
    comparison_df = pd.merge(
        mobile_stay[['date', 'Mobile_Avg_Stay']],
        eoh_avg_stay_df[['date', 'EOH_Avg_Stay']],
        on='date',
        how='inner'
    )
    return comparison_df


##### ^^^^^^ THIS IS THE FUNCTION TO REPLACE ^^^^^^ #####


# (Function prepare_city_population_comparison is correct)
def prepare_city_population_comparison(path_analytics, path_public):
    """
    NEW: Prepares a DataFrame comparing resident population for each provincial capital
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
        public_df = pd.read_csv(public_path, sep=';', encoding='latin1')
        public_pop = public_df[public_df['Sexo'] == 'Total'][['Periodo', 'Total']]
        public_pop.rename(columns={'Periodo': 'year', 'Total': 'Padron_Population'}, inplace=True)
        public_pop['City'] = city_name

        comparison_df = pd.merge(mobile_pop, public_pop, on=['year', 'City'], how='inner')
        results.append(comparison_df)

    return pd.concat(results, ignore_index=True)