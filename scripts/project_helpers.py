# /scripts/project_helpers.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as mticker
from pathlib import Path
import glob

# ==============================================================================
# --- CONFIGURATION ---
# All constants and paths are defined here.
# ==============================================================================

# BASE_DIR points to the project's root directory (the parent of 'scripts')
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Raw INE Data Path
RAW_INE_PATH = DATA_DIR / "raw" / "ine_data"

# Processed Mobile Data Path
ANALYTICS_MOBILE_PATH = DATA_DIR / "analytics"

# Location Mappings
LOCATION_MAPPING = {
    "AB": "Albacete", "CR": "Ciudad Real", "CU": "Cuenca",
    "GU": "Guadalajara", "TO": "Toledo", "CCAA": "Castilla-La Mancha"
}

# Plotting Style
INE_COLOR = "#1f77b4"
MOBILE_DATA_COLOR = "#ff7f0e"

# Data Specifics
TOURIST_CATEGORIES = ['Turista', 'Habitualmente presente']


# ==============================================================================
# --- DATA LOADING FUNCTIONS ---
# Functions dedicated to loading and preprocessing data.
# ==============================================================================

def load_ine_ecp() -> pd.DataFrame:
    """Loads and combines all ECP population files."""
    files = glob.glob(str(RAW_INE_PATH / "ECP_CLM_*.csv"))
    df_list = []
    for f in files:
        year_str = Path(f).stem.split('_')[2]
        if not year_str.isdigit(): continue

        # Using thousands='.' to automatically handle number formatting
        df = pd.read_csv(f, sep=';', thousands='.')
        df.columns = [col.replace(' ', '_') for col in df.columns]

        clm_total_col = 'CASTILLA-LA_MANCHA'
        if clm_total_col not in df.columns: continue

        clm_total_idx = df.columns.get_loc(clm_total_col)

        subset = df[['Edad', df.columns[clm_total_idx + 1], df.columns[clm_total_idx + 2]]].copy()
        subset.columns = ['Edad', 'Hombres', 'Mujeres']

        # Columns are now read as numbers directly
        subset['Total'] = df[clm_total_col]
        subset['Hombres'] = subset['Hombres']
        subset['Mujeres'] = subset['Mujeres']
        subset['year'] = int(year_str)
        df_list.append(subset)

    if not df_list: return pd.DataFrame()

    full_df = pd.concat(df_list, ignore_index=True)
    total_pop = full_df.groupby('year')[['Total', 'Hombres', 'Mujeres']].sum().reset_index()
    return total_pop


def load_ine_municipal_padron() -> pd.DataFrame:
    """Loads annual municipal population data for all cities."""
    files = glob.glob(str(RAW_INE_PATH / "INE_*_padron_genero_anual.csv"))
    df_list = []
    for f in files:
        city_code = Path(f).stem.split('_')[1]
        # Using thousands='.' to automatically handle number formatting
        df = pd.read_csv(f, sep=';', thousands='.')
        df['year'] = pd.to_datetime(df['Periodo'], format='%Y').year
        df['provincia'] = LOCATION_MAPPING.get(city_code, city_code)
        df = df[['year', 'provincia', 'Total']]
        df_list.append(df)
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


def load_ine_frontur() -> pd.DataFrame:
    """Loads annual foreign tourism data (FRONTUR)."""
    file_path = RAW_INE_PATH / "INE_FRONTUR_Total_anual_CLM.csv"
    if not file_path.exists(): return pd.DataFrame()
    # Using thousands='.' to automatically handle number formatting
    df = pd.read_csv(file_path, sep=';', thousands='.')
    df['year'] = pd.to_datetime(df['Periodo'], format='%Y').year
    return df[['year', 'Total']]


def load_ine_eoh() -> pd.DataFrame:
    """Loads monthly hotel occupancy data (EOH)."""
    file_path = RAW_INE_PATH / "INE_EOH_Viajeros_pernoctaciones_mensual.csv"
    if not file_path.exists(): return pd.DataFrame()
    # Using thousands='.' to automatically handle number formatting
    df = pd.read_csv(file_path, sep=';', thousands='.')
    df['fecha'] = pd.to_datetime(df['Periodo'], format='%YM%m')
    df.rename(columns={'Indicador': 'indicador', 'Residencia': 'residencia'}, inplace=True)

    df_pivot = df.pivot_table(
        index=['fecha', 'Provincia'],
        columns=['indicador', 'residencia'],
        values='Total'
    ).reset_index()

    df_pivot.columns = ['_'.join(col).strip() if isinstance(col, tuple) and col[1] else col[0] for col in
                        df_pivot.columns.values]
    df_pivot.rename(columns={'Provincia_': 'provincia'}, inplace=True)
    return df_pivot


def load_mobile_data(location_folder: str, file_name: str) -> pd.DataFrame:
    """Loads a specific mobile data parquet file from a given location."""
    file_path = ANALYTICS_MOBILE_PATH / location_folder / file_name
    if not file_path.exists():
        print(f"Warning: File not found at {file_path}")
        return pd.DataFrame()

    df = pd.read_parquet(file_path)

    if 'mes' in df.columns:
        df['fecha'] = pd.to_datetime(df['mes'], format='%Y%m')
    if 'fecha' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['fecha']):
        df['fecha'] = pd.to_datetime(df['fecha'])

    return df


# ==============================================================================
# --- ANALYSIS FUNCTIONS ---
# Functions that perform the core data comparisons.
# ==============================================================================

def prepare_set1_clm_population(df_mobile_diario, df_ine_ecp):
    mob_residents = df_mobile_diario[df_mobile_diario['categoriadelvisitante'] == 'Residente'].copy()
    mob_residents['year'] = mob_residents['fecha'].dt.year

    mob_avg_pop = mob_residents.groupby('year').agg(
        mobile_total_avg=('volumen_total', 'mean'),
        mobile_hombres_avg=('volumen_genero_h', 'mean'),
        mobile_mujeres_avg=('volumen_genero_m', 'mean')
    ).reset_index()

    df_ine_ecp.rename(columns={'Total': 'ine_total', 'Hombres': 'ine_hombres', 'Mujeres': 'ine_mujeres'}, inplace=True)
    return pd.merge(df_ine_ecp, mob_avg_pop, on='year', how='inner')


def prepare_set2_municipal_population(df_mobile_diario, df_ine_padron):
    mob_residents = df_mobile_diario[df_mobile_diario['categoriadelvisitante'] == 'Residente'].copy()
    mob_residents['year'] = mob_residents['fecha'].dt.year

    mob_avg_pop = mob_residents.groupby(['year', 'provincia'])['volumen_total'].mean().reset_index()
    mob_avg_pop.rename(columns={'volumen_total': 'mobile_total_avg'}, inplace=True)

    df_ine_padron.rename(columns={'Total': 'ine_total'}, inplace=True)
    return pd.merge(df_ine_padron, mob_avg_pop, on=['year', 'provincia'], how='inner')


def prepare_set3_foreign_tourism(df_mobile_diario_origin, df_ine_frontur):
    mob_foreign_tourists = df_mobile_diario_origin[
        (df_mobile_diario_origin['categoriadelvisitante'].isin(TOURIST_CATEGORIES)) &
        (df_mobile_diario_origin['origen'] == 'Extranjero')
        ].copy()
    mob_foreign_tourists['year'] = mob_foreign_tourists['fecha'].dt.year

    mob_annual_total = mob_foreign_tourists.groupby('year')['volumen_total'].sum().reset_index()
    mob_annual_total.rename(columns={'volumen_total': 'mobile_total_annual'}, inplace=True)

    df_ine_frontur.rename(columns={'Total': 'ine_total_annual'}, inplace=True)
    return pd.merge(df_ine_frontur, mob_annual_total, on='year', how='inner')


def prepare_set4_hotel_travelers(df_mobile_nocturno_origin, df_ine_eoh):
    ine_viajeros = df_ine_eoh.copy()
    ine_viajeros['ine_viajeros_esp'] = ine_viajeros['Viajero_Residentes en España']
    ine_viajeros['ine_viajeros_ext'] = ine_viajeros['Viajero_Residentes en el Extranjero']
    ine_viajeros = ine_viajeros[['fecha', 'provincia', 'ine_viajeros_esp', 'ine_viajeros_ext']].dropna(
        subset=['ine_viajeros_esp', 'ine_viajeros_ext'])

    mob_nocturno = df_mobile_nocturno_origin[
        df_mobile_nocturno_origin['categoriadelvisitante'].isin(TOURIST_CATEGORIES)].copy()
    mob_nocturno['origen_agg'] = mob_nocturno['origen'].apply(lambda x: 'ext' if x == 'Extranjero' else 'esp')
    mob_monthly = mob_nocturno.groupby(['fecha', 'provincia', 'origen_agg'])[
        'volumen_total'].sum().unstack().reset_index()
    mob_monthly.rename(columns={'esp': 'mobile_viajeros_esp', 'ext': 'mobile_viajeros_ext'}, inplace=True)

    return pd.merge(ine_viajeros, mob_monthly, on=['fecha', 'provincia'], how='inner')


def prepare_set5_overnight_stays(df_mobile_noche_estancia, df_ine_eoh):
    ine_pernoctaciones = df_ine_eoh.copy()
    ine_pernoctaciones['ine_pernoctaciones_total'] = ine_pernoctaciones['Pernoctaciones_Total']
    ine_pernoctaciones = ine_pernoctaciones[['fecha', 'provincia', 'ine_pernoctaciones_total']].dropna()

    mob_noche = df_mobile_noche_estancia[
        df_mobile_noche_estancia['categoriadelvisitante'].isin(TOURIST_CATEGORIES)].copy()
    mob_noche['total_stays'] = mob_noche['volumen_total'] * mob_noche['duracionestancianum']
    mob_noche['fecha_month'] = mob_noche['fecha'].to_period('M').to_timestamp()

    mob_monthly_stays = mob_noche.groupby(['fecha_month', 'provincia'])['total_stays'].sum().reset_index()
    mob_monthly_stays.rename(columns={'fecha_month': 'fecha', 'total_stays': 'mobile_pernoctaciones_total'},
                             inplace=True)

    return pd.merge(ine_pernoctaciones, mob_monthly_stays, on=['fecha', 'provincia'], how='inner')


# ==============================================================================
# --- PLOTTING FUNCTIONS ---
# Functions for creating professional, reusable visualizations.
# ==============================================================================

def setup_plot_style():
    """Sets a professional and consistent style for all plots."""
    sns.set_theme(style="whitegrid")
    plt.rcParams.update({
        'figure.figsize': (12, 7), 'axes.titlesize': 16, 'axes.labelsize': 12,
        'xtick.labelsize': 10, 'ytick.labelsize': 10, 'legend.fontsize': 11
    })


def format_yaxis_thousands(ax):
    """Formats y-axis labels to K (thousands) or M (millions)."""
    formatter = mticker.FuncFormatter(lambda x, p: f'{x / 1_000_000:,.1f}M' if x >= 1_000_000 else f'{x / 1_000:,.0f}K')
    ax.yaxis.set_major_formatter(formatter)


def plot_annual_comparison(df, ine_col, mobile_col, title, ylabel, filename):
    setup_plot_style()
    df_plot = df[['year', ine_col, mobile_col]].set_index('year')
    df_plot.columns = ['INE (Official)', 'Mobile Data']
    ax = df_plot.plot(kind='bar', color=[INE_COLOR, MOBILE_DATA_COLOR], rot=0)
    ax.set(title=title, xlabel="Year", ylabel=ylabel)
    format_yaxis_thousands(ax)
    ax.legend(title="Data Source")
    plt.tight_layout()
    filepath = OUTPUT_DIR / f"{filename}.png"
    plt.savefig(filepath, dpi=300)
    print(f"Chart saved to {filepath}")
    plt.show()


def plot_monthly_timeseries(df, ine_col, mobile_col, title, ylabel, filename):
    setup_plot_style()
    fig, ax = plt.subplots()
    ax.plot(df['fecha'], df[ine_col], label='INE (Official)', color=INE_COLOR, marker='o', ms=5)
    ax.plot(df['fecha'], df[mobile_col], label='Mobile Data', color=MOBILE_DATA_COLOR, marker='x', ms=5, ls='--')
    ax.set(title=title, xlabel="Date", ylabel=ylabel)
    format_yaxis_thousands(ax)
    ax.legend(title="Data Source")
    fig.autofmt_xdate()
    plt.tight_layout()
    filepath = OUTPUT_DIR / f"{filename}.png"
    plt.savefig(filepath, dpi=300)
    print(f"Chart saved to {filepath}")
    plt.show()


def plot_gender_comparison(df, title, filename):
    setup_plot_style()
    df_plot = df[['year', 'ine_hombres', 'mobile_hombres_avg', 'ine_mujeres', 'mobile_mujeres_avg']].set_index('year')
    men_data = df_plot[['ine_hombres', 'mobile_hombres_avg']].rename(
        columns={'ine_hombres': 'INE', 'mobile_hombres_avg': 'Mobile'})
    women_data = df_plot[['ine_mujeres', 'mobile_mujeres_avg']].rename(
        columns={'ine_mujeres': 'INE', 'mobile_mujeres_avg': 'Mobile'})
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7), sharey=True)
    men_data.plot(kind='bar', ax=ax1, color=[INE_COLOR, MOBILE_DATA_COLOR], rot=0)
    ax1.set(title="Male Population", ylabel="Average Monthly Population", xlabel="Year")
    format_yaxis_thousands(ax1)
    women_data.plot(kind='bar', ax=ax2, color=[INE_COLOR, MOBILE_DATA_COLOR], rot=0)
    ax2.set(title="Female Population", xlabel="Year")
    fig.suptitle(title, fontsize=20)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    filepath = OUTPUT_DIR / f"{filename}.png"
    plt.savefig(filepath, dpi=300)
    print(f"Chart saved to {filepath}")
    plt.show()