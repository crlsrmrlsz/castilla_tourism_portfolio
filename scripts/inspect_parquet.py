# scripts/inspect_parquet.py

import pandas as pd
from pathlib import Path

# --- Configuration ---
# Use Path for OS compatibility
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ANALYTICS_DATA_DIR = PROJECT_ROOT / "data" / "analytics"

# --- IMPORTANT: CHANGE THIS LINE TO THE FILE YOU WANT TO CHECK ---
FILE_TO_INSPECT = "CCAA Castilla-La Mancha/Diario_demographics_analysis.parquet"


# --------------------------------------------------------------------

# --- Main Inspection Logic ---
def inspect_file(file_path):
    """Loads a Parquet file and prints a comprehensive summary."""
    if not file_path.exists():
        print(f"--- ERROR: File not found at {file_path} ---")
        return

    print(f"--- Analyzing File: {file_path.name} ---")

    try:
        df = pd.read_parquet(file_path)

        print("\n[1. Shape (Rows, Columns)]")
        print(df.shape)

        print("\n[2. Schema, Data Types, and Memory Usage (df.info())]")
        df.info(verbose=True, memory_usage='deep')

        print("\n[3. First 5 Rows (df.head())]")
        print(df.head())

        print("\n[4. Summary Statistics (df.describe())]")
        # include='all' provides stats for both numeric and object columns
        print(df.describe(include='all'))

        print(f"\n--- Inspection Complete for {file_path.name} ---")

    except Exception as e:
        print(f"Could not read or analyze file. Error: {e}\n")


if __name__ == '__main__':
    inspect_file(ANALYTICS_DATA_DIR / FILE_TO_INSPECT)