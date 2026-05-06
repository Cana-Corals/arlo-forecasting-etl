import pandas as pd
from pathlib import Path

#Code Description:
#Load every raw file
#Check if Python can read them
#Print columns, rows, and first few records
#Identify which files need special handling


# Project paths
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

# Raw files
files = {
    "res_daily_24": "res_daily_24.csv",
    "res_daily_25": "res_daily_25.csv",
    "res_main": "res_main.csv",

    "daily_stats_source_2024": "wburg_daily_stats_source_2024.csv",
    "daily_stats_source_2025": "wburg_daily_sats_source_2025.csv",

    "daily_stats_market_2024": "wburg_daily_stats_by_market_2024.csv",
    "daily_stats_market_2025": "wburg_daily_stats_by_market_2025.csv",

    "daily_stats_room_type": "wburg_daily_stats_by_rt.csv",

    "rate_change_2024": "Arlo+Williamsburg+RateChange_2024-01-01_2024-12-31.xlsx",
    "rate_change_2025": "Arlo+Williamsburg+RateChange_2025-01-01_2025-12-31.xlsx",

    "medallia_2024_h1": "medallia_1_1_24__6_30_24.xls",
    "medallia_2024_h2": "medallia_7_1_24__12_31_24.xls",
    "medallia_2025_h1": "medallia_1_1_25__6_30_25.xls",
    "medallia_2025_h2": "medallia_7_1_25__12_31__25.xls",
}


def inspect_dataframe(name, df):
    print("\n" + "=" * 80)
    print(f"FILE: {name}")
    print("=" * 80)

    print(f"Rows: {df.shape[0]}")
    print(f"Columns: {df.shape[1]}")

    print("\nColumn Names:")
    print(list(df.columns))

    print("\nFirst 5 Rows:")
    print(df.head())


def read_file(file_path):
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path)

    elif suffix == ".xlsx":
        return pd.read_excel(file_path)

    elif suffix == ".xls":
        try:
            return pd.read_excel(file_path)
        except Exception:
            tables = pd.read_html(file_path)
            return tables[0]

    else:
        raise ValueError(f"Unsupported file type: {suffix}")


def main():
    for name, filename in files.items():
        file_path = RAW_DIR / filename

        try:
            df = read_file(file_path)
            inspect_dataframe(name, df)

        except Exception as e:
            print("\n" + "=" * 80)
            print(f"ERROR READING: {name}")
            print(f"Path: {file_path}")
            print(f"Error: {e}")


if __name__ == "__main__":
    main()