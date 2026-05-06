from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR       = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
FINAL_DIR     = BASE_DIR / "data" / "final"
OUTPUTS_DIR   = BASE_DIR / "outputs"

RAW_FILES = {
    "res_daily_24":           "res_daily_24.csv",
    "res_daily_25":           "res_daily_25.csv",
    "res_main":               "res_main.csv",

    "daily_stats_source_2024": "wburg_daily_stats_source_2024.csv",
    "daily_stats_source_2025": "wburg_daily_sats_source_2025.csv",   # note: typo in source filename

    "daily_stats_market_2024": "wburg_daily_stats_by_market_2024.csv",
    "daily_stats_market_2025": "wburg_daily_stats_by_market_2025.csv",

    "daily_stats_room_type":   "wburg_daily_stats_by_rt.csv",

    "rate_change_2024": "Arlo+Williamsburg+RateChange_2024-01-01_2024-12-31.xlsx",
    "rate_change_2025": "Arlo+Williamsburg+RateChange_2025-01-01_2025-12-31.xlsx",

    "medallia_2024_h1": "medallia_1_1_24__6_30_24.xls",
    "medallia_2024_h2": "medallia_7_1_24__12_31_24.xls",
    "medallia_2025_h1": "medallia_1_1_25__6_30_25.xls",
    "medallia_2025_h2": "medallia_7_1_25__12_31__25.xls",
}
