import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from app.config import RAW_DIR, PROCESSED_DIR
from app.loaders import read_file
from app.cleaners import snake_case_columns, parse_date_column, drop_empty_rows

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_and_clean(filename: str) -> pd.DataFrame:
    df = read_file(RAW_DIR / filename)
    df = snake_case_columns(df)
    df = parse_date_column(df, "business_date")
    df = drop_empty_rows(df)
    # Standardize "average_rate" to the industry-standard abbreviation
    df = df.rename(columns={"average_rate": "adr"})
    return df


def summarize(label: str, df: pd.DataFrame, out_path: Path):
    print(f"{out_path.name}")
    print(f"  Rows     : {len(df):,}")
    print(f"  Columns  : {list(df.columns)}")
    print(f"  Dates    : {df['business_date'].min().date()} -> {df['business_date'].max().date()}")
    null_adr = df['adr'].isna().sum()
    print(f"  Null ADR : {null_adr:,} ({null_adr/len(df)*100:.1f}%)  — rows with no occupied rooms")
    print(f"  Saved    : {out_path}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # --- Daily stats by booking source ---
    source_24 = load_and_clean("wburg_daily_stats_source_2024.csv")
    source_25 = load_and_clean("wburg_daily_sats_source_2025.csv")
    stats_source = pd.concat([source_24, source_25], ignore_index=True)

    out_source = PROCESSED_DIR / "daily_stats_source.csv"
    stats_source.to_csv(out_source, index=False)
    summarize("source", stats_source, out_source)

    # --- Daily stats by market segment ---
    market_24 = load_and_clean("wburg_daily_stats_by_market_2024.csv")
    market_25 = load_and_clean("wburg_daily_stats_by_market_2025.csv")
    stats_market = pd.concat([market_24, market_25], ignore_index=True)

    out_market = PROCESSED_DIR / "daily_stats_market.csv"
    stats_market.to_csv(out_market, index=False)
    summarize("market", stats_market, out_market)

    # --- Daily stats by room type (already spans 2024-2025 in one file) ---
    stats_rt = load_and_clean("wburg_daily_stats_by_rt.csv")
    # Shorten the verbose physical rooms column name
    stats_rt = stats_rt.rename(columns={
        "total_physical_rooms_by_room_type": "total_physical_rooms"
    })

    out_rt = PROCESSED_DIR / "daily_stats_room_type.csv"
    stats_rt.to_csv(out_rt, index=False)
    summarize("room_type", stats_rt, out_rt)


if __name__ == "__main__":
    main()
