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

def clean_daily(key: str, filename: str) -> pd.DataFrame:
    path = RAW_DIR / filename
    df = read_file(path)
    df = snake_case_columns(df)

    # Shorten verbose description columns
    df = df.rename(columns={
        "rate_code_description":   "rate_code_desc",
        "market_code_description": "market_code_desc",
        "source_code_description": "source_code_desc",
    })

    df = parse_date_column(df, "stay_date")
    df = drop_empty_rows(df)

    # 9999 is a PMS placeholder meaning "rate not available" — replace with NaN
    if "rate" in df.columns:
        df.loc[df["rate"] == 9999, "rate"] = pd.NA

    return df


def clean_res_main(filename: str) -> pd.DataFrame:
    path = RAW_DIR / filename
    df = read_file(path)
    df = snake_case_columns(df)

    # "print_rate_y_n" -> "print_rate"
    df = df.rename(columns={"print_rate_y_n": "print_rate"})

    for col in ["created_date", "cancel_date", "arrival_date", "departure_date"]:
        df = parse_date_column(df, col)

    df = drop_empty_rows(df)

    # Derived flag: whether the reservation was ever cancelled
    df["is_cancelled"] = df["cancel_date"].notna()

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # --- Daily reservation files (stay-night level) ---
    daily_24 = clean_daily("res_daily_24", "res_daily_24.csv")
    daily_25 = clean_daily("res_daily_25", "res_daily_25.csv")

    reservations_daily = pd.concat([daily_24, daily_25], ignore_index=True)

    out_daily = PROCESSED_DIR / "reservations_daily.csv"
    reservations_daily.to_csv(out_daily, index=False)

    print("reservations_daily.csv")
    print(f"  Rows   : {len(reservations_daily):,}")
    print(f"  Columns: {list(reservations_daily.columns)}")
    print(f"  Dates  : {reservations_daily['stay_date'].min().date()} -> {reservations_daily['stay_date'].max().date()}")
    print(f"  Nulls in rate: {reservations_daily['rate'].isna().sum():,}")
    print(f"  Saved  : {out_daily}")
    print()

    # --- res_main (booking level, one row per reservation) ---
    res_main = clean_res_main("res_main.csv")

    out_main = PROCESSED_DIR / "res_main_clean.csv"
    res_main.to_csv(out_main, index=False)

    print("res_main_clean.csv")
    print(f"  Rows      : {len(res_main):,}")
    print(f"  Columns   : {list(res_main.columns)}")
    print(f"  Arrivals  : {res_main['arrival_date'].min().date()} -> {res_main['arrival_date'].max().date()}")
    print(f"  Cancelled : {res_main['is_cancelled'].sum():,} of {len(res_main):,} ({res_main['is_cancelled'].mean()*100:.1f}%)")
    print(f"  Saved     : {out_main}")


if __name__ == "__main__":
    main()
