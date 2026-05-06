"""
Fetch US federal holidays for 2024-2025 and save a daily indicator file.

Primary source : https://www.federalpay.org/resources/holidays/Federal%20Holidays%20-%20all.csv
Fallback source: https://date.nager.at/api/v3/PublicHolidays/{year}/US
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import requests
from app.config import PROCESSED_DIR

YEARS       = [2024, 2025]
DATE_START  = "2024-01-01"
DATE_END    = "2025-12-31"

PRIMARY_URL  = "https://www.federalpay.org/resources/holidays/Federal%20Holidays%20-%20all.csv"
NAGER_URL    = "https://date.nager.at/api/v3/PublicHolidays/{year}/US"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _load_primary() -> pd.DataFrame | None:
    """Try the FederalPay CSV. Returns a clean DataFrame or None on failure."""
    try:
        resp = requests.get(PRIMARY_URL, timeout=15)
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        # Columns vary; find the date and name columns heuristically
        date_col = next((c for c in df.columns if "date" in c.lower()), None)
        name_col = next((c for c in df.columns if "name" in c.lower() or "holiday" in c.lower()), None)
        if date_col is None or name_col is None:
            print(f"  [primary] Unexpected columns: {list(df.columns)} — skipping")
            return None
        df = df[[date_col, name_col]].rename(columns={date_col: "business_date", name_col: "holiday_name"})
        df["business_date"] = pd.to_datetime(df["business_date"], errors="coerce")
        df = df.dropna(subset=["business_date"])
        # Filter to our date range
        df = df[
            (df["business_date"] >= DATE_START) &
            (df["business_date"] <= DATE_END)
        ]
        print(f"  [primary] Loaded {len(df)} holidays from FederalPay CSV")
        return df
    except Exception as exc:
        print(f"  [primary] Failed: {exc}")
        return None


def _load_nager() -> pd.DataFrame:
    """Fetch from Nager API for each year and concatenate."""
    rows = []
    for year in YEARS:
        url = NAGER_URL.format(year=year)
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        for item in resp.json():
            rows.append({"business_date": item["date"], "holiday_name": item["localName"]})
    df = pd.DataFrame(rows)
    df["business_date"] = pd.to_datetime(df["business_date"])
    print(f"  [fallback] Loaded {len(df)} holidays from Nager API")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Fetching US federal holidays (2024-2025)...")

    holidays = _load_primary()
    if holidays is None or holidays.empty:
        print("  Primary source failed — using Nager API fallback")
        holidays = _load_nager()

    holidays = holidays.drop_duplicates(subset="business_date").sort_values("business_date")

    # Build a full daily spine and left-join the holidays
    spine = pd.DataFrame({
        "business_date": pd.date_range(DATE_START, DATE_END, freq="D")
    })
    daily = spine.merge(holidays, on="business_date", how="left")
    daily["is_federal_holiday"] = daily["holiday_name"].notna()

    out_path = PROCESSED_DIR / "federal_holidays.csv"
    daily.to_csv(out_path, index=False)

    n_holidays = daily["is_federal_holiday"].sum()
    print(f"\nfederal_holidays.csv")
    print(f"  Rows             : {len(daily):,} (one per calendar day)")
    print(f"  Federal holidays : {n_holidays}")
    print()
    print(daily[daily["is_federal_holiday"]][["business_date", "holiday_name"]].to_string(index=False))
    print(f"\n  Saved: {out_path}")


if __name__ == "__main__":
    main()
