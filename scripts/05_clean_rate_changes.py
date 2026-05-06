import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from app.config import RAW_DIR, PROCESSED_DIR
from app.cleaners import snake_case_columns, parse_date_column, drop_empty_rows

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Rows 0-1 are title/metadata; row 2 is the real header
HEADER_ROW = 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_rate_change(filename: str) -> pd.DataFrame:
    df = pd.read_excel(RAW_DIR / filename, header=HEADER_ROW)
    df = snake_case_columns(df)
    df = drop_empty_rows(df)

    # "stay_date" column exists in the report template but is never populated — drop it
    if "stay_date" in df.columns:
        df = df.drop(columns=["stay_date"])

    df = parse_date_column(df, "modified_date")

    # Coerce rate columns to numeric (some cells exported as strings)
    for col in ["new_rate", "new_rate_sent", "old_rate", "old_rate_sent"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Derived: magnitude of the rate change
    if "new_rate" in df.columns and "old_rate" in df.columns:
        df["rate_change"] = df["new_rate"] - df["old_rate"]

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    rc_24 = load_rate_change("Arlo+Williamsburg+RateChange_2024-01-01_2024-12-31.xlsx")
    rc_25 = load_rate_change("Arlo+Williamsburg+RateChange_2025-01-01_2025-12-31.xlsx")

    rate_changes = pd.concat([rc_24, rc_25], ignore_index=True)

    out_path = PROCESSED_DIR / "rate_changes_clean.csv"
    rate_changes.to_csv(out_path, index=False)

    print("rate_changes_clean.csv")
    print(f"  Rows          : {len(rate_changes):,}")
    print(f"  Columns       : {list(rate_changes.columns)}")
    print(f"  modified_date : {rate_changes['modified_date'].min().date()} -> {rate_changes['modified_date'].max().date()}")
    print(f"  Segments      : {sorted(rate_changes['segment'].dropna().unique().tolist())}")
    print(f"  Rate codes    : {sorted(rate_changes['rate_codes'].dropna().unique().tolist())}")
    print(f"  rate_change range: {rate_changes['rate_change'].min():.0f} to {rate_changes['rate_change'].max():.0f}")
    print(f"  Saved         : {out_path}")


if __name__ == "__main__":
    main()
