import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from app.config import RAW_DIR, PROCESSED_DIR
from app.loaders import read_file
from app.cleaners import snake_case_columns

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Week columns to exclude from the melt (aggregates, not weekly data points)
EXCLUDE_COLS = {"total", "benchmarkprior_3_months", "benchmarkprior_3_months_1"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_week_date(col_name: str, base_year: int, is_h2: bool) -> pd.Timestamp:
    """Convert 'Week of MM/ DD' column name to a date.

    H1 files: months > 6 belong to the previous year (e.g., Dec 31 in a Jan-Jun file).
    H2 files: all months belong to base_year.
    """
    match = re.search(r"Week of\s+(\d{1,2})/\s*(\d{1,2})", col_name, re.IGNORECASE)
    if not match:
        return pd.NaT
    month, day = int(match.group(1)), int(match.group(2))
    year = base_year - 1 if (not is_h2 and month > 6) else base_year
    try:
        return pd.Timestamp(year=year, month=month, day=day)
    except Exception:
        return pd.NaT


def is_section_header(row: pd.Series) -> bool:
    """Return True if the week columns repeat the metric name (i.e. no data)."""
    metric = str(row.iloc[0]).strip()
    first_week_val = str(row.iloc[1]).strip()
    return first_week_val == metric


def clean_medallia(filename: str, base_year: int, is_h2: bool) -> pd.DataFrame:
    df = read_file(RAW_DIR / filename)

    # Identify week columns (exclude Unnamed: 0, Total, Benchmark cols)
    week_cols = [c for c in df.columns if str(c).startswith("Week of")]

    # Drop section header rows (metric name repeated across all week columns)
    df = df[~df.apply(is_section_header, axis=1)].reset_index(drop=True)

    # Build metric name → snake_case mapping from column 0
    df = df.rename(columns={"Unnamed: 0": "metric"})

    # Keep only metric + week columns
    df = df[["metric"] + week_cols]

    # Replace dash (no response) with NaN and coerce to numeric
    df[week_cols] = df[week_cols].replace("-", pd.NA)
    for col in week_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Melt wide → long: one row per (metric, week)
    df_long = df.melt(id_vars="metric", value_vars=week_cols,
                      var_name="week_label", value_name="score")

    # Map week label → actual date
    df_long["week_date"] = df_long["week_label"].apply(
        lambda c: parse_week_date(c, base_year, is_h2)
    )

    # Clean up metric names to snake_case
    df_long["metric"] = df_long["metric"].str.strip()

    df_long = df_long[["week_date", "metric", "score"]].dropna(subset=["week_date"])
    df_long = df_long.sort_values(["week_date", "metric"]).reset_index(drop=True)

    return df_long


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

FILES = [
    ("medallia_1_1_24__6_30_24.xls",  2024, False),
    ("medallia_7_1_24__12_31_24.xls",  2024, True),
    ("medallia_1_1_25__6_30_25.xls",  2025, False),
    ("medallia_7_1_25__12_31__25.xls", 2025, True),
]


def main():
    parts = []
    for filename, base_year, is_h2 in FILES:
        df = clean_medallia(filename, base_year, is_h2)
        parts.append(df)

    medallia = pd.concat(parts, ignore_index=True)

    # Remove duplicate (week_date, metric) rows that appear in overlapping H1/H2 boundary weeks
    medallia = medallia.drop_duplicates(subset=["week_date", "metric"], keep="first")
    medallia = medallia.sort_values(["week_date", "metric"]).reset_index(drop=True)

    out_path = PROCESSED_DIR / "medallia_clean.csv"
    medallia.to_csv(out_path, index=False)

    print("medallia_clean.csv")
    print(f"  Rows        : {len(medallia):,}")
    print(f"  Columns     : {list(medallia.columns)}")
    print(f"  Weeks       : {medallia['week_date'].min().date()} -> {medallia['week_date'].max().date()}")
    print(f"  Metrics     : {medallia['metric'].nunique()} unique")
    print(f"  Top metrics :")
    for m in ["Sample Size (Red = less than 75)", "Net Promoter Score",
              "Likelihood To Recommend", "Value for Price",
              "Overall Satisfaction with Service", "Hotel cleanliness"]:
        subset = medallia[medallia["metric"] == m]
        if not subset.empty:
            avg = subset["score"].mean()
            print(f"    {m}: avg={avg:.1f}, weeks={subset['score'].notna().sum()}")
    print(f"  Saved       : {out_path}")


if __name__ == "__main__":
    main()
