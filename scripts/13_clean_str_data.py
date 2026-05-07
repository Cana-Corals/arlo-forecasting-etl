import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from app.config import RAW_DIR, PROCESSED_DIR

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

STR_FILE    = RAW_DIR / "Arlo_Williamsburg_CompSetRaw.csv"
MASTER_START = "2024-01-01"   # first date in hotel_daily_master.csv


def clean_str(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    df = df.rename(columns={"period": "business_date"})
    df["business_date"] = pd.to_datetime(df["business_date"], format="mixed")
    df = df.sort_values("business_date").reset_index(drop=True)

    # Arlo KPIs (cross-check against PMS)
    df["str_occ"]    = (df["demand"] / df["supply"]).round(4)
    df["str_adr"]    = (df["revenue"] / df["demand"].replace(0, pd.NA)).round(2)
    df["str_revpar"] = (df["revenue"] / df["supply"]).round(2)

    # Comp set KPIs
    df["comp_occ"]    = (df["comp_set_demand"] / df["comp_set_supply"]).round(4)
    df["comp_adr"]    = (df["comp_set_revenue"] / df["comp_set_demand"].replace(0, pd.NA)).round(2)
    df["comp_revpar"] = (df["comp_set_revenue"] / df["comp_set_supply"]).round(2)

    # STR index scores (100 = parity with comp set; >100 = outperforming)
    df["mpi"] = (df["str_occ"]    / df["comp_occ"].replace(0, pd.NA)    * 100).round(2)
    df["ari"] = (df["str_adr"]    / df["comp_adr"].replace(0, pd.NA)    * 100).round(2)
    df["rgi"] = (df["str_revpar"] / df["comp_revpar"].replace(0, pd.NA) * 100).round(2)

    col_order = [
        "business_date",
        "demand", "supply", "revenue",
        "comp_set_demand", "comp_set_supply", "comp_set_revenue",
        "str_occ", "str_adr", "str_revpar",
        "comp_occ", "comp_adr", "comp_revpar",
        "mpi", "ari", "rgi",
    ]
    return df[col_order]


def backfill_from_prior_year(df: pd.DataFrame, full_start: str = MASTER_START) -> pd.DataFrame:
    """
    Fill the gap between full_start and the first actual STR date using the
    same calendar day from the following year (e.g., Jan 15 2024 gets the
    values from Jan 15 2025). This preserves month and day-of-week seasonality.

    Jan–Apr 2024 had the same comp set composition as 2025, so the year-over-year
    fill is the most defensible imputation available given the STR 3-year purge policy.
    """
    str_start = df["business_date"].min()
    gap_dates = pd.date_range(full_start, str_start - pd.Timedelta(days=1))

    if len(gap_dates) == 0:
        return df

    metric_cols = [c for c in df.columns if c != "business_date"]
    rows = []
    for date in gap_dates:
        try:
            match_date = date.replace(year=date.year + 1)
        except ValueError:
            # Feb 29 in a leap year — use Feb 28
            match_date = date.replace(year=date.year + 1, day=28)

        match_row = df[df["business_date"] == match_date]
        if match_row.empty:
            continue

        row = match_row.iloc[0][metric_cols].copy()
        row["business_date"] = date
        rows.append(row)

    if not rows:
        return df

    backfill_df = pd.DataFrame(rows)[["business_date"] + metric_cols]
    df = pd.concat([backfill_df, df], ignore_index=True)
    df = df.sort_values("business_date").reset_index(drop=True)
    return df


def main():
    df = clean_str(STR_FILE)

    str_actual_start = df["business_date"].min().date()
    str_actual_end   = df["business_date"].max().date()
    n_actual = len(df)

    df = backfill_from_prior_year(df)

    n_backfilled = len(df) - n_actual
    str_full_start = df["business_date"].min().date()

    out_path = PROCESSED_DIR / "str_daily.csv"
    df.to_csv(out_path, index=False)

    print("str_daily.csv")
    print(f"  Actual STR data : {str_actual_start} -> {str_actual_end} ({n_actual:,} days)")
    print(f"  YoY backfill    : {str_full_start} -> {str_actual_start} ({n_backfilled} days imputed from 2025)")
    print(f"  Total rows      : {len(df):,}")
    print(f"  Columns         : {list(df.columns)}")
    print()
    print("  Comp Set (full range, backfill included):")
    print(f"    Occ%           : {df['comp_occ'].mean()*100:.1f}%")
    print(f"    ADR            : ${df['comp_adr'].mean():.2f}")
    print(f"    RevPAR         : ${df['comp_revpar'].mean():.2f}")
    print()
    print("  Index scores (100 = parity):")
    print(f"    MPI (occ)      : {df['mpi'].mean():.1f}")
    print(f"    ARI (rate)     : {df['ari'].mean():.1f}")
    print(f"    RGI (rev)      : {df['rgi'].mean():.1f}")
    print(f"  Saved            : {out_path}")


if __name__ == "__main__":
    main()
