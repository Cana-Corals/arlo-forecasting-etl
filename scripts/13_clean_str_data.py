import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from app.config import RAW_DIR, PROCESSED_DIR

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

STR_FILE = RAW_DIR / "Arlo_Williamsburg_CompSetRaw.csv"


def clean_str(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    # period -> business_date
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
        # Raw counts
        "demand", "supply", "revenue",
        "comp_set_demand", "comp_set_supply", "comp_set_revenue",
        # Arlo KPIs
        "str_occ", "str_adr", "str_revpar",
        # Comp set KPIs
        "comp_occ", "comp_adr", "comp_revpar",
        # Index scores
        "mpi", "ari", "rgi",
    ]
    return df[col_order]


def main():
    df = clean_str(STR_FILE)

    out_path = PROCESSED_DIR / "str_daily.csv"
    df.to_csv(out_path, index=False)

    print("str_daily.csv")
    print(f"  Rows        : {len(df):,}")
    print(f"  Date range  : {df['business_date'].min().date()} -> {df['business_date'].max().date()}")
    print(f"  Columns     : {list(df.columns)}")
    print()
    print("  Arlo (STR-reported):")
    print(f"    Occ%       : {df['str_occ'].mean()*100:.1f}%")
    print(f"    ADR        : ${df['str_adr'].mean():.2f}")
    print(f"    RevPAR     : ${df['str_revpar'].mean():.2f}")
    print()
    print("  Comp Set:")
    print(f"    Occ%       : {df['comp_occ'].mean()*100:.1f}%")
    print(f"    ADR        : ${df['comp_adr'].mean():.2f}")
    print(f"    RevPAR     : ${df['comp_revpar'].mean():.2f}")
    print()
    print("  Index scores (100 = parity):")
    print(f"    MPI (occ)  : {df['mpi'].mean():.1f}")
    print(f"    ARI (rate) : {df['ari'].mean():.1f}")
    print(f"    RGI (rev)  : {df['rgi'].mean():.1f}")
    print(f"  Saved       : {out_path}")


if __name__ == "__main__":
    main()
