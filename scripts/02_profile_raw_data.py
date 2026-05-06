import sys
from pathlib import Path
from io import StringIO

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from app.config import RAW_DIR, RAW_FILES, OUTPUTS_DIR
from app.loaders import read_file

OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUTS_DIR / "raw_data_profile.txt"


def profile_dataframe(name: str, df: pd.DataFrame) -> str:
    buf = StringIO()

    def w(line=""):
        buf.write(line + "\n")

    w("=" * 80)
    w(f"FILE KEY : {name}")
    w("=" * 80)
    w(f"Shape    : {df.shape[0]:,} rows  x  {df.shape[1]} columns")
    w()

    # Column-level summary
    w(f"{'Column':<40} {'Dtype':<15} {'Nulls':>8} {'Null %':>8}")
    w("-" * 75)
    for col in df.columns:
        dtype  = str(df[col].dtype)
        nulls  = int(df[col].isna().sum())
        pct    = nulls / len(df) * 100 if len(df) > 0 else 0
        w(f"  {col:<38} {dtype:<15} {nulls:>8,} {pct:>7.1f}%")
    w()

    # Numeric columns: range summary
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if num_cols:
        w("Numeric ranges:")
        w(f"  {'Column':<38} {'Min':>14} {'Max':>14} {'Mean':>14}")
        w("  " + "-" * 70)
        for col in num_cols:
            col_data = df[col].dropna()
            if len(col_data) == 0:
                continue
            w(f"  {col:<38} {col_data.min():>14,.2f} {col_data.max():>14,.2f} {col_data.mean():>14,.2f}")
        w()

    # Date detection: columns whose name contains common date keywords
    date_keywords = ("date", "dt", "day", "arrival", "departure", "check", "stay", "period", "created")
    date_candidates = [
        c for c in df.columns
        if any(kw in c.lower() for kw in date_keywords)
    ]
    if date_candidates:
        w("Date column ranges (parsed):")
        for col in date_candidates:
            try:
                parsed = pd.to_datetime(df[col], errors="coerce")
                valid  = parsed.dropna()
                if len(valid) == 0:
                    w(f"  {col:<38}  (no parseable dates)")
                else:
                    w(f"  {col:<38}  {str(valid.min().date())}  ->  {str(valid.max().date())}  ({len(valid):,} valid)")
            except Exception as e:
                w(f"  {col:<38}  ERROR: {e}")
        w()

    return buf.getvalue()


def main():
    lines = []
    lines.append("RAW DATA PROFILE REPORT")
    lines.append(f"Generated from: {RAW_DIR}")
    lines.append("=" * 80)
    lines.append("")

    for name, filename in RAW_FILES.items():
        file_path = RAW_DIR / filename
        try:
            df = read_file(file_path)
            block = profile_dataframe(name, df)
            lines.append(block)
        except Exception as e:
            lines.append("=" * 80)
            lines.append(f"FILE KEY : {name}")
            lines.append("=" * 80)
            lines.append(f"  ERROR: {e}")
            lines.append("")

    report = "\n".join(lines)

    # Print to console
    print(report)

    # Save to file
    OUTPUT_FILE.write_text(report, encoding="utf-8")
    print(f"\nReport saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
