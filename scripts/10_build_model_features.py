"""
Build the model-ready dataset for hotel revenue forecasting.

Starts from hotel_daily_master.csv and adds:
  1. US federal holiday flags + proximity features
  2. Historical weather (temperature, precipitation, snow, wind)
  3. Booking pace (rooms on books at 7/14/21/30/60 day lead times)
  4. Rooms on books + average booked rate (as of data cut)
  5. Lag & rolling features for occupancy and revenue
  6. Explicit target columns
  7. Train / test split flag (train=2024, test=2025)

Output: data/final/hotel_model_ready.csv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
from app.config import FINAL_DIR, PROCESSED_DIR

FINAL_DIR.mkdir(parents=True, exist_ok=True)

PACE_LEAD_TIMES = [7, 14, 21, 30, 60]   # days before arrival


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lag(series: pd.Series, n: int) -> pd.Series:
    return series.shift(n)


def _rolling_mean(series: pd.Series, window: int) -> pd.Series:
    return series.shift(1).rolling(window, min_periods=1).mean().round(4)


# ---------------------------------------------------------------------------
# Step 1: Load master
# ---------------------------------------------------------------------------

def load_master() -> pd.DataFrame:
    df = pd.read_csv(FINAL_DIR / "hotel_daily_master.csv", parse_dates=["business_date"])
    df = df.sort_values("business_date").reset_index(drop=True)
    print(f"  [1] Master loaded: {len(df):,} rows")
    return df


# ---------------------------------------------------------------------------
# Step 2: Join holidays
# ---------------------------------------------------------------------------

def join_holidays(df: pd.DataFrame) -> pd.DataFrame:
    hol = pd.read_csv(
        PROCESSED_DIR / "federal_holidays.csv",
        parse_dates=["business_date"],
    )
    hol = hol[["business_date", "is_federal_holiday", "holiday_name"]]

    # Master already carries these columns; drop before re-merging to avoid _x/_y suffixes
    df = df.drop(columns=["is_federal_holiday", "holiday_name"], errors="ignore")
    df = df.merge(hol, on="business_date", how="left")

    # Days to next / from last federal holiday
    holiday_dates = hol.loc[hol["is_federal_holiday"], "business_date"].sort_values().values

    def _days_to_next(d):
        future = holiday_dates[holiday_dates > d]
        return (future[0] - d).astype("timedelta64[D]").astype(int) if len(future) else np.nan

    def _days_from_last(d):
        past = holiday_dates[holiday_dates < d]
        return (d - past[-1]).astype("timedelta64[D]").astype(int) if len(past) else np.nan

    dates_np = df["business_date"].values
    df["days_to_next_holiday"]  = [_days_to_next(d)  for d in dates_np]
    df["days_from_last_holiday"] = [_days_from_last(d) for d in dates_np]

    print(f"  [2] Holidays joined: {df['is_federal_holiday'].sum()} holiday days flagged")
    return df


# ---------------------------------------------------------------------------
# Step 3: Join weather
# ---------------------------------------------------------------------------

def join_weather(df: pd.DataFrame) -> pd.DataFrame:
    wx = pd.read_csv(
        PROCESSED_DIR / "weather_daily.csv",
        parse_dates=["business_date"],
    )
    wx = wx.drop(columns=["weather_description"], errors="ignore")

    # Master already carries weather columns; drop before re-merging to avoid _x/_y suffixes
    weather_cols = [c for c in wx.columns if c != "business_date"]
    df = df.drop(columns=weather_cols, errors="ignore")
    df = df.merge(wx, on="business_date", how="left")
    n_filled = df["temp_mean_f"].notna().sum()
    print(f"  [3] Weather joined: {n_filled:,} days with temperature data")
    return df


# ---------------------------------------------------------------------------
# Step 4: Booking pace from res_main_clean.csv
# ---------------------------------------------------------------------------

def join_booking_pace(df: pd.DataFrame) -> pd.DataFrame:
    res = pd.read_csv(PROCESSED_DIR / "res_main_clean.csv", low_memory=False)
    res["arrival_date"]  = pd.to_datetime(res["arrival_date"],  errors="coerce")
    res["created_date"]  = pd.to_datetime(res["created_date"],  errors="coerce")

    # Only confirmed reservations in our study window
    res = res[
        res["is_cancelled"].eq(False) &
        res["arrival_date"].between("2024-01-01", "2025-12-31") &
        res["arrival_date"].notna() &
        res["created_date"].notna()
    ].copy()

    res["lead_time_days"] = (res["arrival_date"] - res["created_date"]).dt.days

    for n in PACE_LEAD_TIMES:
        # Rooms on the books at least n days before arrival date
        pace = (
            res[res["lead_time_days"] >= n]
            .groupby("arrival_date")
            .size()
            .rename(f"pickup_{n}d")
            .reset_index()
            .rename(columns={"arrival_date": "business_date"})
        )
        df = df.merge(pace, on="business_date", how="left")
        df[f"pickup_{n}d"] = df[f"pickup_{n}d"].fillna(0).astype(int)

    cols = [f"pickup_{n}d" for n in PACE_LEAD_TIMES]
    print(f"  [4] Booking pace joined: {cols}")
    return df


# ---------------------------------------------------------------------------
# Step 5: Rooms on books + avg booked rate (as of data cut)
# ---------------------------------------------------------------------------

def join_rooms_on_books(df: pd.DataFrame) -> pd.DataFrame:
    rdaily = pd.read_csv(PROCESSED_DIR / "reservations_daily.csv", low_memory=False)
    rdaily["stay_date"] = pd.to_datetime(rdaily["stay_date"], errors="coerce")

    agg = rdaily.groupby("stay_date").agg(
        total_rooms_on_books=("confirmation_number", "count"),
        avg_booked_rate=("rate", "mean"),
    ).reset_index().rename(columns={"stay_date": "business_date"})
    agg["avg_booked_rate"] = agg["avg_booked_rate"].round(2)

    df = df.merge(agg, on="business_date", how="left")
    n = df["total_rooms_on_books"].notna().sum()
    print(f"  [5] Rooms on books joined: {n:,} days with reservation data")
    return df


# ---------------------------------------------------------------------------
# Step 6: Lag & rolling features
# ---------------------------------------------------------------------------

def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("business_date").reset_index(drop=True)

    for col, alias in [("occupancy_rate", "occ"), ("room_revenue", "rev"), ("adr", "adr")]:
        for lag in [7, 14, 28, 364]:
            df[f"{alias}_lag_{lag}d"] = _lag(df[col], lag).round(4)
        for window in [7, 28]:
            df[f"{alias}_roll_{window}d"] = _rolling_mean(df[col], window)

    print("  [6] Lag & rolling features added (occ, rev, adr x lag 7/14/28/364 + roll 7/28)")
    return df


# ---------------------------------------------------------------------------
# Step 7: STR comp set lag & rolling features
# ---------------------------------------------------------------------------

STR_LAG_COLS = [
    ("comp_occ",         "comp_occ"),
    ("comp_adr",         "comp_adr"),
    ("comp_revpar",      "comp_revpar"),
    ("mpi",              "mpi"),
    ("ari",              "ari"),
    ("rgi",              "rgi"),
    ("adr_gap_vs_comp",  "adr_gap_vs_comp"),   # competitive pricing constraint
]


def add_str_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("business_date").reset_index(drop=True)

    # Rate gap: positive = Arlo priced above comp set, negative = below.
    # Captures competitive positioning for dynamic pricing.
    if "adr" in df.columns and "comp_adr" in df.columns:
        df["adr_gap_vs_comp"] = (df["adr"] - df["comp_adr"]).round(2)

    added = []
    for col, alias in STR_LAG_COLS:
        if col not in df.columns:
            continue
        for lag in [7, 28]:
            name = f"{alias}_lag_{lag}d"
            df[name] = _lag(df[col], lag).round(4)
            added.append(name)
        for window in [7, 28]:
            name = f"{alias}_roll_{window}d"
            df[name] = _rolling_mean(df[col], window)
            added.append(name)
    print(f"  [7] STR lag/roll features added: {len(added)} columns (incl. adr_gap_vs_comp)")
    return df


# ---------------------------------------------------------------------------
# Step 8: Forward-fill Medallia nulls (scores lag by a week at most)
# ---------------------------------------------------------------------------

def fill_medallia(df: pd.DataFrame) -> pd.DataFrame:
    medallia_cols = [c for c in df.columns if c.startswith("medallia_")]
    df[medallia_cols] = df[medallia_cols].ffill().bfill()
    remaining = df[medallia_cols].isnull().sum().sum()
    print(f"  [7] Medallia forward/back-filled — {remaining} nulls remaining")
    return df


# ---------------------------------------------------------------------------
# Step 9: Drop string label columns (int versions already exist)
# ---------------------------------------------------------------------------

def drop_label_cols(df: pd.DataFrame) -> pd.DataFrame:
    drop = ["day_name", "month_name", "holiday_name"]
    df = df.drop(columns=[c for c in drop if c in df.columns])
    print(f"  [9] Dropped string label columns: {drop}")
    return df


# ---------------------------------------------------------------------------
# Step 10: Targets + train/test flag
# ---------------------------------------------------------------------------

def add_targets_and_split(df: pd.DataFrame) -> pd.DataFrame:
    df["target_room_revenue"]   = df["room_revenue"]
    df["target_occupancy_rate"] = df["occupancy_rate"]
    df["target_adr"]            = df["adr"]
    df["split"] = np.where(df["business_date"] < "2025-11-01", "train", "test")

    n_train = (df["split"] == "train").sum()
    n_test  = (df["split"] == "test").sum()
    print(f"  [10] Targets defined. Split — train: {n_train} days (2024–Oct 2025), test: {n_test} days (Nov–Dec 2025)")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Building model-ready dataset...")
    print()

    df = load_master()
    df = join_holidays(df)
    df = join_weather(df)
    df = join_booking_pace(df)
    df = join_rooms_on_books(df)
    df = add_lag_features(df)
    df = add_str_lag_features(df)
    df = fill_medallia(df)
    df = drop_label_cols(df)
    df = add_targets_and_split(df)

    # Final column ordering: date | actuals | external | pace | lags | targets | split
    front = ["business_date", "split"]
    targets = ["target_room_revenue", "target_occupancy_rate", "target_adr"]
    actuals = [
        "total_physical_rooms", "ooo_rooms", "available_rooms",
        "room_nights", "occupancy_rate",
        "room_revenue", "food_revenue", "other_revenue", "total_revenue",
        "adr", "revpar",
        "cancelled_rooms", "no_show_rooms", "day_use_rooms",
        "deduct_individual_rooms", "deduct_group_rooms",
    ]
    calendar = ["day_of_week", "month", "quarter", "week_of_year", "is_weekend"]
    retail   = ["retail_rate"]
    holiday  = ["is_federal_holiday", "days_to_next_holiday", "days_from_last_holiday"]
    weather  = ["temp_mean_f", "temp_max_f", "temp_min_f",
                "precipitation_in", "had_precipitation",
                "snowfall_in", "had_snow", "windspeed_max_mph", "weathercode"]
    medallia = [c for c in df.columns if c.startswith("medallia_")]
    pace     = [f"pickup_{n}d" for n in PACE_LEAD_TIMES] + ["total_rooms_on_books", "avg_booked_rate"]
    lags     = sorted([c for c in df.columns if "_lag_" in c or "_roll_" in c])

    col_order = front + targets + actuals + calendar + retail + holiday + weather + medallia + pace + lags
    col_order = [c for c in col_order if c in df.columns]
    remaining = [c for c in df.columns if c not in col_order]
    df = df[col_order + remaining]

    out_path = FINAL_DIR / "hotel_model_ready.csv"
    df.to_csv(out_path, index=False)

    print()
    print("hotel_model_ready.csv")
    print(f"  Rows    : {len(df):,}")
    print(f"  Columns : {len(df.columns)}")
    print()

    # Null summary for key feature groups
    lag_nulls = df[lags].isnull().sum()
    lag_nulls = lag_nulls[lag_nulls > 0]

    print("  Null counts (lag features — expected at start of series):")
    for col, n in lag_nulls.items():
        print(f"    {col:<35} {n}")

    print()
    print(f"  Saved: {out_path}")


if __name__ == "__main__":
    main()
