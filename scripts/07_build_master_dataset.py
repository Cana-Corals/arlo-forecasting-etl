import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from app.config import PROCESSED_DIR, FINAL_DIR

FINAL_DIR.mkdir(parents=True, exist_ok=True)

# Medallia metrics to include in the master dataset (10-point scale, Key Scores section)
MEDALLIA_METRICS = {
    "Sample Size (Red = less than 75)": "medallia_sample_size",
    "Likelihood To Recommend":          "medallia_likelihood_to_recommend",
    "Likelihood to Return":             "medallia_likelihood_to_return",
    "Value for Price":                  "medallia_value_for_price",
    "Overall Satisfaction with Service":"medallia_overall_satisfaction",
    "Hotel cleanliness":                "medallia_hotel_cleanliness",
}


# ---------------------------------------------------------------------------
# Step 1: Build daily property spine from room-type stats
# ---------------------------------------------------------------------------

def build_daily_spine() -> pd.DataFrame:
    rt = pd.read_csv(PROCESSED_DIR / "daily_stats_room_type.csv", parse_dates=["business_date"])

    agg = rt.groupby("business_date").agg(
        total_physical_rooms    =("total_physical_rooms",    "sum"),
        ooo_rooms               =("ooo_rooms",               "sum"),
        cancelled_rooms         =("cancelled_rooms",         "sum"),
        day_use_rooms           =("day_use_rooms",           "sum"),
        no_show_rooms           =("no_show_rooms",           "sum"),
        deduct_individual_rooms =("deduct_individual_rooms", "sum"),
        deduct_group_rooms      =("deduct_group_rooms",      "sum"),
        room_nights             =("room_nights",             "sum"),
        room_revenue            =("room_revenue",            "sum"),
        food_revenue            =("food_revenue",            "sum"),
        other_revenue           =("other_revenue",           "sum"),
        total_revenue           =("total_revenue",           "sum"),
    ).reset_index()

    # Derived KPIs
    agg["available_rooms"] = agg["total_physical_rooms"] - agg["ooo_rooms"]
    agg["occupancy_rate"]  = (agg["room_nights"] / agg["available_rooms"]).round(4)
    # Recalculate ADR at property level (revenue-weighted across room types)
    agg["adr"]    = (agg["room_revenue"] / agg["room_nights"].replace(0, pd.NA)).round(2)
    agg["revpar"] = (agg["room_revenue"] / agg["available_rooms"].replace(0, pd.NA)).round(2)

    return agg


# ---------------------------------------------------------------------------
# Step 2: Join current retail rate (most recent rate change as of each date)
# ---------------------------------------------------------------------------

def join_retail_rate(daily: pd.DataFrame) -> pd.DataFrame:
    rc = pd.read_csv(PROCESSED_DIR / "rate_changes_clean.csv", parse_dates=["modified_date"])

    rate_spine = (
        rc[["modified_date", "new_rate"]]
        .dropna()
        .sort_values("modified_date")
        .rename(columns={"modified_date": "business_date", "new_rate": "retail_rate"})
    )

    daily = daily.sort_values("business_date")
    daily = pd.merge_asof(daily, rate_spine, on="business_date", direction="backward")
    return daily


# ---------------------------------------------------------------------------
# Step 3: Join Medallia weekly scores (forward-filled to daily)
# ---------------------------------------------------------------------------

def join_medallia(daily: pd.DataFrame) -> pd.DataFrame:
    med = pd.read_csv(PROCESSED_DIR / "medallia_clean.csv", parse_dates=["week_date"])

    # Keep only the metrics we want and rename them
    med = med[med["metric"].isin(MEDALLIA_METRICS)].copy()
    med["metric"] = med["metric"].map(MEDALLIA_METRICS)

    # Pivot long -> wide: one row per week_date, one column per metric
    med_wide = med.pivot_table(index="week_date", columns="metric", values="score", aggfunc="mean")
    med_wide = med_wide.reset_index().rename_axis(None, axis=1)
    med_wide = med_wide.sort_values("week_date")

    # Merge weekly scores onto daily dates using backward fill
    # (each day inherits the most recent week's scores)
    daily = pd.merge_asof(daily, med_wide, left_on="business_date", right_on="week_date",
                          direction="backward")
    daily = daily.drop(columns=["week_date"], errors="ignore")
    return daily


# ---------------------------------------------------------------------------
# Step 4: Add calendar features
# ---------------------------------------------------------------------------

def add_calendar_features(daily: pd.DataFrame) -> pd.DataFrame:
    d = daily["business_date"]
    daily["day_of_week"]  = d.dt.dayofweek          # 0=Monday, 6=Sunday
    daily["day_name"]     = d.dt.day_name()
    daily["month"]        = d.dt.month
    daily["month_name"]   = d.dt.month_name()
    daily["quarter"]      = d.dt.quarter
    daily["week_of_year"] = d.dt.isocalendar().week.astype(int)
    daily["is_weekend"]   = d.dt.dayofweek >= 5
    return daily


# ---------------------------------------------------------------------------
# Step 5: Join daily weather signals
# ---------------------------------------------------------------------------

def join_weather(daily: pd.DataFrame) -> pd.DataFrame:
    path = PROCESSED_DIR / "weather_daily.csv"
    if not path.exists():
        print("  [WARNING] weather_daily.csv not found — run 09_fetch_weather.py first")
        return daily
    w = pd.read_csv(path, parse_dates=["business_date"],
                    true_values=["True"], false_values=["False"])
    return daily.merge(w, on="business_date", how="left")


# ---------------------------------------------------------------------------
# Step 6: Join federal holiday flags
# ---------------------------------------------------------------------------

def join_holidays(daily: pd.DataFrame) -> pd.DataFrame:
    path = PROCESSED_DIR / "federal_holidays.csv"
    if not path.exists():
        print("  [WARNING] federal_holidays.csv not found — run 08_fetch_holidays.py first")
        return daily
    h = pd.read_csv(path, parse_dates=["business_date"],
                    true_values=["True"], false_values=["False"])
    h = h[["business_date", "is_federal_holiday", "holiday_name"]]
    return daily.merge(h, on="business_date", how="left")


# ---------------------------------------------------------------------------
# Step 7: Join NYC major event signals
# ---------------------------------------------------------------------------

def join_nyc_events(daily: pd.DataFrame) -> pd.DataFrame:
    path = PROCESSED_DIR / "nyc_events_daily.csv"
    if not path.exists():
        print("  [WARNING] nyc_events_daily.csv not found — run 10_fetch_nyc_events.py first")
        return daily
    ev = pd.read_csv(path, parse_dates=["business_date"],
                     true_values=["True"], false_values=["False"])
    return daily.merge(ev, on="business_date", how="left")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Building daily master dataset...")

    daily = build_daily_spine()
    print(f"  [1/7] Spine built         : {len(daily):,} days ({daily['business_date'].min().date()} -> {daily['business_date'].max().date()})")

    daily = join_retail_rate(daily)
    print(f"  [2/7] Retail rate joined  : {daily['retail_rate'].notna().sum():,} days with rate data")

    daily = join_medallia(daily)
    print(f"  [3/7] Medallia joined     : {daily['medallia_overall_satisfaction'].notna().sum():,} days with satisfaction scores")

    daily = add_calendar_features(daily)
    print(f"  [4/7] Calendar features   : added day_of_week, month, quarter, is_weekend, etc.")

    daily = join_weather(daily)
    n_weather = daily["temp_mean_f"].notna().sum() if "temp_mean_f" in daily.columns else 0
    print(f"  [5/7] Weather joined      : {n_weather:,} days with weather data")

    daily = join_holidays(daily)
    n_holidays = int(daily["is_federal_holiday"].sum()) if "is_federal_holiday" in daily.columns else 0
    print(f"  [6/7] Holidays joined     : {n_holidays} federal holiday days flagged")

    daily = join_nyc_events(daily)
    n_events = int(daily["is_major_event_day"].sum()) if "is_major_event_day" in daily.columns else 0
    print(f"  [7/7] NYC events joined   : {n_events} major event days")

    # Final column order
    col_order = [
        "business_date",
        # Capacity
        "total_physical_rooms", "ooo_rooms", "available_rooms",
        # Occupancy
        "room_nights", "occupancy_rate",
        # Revenue & pricing
        "room_revenue", "food_revenue", "other_revenue", "total_revenue",
        "adr", "revpar",
        # Other room activity
        "cancelled_rooms", "no_show_rooms", "day_use_rooms",
        "deduct_individual_rooms", "deduct_group_rooms",
        # Retail rate
        "retail_rate",
        # Satisfaction
        "medallia_sample_size", "medallia_overall_satisfaction",
        "medallia_likelihood_to_recommend", "medallia_likelihood_to_return",
        "medallia_value_for_price", "medallia_hotel_cleanliness",
        # Calendar
        "day_of_week", "day_name", "month", "month_name",
        "quarter", "week_of_year", "is_weekend",
        # Weather
        "temp_mean_f", "temp_max_f", "temp_min_f",
        "precipitation_in", "had_precipitation",
        "snowfall_in", "had_snow",
        "windspeed_max_mph", "weathercode", "weather_description",
        # Holidays
        "is_federal_holiday", "holiday_name",
        # NYC events
        "event_count", "is_major_event_day",
        "is_barclays_event", "is_msg_event",
        "is_yankees_game", "is_mets_game",
        "is_nba_game", "is_mlb_game", "is_nhl_game",
        "is_major_concert", "is_major_sports_event",
        "days_to_next_event",
    ]
    daily = daily[[c for c in col_order if c in daily.columns]]

    out_path = FINAL_DIR / "hotel_daily_master.csv"
    daily.to_csv(out_path, index=False)

    print()
    print("hotel_daily_master.csv")
    print(f"  Rows    : {len(daily):,}")
    print(f"  Columns : {len(daily.columns)} — {list(daily.columns)}")
    print()
    print("  Sample KPIs (2024-2025 averages):")
    print(f"    Occupancy rate : {daily['occupancy_rate'].mean()*100:.1f}%")
    print(f"    ADR            : ${daily['adr'].mean():.2f}")
    print(f"    RevPAR         : ${daily['revpar'].mean():.2f}")
    print(f"    Room revenue   : ${daily['room_revenue'].mean():,.0f}/day")
    print(f"    Retail rate    : ${daily['retail_rate'].mean():.2f} avg")
    print(f"  Saved   : {out_path}")


if __name__ == "__main__":
    main()
