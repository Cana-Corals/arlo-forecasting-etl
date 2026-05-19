"""
Forward-looking forecast engine.

Strategy:
  - 364-day lag from 2025 is the primary seasonality signal.
  - 2024 same-period data is used to compute a YoY growth trend.
  - Trend is applied to 2025 values to project 2026.
  - Booking pace features use the average of 2024 and 2025 same periods.
  - Weather uses historical monthly averages across both years.
  - Events default to 0 (no 2026 event calendar yet).
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path

BASE = Path(__file__).resolve().parents[2]

# ── 2026 US Federal holidays ──────────────────────────────────────────────────
_HOLIDAYS_2026 = [
    pd.Timestamp("2026-01-01"),  # New Year's Day
    pd.Timestamp("2026-01-19"),  # MLK Day
    pd.Timestamp("2026-02-16"),  # Presidents' Day
    pd.Timestamp("2026-05-25"),  # Memorial Day
    pd.Timestamp("2026-06-19"),  # Juneteenth
    pd.Timestamp("2026-07-04"),  # Independence Day
    pd.Timestamp("2026-09-07"),  # Labor Day
    pd.Timestamp("2026-10-12"),  # Columbus Day
    pd.Timestamp("2026-11-11"),  # Veterans Day
    pd.Timestamp("2026-11-26"),  # Thanksgiving
    pd.Timestamp("2026-12-25"),  # Christmas
]


def _days_to_next(d, hol_list):
    future = [h for h in hol_list if h > d]
    return int((future[0] - d).days) if future else 60


def _days_from_last(d, hol_list):
    past = [h for h in hol_list if h <= d]
    return int((d - past[-1]).days) if past else 30


def generate_future_predictions(forecast_year: int = 2026) -> pd.DataFrame:
    """
    Returns daily ML predictions for forecast_year.
    Columns: business_date, pred_revenue, pred_occupancy, pred_adr, pred_revpar
    """
    ready  = pd.read_csv(BASE / "data" / "final" / "hotel_model_ready.csv",  parse_dates=["business_date"])
    master = pd.read_csv(BASE / "data" / "final" / "hotel_daily_master.csv", parse_dates=["business_date"])

    m_rev = lgb.Booster(model_file=str(BASE / "models" / "lgbm_revenue.txt"))
    m_occ = lgb.Booster(model_file=str(BASE / "models" / "lgbm_occupancy.txt"))
    m_adr = lgb.Booster(model_file=str(BASE / "models" / "lgbm_adr.txt"))

    rev_features = m_rev.feature_name()   # 67
    adr_features = m_adr.feature_name()   # 95

    ready_2025 = ready[ready["business_date"].dt.year == 2025].copy()
    ready_2024 = ready[ready["business_date"].dt.year == 2024].copy()
    master_all = master.copy()

    # ── YoY trend: compute growth rate 2024→2025 by day-of-year ──────────────
    _KPI_COLS = {
        "occ":  "target_occupancy_rate",
        "rev":  "target_room_revenue",
        "adr":  "target_adr",
    }
    trend_2025 = ready_2025.set_index(ready_2025["business_date"].dt.dayofyear)
    trend_2024 = ready_2024.set_index(ready_2024["business_date"].dt.dayofyear)

    # Per-month growth multiplier (smoother than per-day)
    def monthly_growth(col):
        y25 = master[master["business_date"].dt.year == 2025].groupby(
            master[master["business_date"].dt.year == 2025]["business_date"].dt.month)[col].mean()
        y24 = master[master["business_date"].dt.year == 2024].groupby(
            master[master["business_date"].dt.year == 2024]["business_date"].dt.month)[col].mean()
        ratio = y25 / y24.replace(0, np.nan)
        return ratio.fillna(1.0).clip(0.8, 1.3)   # cap extreme swings

    occ_growth = monthly_growth("occupancy_rate")
    rev_growth = monthly_growth("room_revenue")
    adr_growth = monthly_growth("adr")

    # ── Weather: 2-year monthly averages ────────────────────────────────────
    wx_cols = ["temp_mean_f", "temp_max_f", "temp_min_f",
               "precipitation_in", "had_precipitation",
               "snowfall_in", "had_snow", "windspeed_max_mph", "weathercode"]
    wx_avg = master_all.groupby(master_all["business_date"].dt.month)[wx_cols].mean()

    # ── STR monthly averages (2025 only, most recent) ───────────────────────
    str_extra = [c for c in adr_features if c not in rev_features]
    str_cols_in_ready = [c for c in str_extra if c in ready.columns]
    str_zero = [c for c in str_extra if c not in ready.columns]
    if str_cols_in_ready:
        str_avg = ready_2025.groupby(ready_2025["business_date"].dt.month)[str_cols_in_ready].mean()
    else:
        str_avg = pd.DataFrame()

    # ── Medallia: trailing 90-day average (end of 2025) ─────────────────────
    med_cols = ["medallia_sample_size", "medallia_overall_satisfaction",
                "medallia_likelihood_to_recommend", "medallia_likelihood_to_return",
                "medallia_value_for_price", "medallia_hotel_cleanliness"]
    med_avg = master[master["business_date"] >= "2025-10-01"][med_cols].mean().fillna(0)

    # ── Retail rate: 2025 monthly average ───────────────────────────────────
    rate_avg = (master[master["business_date"].dt.year == 2025]
                .groupby(master[master["business_date"].dt.year == 2025]["business_date"].dt.month)["retail_rate"]
                .mean())

    # ── Booking pace: average of 2024 and 2025 same-period ──────────────────
    pace_cols = ["pickup_7d", "pickup_14d", "pickup_21d", "pickup_30d", "pickup_60d",
                 "total_rooms_on_books", "avg_booked_rate"]
    pace_2025 = ready_2025.groupby(ready_2025["business_date"].dt.month)[pace_cols].mean()
    pace_2024 = ready_2024.groupby(ready_2024["business_date"].dt.month)[pace_cols].mean()
    pace_avg  = ((pace_2025 + pace_2024) / 2).fillna(pace_2025)

    # ── Build future rows ────────────────────────────────────────────────────
    future_dates = pd.date_range(f"{forecast_year}-01-01", f"{forecast_year}-12-31", freq="D")
    rows = []

    for d in future_dates:
        lag_d   = d - pd.Timedelta(days=364)
        lag_row = ready[ready["business_date"] == lag_d]
        if lag_row.empty:
            diffs   = (ready["business_date"] - lag_d).abs()
            lag_row = ready.loc[[diffs.idxmin()]]
        lr = lag_row.iloc[0]

        month = d.month
        g_occ = float(occ_growth.get(month, 1.0))
        g_rev = float(rev_growth.get(month, 1.0))
        g_adr = float(adr_growth.get(month, 1.0))

        wx = wx_avg.loc[month]
        pace = pace_avg.loc[month] if month in pace_avg.index else pace_2025.loc[month]

        def fl(key, fallback=0.0):
            v = lr.get(key, fallback)
            return float(v) if v is not None and not (isinstance(v, float) and np.isnan(v)) else float(fallback)

        row = {
            "business_date":          d,
            "ooo_rooms":              0,
            "available_rooms":        147,
            "day_of_week":            d.dayofweek,
            "month":                  month,
            "quarter":                d.quarter,
            "week_of_year":           int(d.isocalendar().week),
            "is_weekend":             int(d.dayofweek >= 5),
            "retail_rate":            float(rate_avg.get(month, 220)) * g_adr,
            "is_federal_holiday":     int(d in _HOLIDAYS_2026),
            "days_to_next_holiday":   _days_to_next(d, _HOLIDAYS_2026),
            "days_from_last_holiday": _days_from_last(d, _HOLIDAYS_2026),
            **wx.to_dict(),
            **med_avg.to_dict(),
            # Booking pace: 2-year average for same month
            "pickup_7d":              float(pace.get("pickup_7d", 0)),
            "pickup_14d":             float(pace.get("pickup_14d", 0)),
            "pickup_21d":             float(pace.get("pickup_21d", 0)),
            "pickup_30d":             float(pace.get("pickup_30d", 0)),
            "pickup_60d":             float(pace.get("pickup_60d", 0)),
            "total_rooms_on_books":   float(pace.get("total_rooms_on_books", 80)),
            "avg_booked_rate":        float(pace.get("avg_booked_rate", 200)) * g_adr,
            # Lag features: 2025 same-period × YoY growth trend
            "occ_lag_7d":    fl("occ_lag_7d")   * g_occ,
            "occ_lag_14d":   fl("occ_lag_14d")  * g_occ,
            "occ_lag_28d":   fl("occ_lag_28d")  * g_occ,
            "occ_lag_364d":  fl("occ_lag_364d") * g_occ,
            "occ_roll_7d":   fl("occ_roll_7d")  * g_occ,
            "occ_roll_28d":  fl("occ_roll_28d") * g_occ,
            "rev_lag_7d":    fl("rev_lag_7d")   * g_rev,
            "rev_lag_14d":   fl("rev_lag_14d")  * g_rev,
            "rev_lag_28d":   fl("rev_lag_28d")  * g_rev,
            "rev_lag_364d":  fl("rev_lag_364d") * g_rev,
            "rev_roll_7d":   fl("rev_roll_7d")  * g_rev,
            "rev_roll_28d":  fl("rev_roll_28d") * g_rev,
            "adr_lag_7d":    fl("adr_lag_7d")   * g_adr,
            "adr_lag_14d":   fl("adr_lag_14d")  * g_adr,
            "adr_lag_28d":   fl("adr_lag_28d")  * g_adr,
            "adr_lag_364d":  fl("adr_lag_364d") * g_adr,
            "adr_roll_7d":   fl("adr_roll_7d")  * g_adr,
            "adr_roll_28d":  fl("adr_roll_28d") * g_adr,
            # Events: none loaded for 2026 yet
            "is_major_event_day":    0, "is_major_sports_event": 0,
            "is_barclays_event": 0, "is_msg_event": 0,
            "is_yankees_game": 0,   "is_mets_game": 0,
            "is_knicks_game": 0,    "is_nets_game": 0,
            "is_rangers_game": 0,   "is_nba_game": 0,
            "is_mlb_game": 0,       "is_nhl_game": 0,
            "is_us_open_event": 0,  "is_major_concert": 0,
            "event_count": 0,       "days_to_next_event": 30,
        }
        # STR features for ADR model
        for c in str_cols_in_ready:
            v = float(str_avg.loc[month, c]) if month in str_avg.index else 0.0
            row[c] = v * g_adr if "adr" in c else v
        for c in str_zero:
            row[c] = 0.0

        rows.append(row)

    fdf = pd.DataFrame(rows)

    X_rev = fdf[rev_features].astype(float)
    X_adr = fdf[adr_features].astype(float)

    fdf["pred_revenue"]   = m_rev.predict(X_rev)
    fdf["pred_occupancy"] = m_occ.predict(X_rev)
    fdf["pred_adr"]       = m_adr.predict(X_adr)
    fdf["pred_revpar"]    = fdf["pred_revenue"] / 147

    return fdf[["business_date", "pred_revenue", "pred_occupancy", "pred_adr", "pred_revpar"]]
