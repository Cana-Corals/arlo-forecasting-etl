"""
Train LightGBM regression models for hotel revenue forecasting.

Three targets:
  - room_revenue     (primary)
  - occupancy_rate
  - adr

Temporal split:
  - Train fit  : 2024-01-01 – 2025-09-30  (639 days)
  - Val (ES)   : 2025-10-01 – 2025-10-31  ( 31 days)  used for early stopping only
  - Test        : 2025-11-01 – 2025-12-31  ( 61 days)

Features are restricted to information known before arrival day (no same-day actuals).
LightGBM handles NaN natively, so lag-364 nulls in the 2024 train set are fine.

STR feature strategy (optimal — validated by three-way comparison):
  - ADR model      : all STR features — comp rates are a direct causal driver of pricing
  - Revenue model  : no STR features — baseline outperformed all STR variants
  - Occupancy model: no STR features — near-zero STR importance confirmed across all experiments

Outputs:
  models/lgbm_{target}.txt             LightGBM booster (text format, portable)
  outputs/model_predictions_optimal.csv    Date, actuals, predictions for all 3 targets
  outputs/feature_importance_optimal.csv   Gain-based feature importance for each model
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from app.config import FINAL_DIR, OUTPUTS_DIR

MODELS_DIR = Path(__file__).resolve().parents[1] / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Feature sets — per-model, no same-day actuals, no targets, no identifiers
# ---------------------------------------------------------------------------

BASE_FEATURES = [
    # Capacity (known from maintenance schedule)
    "ooo_rooms", "available_rooms",
    # Calendar
    "day_of_week", "month", "quarter", "week_of_year", "is_weekend",
    # Pricing
    "retail_rate",
    # Holidays
    "is_federal_holiday", "days_to_next_holiday", "days_from_last_holiday",
    # Weather
    "temp_mean_f", "temp_max_f", "temp_min_f",
    "precipitation_in", "had_precipitation",
    "snowfall_in", "had_snow",
    "windspeed_max_mph", "weathercode",
    # Guest satisfaction (weekly, lagged)
    "medallia_sample_size", "medallia_overall_satisfaction",
    "medallia_likelihood_to_recommend", "medallia_likelihood_to_return",
    "medallia_value_for_price", "medallia_hotel_cleanliness",
    # Booking pace
    "pickup_7d", "pickup_14d", "pickup_21d", "pickup_30d", "pickup_60d",
    "total_rooms_on_books", "avg_booked_rate",
    # Lag & rolling features
    "occ_lag_7d",  "occ_lag_14d",  "occ_lag_28d",  "occ_lag_364d",
    "occ_roll_7d", "occ_roll_28d",
    "rev_lag_7d",  "rev_lag_14d",  "rev_lag_28d",  "rev_lag_364d",
    "rev_roll_7d", "rev_roll_28d",
    "adr_lag_7d",  "adr_lag_14d",  "adr_lag_28d",  "adr_lag_364d",
    "adr_roll_7d", "adr_roll_28d",
]

# Index-based STR features: measure Arlo's position relative to the market.
# Not derivable from Arlo's own data alone — genuinely new signal.
STR_INDEX_FEATURES = [
    "mpi_lag_7d",  "mpi_lag_28d",  "mpi_roll_7d",  "mpi_roll_28d",
    "ari_lag_7d",  "ari_lag_28d",  "ari_roll_7d",  "ari_roll_28d",
    "rgi_lag_7d",  "rgi_lag_28d",  "rgi_roll_7d",  "rgi_roll_28d",
    "adr_gap_vs_comp_lag_7d",  "adr_gap_vs_comp_lag_28d",
    "adr_gap_vs_comp_roll_7d", "adr_gap_vs_comp_roll_28d",
]

# Raw comp set values: correlated with Arlo's own lags — adds redundancy.
# Kept only for ADR model where comp pricing is a direct causal driver.
STR_RAW_FEATURES = [
    "comp_occ_lag_7d",    "comp_occ_lag_28d",    "comp_occ_roll_7d",    "comp_occ_roll_28d",
    "comp_adr_lag_7d",    "comp_adr_lag_28d",    "comp_adr_roll_7d",    "comp_adr_roll_28d",
    "comp_revpar_lag_7d", "comp_revpar_lag_28d",  "comp_revpar_roll_7d", "comp_revpar_roll_28d",
]

FEATURES = {
    "revenue":   BASE_FEATURES,
    "occupancy": BASE_FEATURES,
    "adr":       BASE_FEATURES + STR_INDEX_FEATURES + STR_RAW_FEATURES,
}

TARGETS = {
    "revenue":   "target_room_revenue",
    "occupancy": "target_occupancy_rate",
    "adr":       "target_adr",
}

LGBM_PARAMS = {
    "objective":        "regression",
    "metric":           "rmse",
    "verbosity":        -1,
    "n_estimators":     2000,
    "learning_rate":    0.03,
    "num_leaves":       31,
    "min_child_samples": 15,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq":     5,
    "lambda_l1":        0.05,
    "lambda_l2":        0.05,
    "random_state":     42,
}


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def _mape(y_true, y_pred):
    mask = y_true != 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100


def eval_metrics(y_true, y_pred, label=""):
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae  = mean_absolute_error(y_true, y_pred)
    mape = _mape(np.array(y_true), np.array(y_pred))
    r2   = r2_score(y_true, y_pred)
    if label:
        print(f"    {label:<8}  RMSE={rmse:>10.2f}  MAE={mae:>10.2f}  MAPE={mape:>6.2f}%  R2={r2:.4f}")
    return dict(rmse=rmse, mae=mae, mape=mape, r2=r2)


# ---------------------------------------------------------------------------
# Train one model
# ---------------------------------------------------------------------------

def train_one(name: str, target_col: str, df: pd.DataFrame, features: list) -> tuple:
    train = df[df["split"] == "train"].copy()
    test  = df[df["split"] == "test"].copy()

    # Split training into fit + early-stopping validation (last 2 months of 2024)
    val_cutoff = pd.Timestamp("2025-10-01")
    fit = train[train["business_date"] < val_cutoff]
    val = train[train["business_date"] >= val_cutoff]

    X_fit = fit[features]
    y_fit = fit[target_col]
    X_val = val[features]
    y_val = val[target_col]
    X_test = test[features]
    y_test  = test[target_col]

    model = lgb.LGBMRegressor(**LGBM_PARAMS)
    model.fit(
        X_fit, y_fit,
        eval_set=[(X_val, y_val)],
        callbacks=[
            lgb.early_stopping(stopping_rounds=50, verbose=False),
            lgb.log_evaluation(period=-1),
        ],
    )

    print(f"\n  [{name}]  features: {len(features)}  |  best iteration: {model.best_iteration_}")
    eval_metrics(y_fit,            model.predict(X_fit),   label="train-fit")
    eval_metrics(y_val,            model.predict(X_val),   label="val (ES)")
    metrics = eval_metrics(y_test, model.predict(X_test),  label="TEST")

    return model, metrics


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Loading model-ready dataset...")
    df = pd.read_csv(FINAL_DIR / "hotel_model_ready.csv", parse_dates=["business_date"])
    df = df.sort_values("business_date").reset_index(drop=True)

    # Verify all features are present
    all_features = set(BASE_FEATURES + STR_INDEX_FEATURES + STR_RAW_FEATURES)
    missing = [f for f in all_features if f not in df.columns]
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")

    print(f"  Rows: {len(df):,}  |  Train: {(df['split']=='train').sum()}  |  Test: {(df['split']=='test').sum()}")
    print()

    models      = {}
    all_metrics = {}
    importance_frames = []

    for name, target_col in TARGETS.items():
        features = FEATURES[name]
        print(f"Training {name} model  ({len(features)} features)...")
        model, metrics = train_one(name, target_col, df, features)
        models[name] = model
        all_metrics[name] = metrics

        # Save model
        out = MODELS_DIR / f"lgbm_{name}.txt"
        model.booster_.save_model(str(out))
        print(f"  Saved: {out}")

        # Feature importance
        imp = pd.DataFrame({
            "feature":    features,
            "importance": model.booster_.feature_importance(importance_type="gain"),
            "model":      name,
        }).sort_values("importance", ascending=False)
        importance_frames.append(imp)

    # ---------------------------------------------------------------------------
    # Predictions CSV
    # ---------------------------------------------------------------------------
    pred_df = df[["business_date", "split"]].copy()
    for name, target_col in TARGETS.items():
        features = FEATURES[name]
        pred_df[f"actual_{name}"]    = df[target_col].values
        pred_df[f"predicted_{name}"] = models[name].predict(df[features]).round(4)

    out_pred = OUTPUTS_DIR / "model_predictions_optimal.csv"
    pred_df.to_csv(out_pred, index=False)
    print(f"\n  Predictions saved: {out_pred}")

    # ---------------------------------------------------------------------------
    # Feature importance CSV
    # ---------------------------------------------------------------------------
    fi = pd.concat(importance_frames, ignore_index=True)
    fi = fi.pivot_table(index="feature", columns="model", values="importance", aggfunc="sum")
    fi = fi.fillna(0).sort_values("revenue", ascending=False)
    fi.columns.name = None
    fi = fi.reset_index()
    out_fi = OUTPUTS_DIR / "feature_importance_optimal.csv"
    fi.to_csv(out_fi, index=False)
    print(f"  Feature importance saved: {out_fi}")

    # ---------------------------------------------------------------------------
    # Final summary
    # ---------------------------------------------------------------------------
    print()
    print("=" * 65)
    print("  TEST SET RESULTS (Nov–Dec 2025, 61 days)")
    print("=" * 65)
    print(f"  {'Model':<12} {'RMSE':>12} {'MAE':>10} {'MAPE':>8} {'R2':>8}")
    print(f"  {'-'*60}")
    units = {"revenue": "$", "occupancy": "", "adr": "$"}
    for name, m in all_metrics.items():
        u = units[name]
        print(f"  {name:<12} {u}{m['rmse']:>10.2f} {u}{m['mae']:>8.2f} {m['mape']:>7.2f}% {m['r2']:>8.4f}")
    print("=" * 65)

    print()
    print("Top 10 features by revenue model gain:")
    top10 = fi.nlargest(10, "revenue")[["feature", "revenue"]]
    for _, row in top10.iterrows():
        bar = "#" * int(row["revenue"] / fi["revenue"].max() * 30)
        print(f"  {row['feature']:<35} {bar}")


if __name__ == "__main__":
    main()
