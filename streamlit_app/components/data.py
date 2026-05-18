import pandas as pd
from pathlib import Path
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
FINAL_DIR    = BASE_DIR / "data" / "final"
OUTPUTS_DIR  = BASE_DIR / "outputs"
PROCESSED_DIR = BASE_DIR / "data" / "processed"


@st.cache_data
def load_master() -> pd.DataFrame:
    df = pd.read_csv(FINAL_DIR / "hotel_daily_master.csv", parse_dates=["business_date"])
    return df.sort_values("business_date").reset_index(drop=True)


@st.cache_data
def load_predictions() -> pd.DataFrame:
    df = pd.read_csv(OUTPUTS_DIR / "model_predictions_with_events.csv", parse_dates=["business_date"])
    return df.sort_values("business_date").reset_index(drop=True)


@st.cache_data
def load_feature_importance() -> pd.DataFrame:
    return pd.read_csv(OUTPUTS_DIR / "feature_importance_with_events.csv")


@st.cache_data
def load_events() -> pd.DataFrame:
    df = pd.read_csv(PROCESSED_DIR / "nyc_events_daily.csv", parse_dates=["business_date"])
    return df.sort_values("business_date").reset_index(drop=True)


@st.cache_data
def load_str() -> pd.DataFrame:
    path = PROCESSED_DIR / "str_daily.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["business_date"])
    return df.sort_values("business_date").reset_index(drop=True)
