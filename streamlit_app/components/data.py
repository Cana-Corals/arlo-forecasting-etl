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


_CHANNEL_MAP = {
    "EXTRA":   "OTA",
    "EBL":     "OTA",
    "GTA":     "OTA",
    "GLOB":    "OTA",
    "WEBSITE": "Direct / Web",
    "WEBBOOK": "Direct / Web",
    "MOBILE":  "Direct / Web",
    "FD":      "Direct / Web",
    "GDSS":    "GDS / Corporate",
    "EXEC":    "GDS / Corporate",
    "SLS":     "GDS / Corporate",
    "CALL":    "Voice / Other",
    "PHONE":   "Voice / Other",
    "WHLS":    "Voice / Other",
    "INP":     "Voice / Other",
    "EVENTS":  "Voice / Other",
}

@st.cache_data
def load_source_stats() -> pd.DataFrame:
    path = PROCESSED_DIR / "daily_stats_source.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["business_date"])
    df["channel"] = df["source_code"].map(_CHANNEL_MAP).fillna("Voice / Other")
    return df.sort_values("business_date").reset_index(drop=True)


@st.cache_data
def load_room_type_stats() -> pd.DataFrame:
    path = PROCESSED_DIR / "daily_stats_room_type.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["business_date"])
    return df.sort_values(["business_date", "room_type"]).reset_index(drop=True)


MARKET_NAMES = {
    "OTA": "OTA",        "SP": "Special / Pkg",
    "CO":  "Corporate",  "HSE": "Hotel Sales",
    "GC":  "Gov / Corp", "NG":  "Negotiated",
    "RA":  "Rack / Retail", "GS": "Group & Social",
    "PK":  "Packages",   "CS":  "Consortia",
    "GE":  "Gov / Ext",  "WH":  "Wholesale",
}

@st.cache_data
def load_market_stats() -> pd.DataFrame:
    path = PROCESSED_DIR / "daily_stats_market.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["business_date"])
    df["segment"] = df["market_code"].map(MARKET_NAMES).fillna(df["market_code"])
    return df.sort_values("business_date").reset_index(drop=True)


@st.cache_data
def load_str() -> pd.DataFrame:
    path = PROCESSED_DIR / "str_daily.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["business_date"])
    return df.sort_values("business_date").reset_index(drop=True)
