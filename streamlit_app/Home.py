import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from pathlib import Path
from datetime import date, datetime
import sys
import re
import threading
import time
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))

# ── Page config — must be first ──────────────────────────────────────────────
st.set_page_config(
    page_title="Arlo Williamsburg Forecasting",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Shared CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    #MainMenu, footer, header { visibility: hidden; }
    [data-testid="collapsedControl"] { display: none; }
    .stApp { background: #111111 !important; }
    [data-testid="stAppViewContainer"] { background: #111111 !important; }
    .block-container { padding-top: 0 !important; padding-bottom: 0 !important; max-width: 1400px !important; }

    .arlo-title { font-size: 1.6rem; font-weight: 600; color: #f5f5f0; margin-bottom: 0.15rem; }
    .arlo-date  { font-size: 0.85rem; color: rgba(245,245,240,0.4); margin-bottom: 1rem; }
    .search-hint { font-size: 0.72rem; color: rgba(245,245,240,0.3); text-align: center; margin-top: 0.3rem; }

    div[data-testid="stTextInput"] input {
        background: #1a1a1a !important;
        border: 1px solid rgba(255,255,255,0.12) !important;
        border-radius: 8px !important;
        color: #f5f5f0 !important;
        font-size: 0.95rem !important;
        padding: 12px 16px !important;
    }
    div[data-testid="stTextInput"] input:focus {
        border-color: rgba(232,133,74,0.5) !important;
        box-shadow: 0 0 0 2px rgba(232,133,74,0.1) !important;
    }
    button[kind="primaryFormSubmit"] { display: none !important; }

    .chat-q {
        font-size: 0.75rem; font-weight: 600; color: rgba(232,133,74,0.85);
        text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 6px;
    }
    .chat-answer {
        background: #1c1c1c; border: 1px solid rgba(255,255,255,0.07);
        border-radius: 8px; padding: 12px 16px; margin-bottom: 14px;
        color: rgba(245,245,240,0.82); font-size: 0.87rem; line-height: 1.65;
    }
    .chat-answer p  { margin: 0 0 6px 0; }
    .chat-answer ul, .chat-answer ol { margin: 4px 0 6px 0; padding-left: 18px; }
    .chat-answer strong { color: #f5f5f0; }
    .chat-empty {
        text-align: center; padding-top: 80px;
        color: rgba(245,245,240,0.18); font-size: 0.82rem; line-height: 2;
    }
</style>
""", unsafe_allow_html=True)

# ── Auth config ───────────────────────────────────────────────────────────────
def load_auth_config() -> dict:
    if "credentials" in st.secrets:
        credentials = {"usernames": {}}
        for username, data in st.secrets["credentials"]["usernames"].items():
            credentials["usernames"][username] = dict(data)
        return {
            "credentials": credentials,
            "cookie": {
                "name":        st.secrets["cookie"]["name"],
                "key":         st.secrets["cookie"]["key"],
                "expiry_days": st.secrets["cookie"]["expiry_days"],
            },
        }
    config_path = BASE / "config" / "users.yaml"
    with open(config_path) as f:
        return yaml.load(f, Loader=SafeLoader)


config        = load_auth_config()
authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
)
st.session_state["_auth"] = authenticator

# ── Dark chart theme ──────────────────────────────────────────────────────────
DARK_LAYOUT = dict(
    paper_bgcolor="#1a1a1a",
    plot_bgcolor="#1a1a1a",
    font=dict(color="rgba(245,245,240,0.7)", family="Inter, system-ui, sans-serif", size=11),
    title=dict(font=dict(size=14)),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
    margin=dict(l=10, r=10, t=36, b=10),
    hoverlabel=dict(bgcolor="#222222", bordercolor="rgba(255,255,255,0.15)", font_color="#f5f5f0"),
)

def _apply_dark(fig: go.Figure) -> go.Figure:
    """Apply dark theme to any Plotly figure without conflicting with user axis kwargs."""
    fig.update_layout(**DARK_LAYOUT)
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.06)", linecolor="rgba(255,255,255,0.1)", zeroline=False)
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.06)", linecolor="rgba(255,255,255,0.1)", zeroline=False)
    return fig
ACCENT = "#e8854a"
GREEN  = "#3ecf8e"
RED    = "#e05252"
SLATE  = "#4a6fa5"

# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _load_master() -> pd.DataFrame:
    return pd.read_csv(BASE / "data" / "final" / "hotel_daily_master.csv", parse_dates=["business_date"])

@st.cache_data(ttl=3600, show_spinner=False)
def _load_forecast() -> pd.DataFrame:
    try:
        from components.forecast_engine import generate_future_predictions
        return generate_future_predictions(2026)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def _load_room_types() -> pd.DataFrame:
    return pd.read_csv(BASE / "data" / "processed" / "daily_stats_room_type.csv", parse_dates=["business_date"])

@st.cache_data(ttl=3600, show_spinner=False)
def _load_ready() -> pd.DataFrame:
    return pd.read_csv(BASE / "data" / "final" / "hotel_model_ready.csv", parse_dates=["business_date"])

@st.cache_data(ttl=3600, show_spinner=False)
def _build_hotel_context() -> str:
    """Compact single-line-per-month format. ~500 tokens vs ~3600 previously."""
    master = _load_master()
    mn  = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    out = ["Arlo Williamsburg | 147 rooms | Brooklyn NY\n"]

    for year in [2024, 2025]:
        df_y = master[master["business_date"].dt.year == year]
        if df_y.empty:
            continue
        out.append(
            f"{year}: Rev ${df_y['room_revenue'].sum()/1e6:.2f}M | "
            f"Occ {df_y['occupancy_rate'].mean()*100:.1f}% | "
            f"ADR ${df_y['adr'].mean():.0f} | RevPAR ${df_y['revpar'].mean():.0f}\n"
        )
        agg = df_y.groupby(df_y["business_date"].dt.month).agg(
            occ=("occupancy_rate","mean"), adr=("adr","mean"), rev=("room_revenue","sum")
        )
        out.append(f"{year} by month: " + " | ".join(
            f"{mn[m-1]} {r['occ']*100:.0f}%/${r['adr']:.0f}/${r['rev']/1e3:.0f}K"
            for m, r in agg.iterrows()
        ) + "\n")

    df24 = master[master["business_date"].dt.year == 2024]
    df25 = master[master["business_date"].dt.year == 2025]
    if not df24.empty and not df25.empty:
        out.append(
            f"YoY: Rev {(df25['room_revenue'].sum()/df24['room_revenue'].sum()-1)*100:+.1f}% | "
            f"Occ {(df25['occupancy_rate'].mean()-df24['occupancy_rate'].mean())*100:+.1f}pp | "
            f"ADR {(df25['adr'].mean()/df24['adr'].mean()-1)*100:+.1f}%\n"
        )
    try:
        fc = _load_forecast()
        if not fc.empty:
            out.append(
                f"2026 forecast: Rev ${fc['pred_revenue'].sum()/1e6:.2f}M | "
                f"Occ {fc['pred_occupancy'].mean()*100:.1f}% | ADR ${fc['pred_adr'].mean():.0f}\n"
            )
            fc_m = fc.groupby(fc["business_date"].dt.month).agg(
                occ=("pred_occupancy","mean"), adr=("pred_adr","mean"), rev=("pred_revenue","sum")
            )
            out.append("2026 by month: " + " | ".join(
                f"{mn[m-1]} {r['occ']*100:.0f}%/${r['adr']:.0f}/${r['rev']/1e3:.0f}K"
                for m, r in fc_m.iterrows()
            ) + "\n")
    except Exception:
        pass
    return "".join(out)

# ── Pre-built chart catalog ───────────────────────────────────────────────────
_MN = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

def _chart_revenue_monthly() -> go.Figure:
    master = _load_master()
    fig = go.Figure()
    for year, color in [(2024, SLATE), (2025, ACCENT)]:
        df = master[master["business_date"].dt.year == year]
        m  = df.groupby(df["business_date"].dt.month)["room_revenue"].sum()
        fig.add_trace(go.Bar(name=str(year), x=[_MN[i-1] for i in m.index], y=m.values, marker_color=color))
    fig.update_layout(barmode="group")
    _apply_dark(fig)
    fig.update_layout(title=dict(text="Monthly Revenue 2024 vs 2025", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_adr_trend() -> go.Figure:
    master = _load_master()
    fig = go.Figure()
    for year, color in [(2024, SLATE), (2025, ACCENT)]:
        df = master[master["business_date"].dt.year == year].sort_values("business_date")
        m  = df.groupby(df["business_date"].dt.month)["adr"].mean()
        fig.add_trace(go.Scatter(name=str(year), x=[_MN[i-1] for i in m.index], y=m.values,
                                 mode="lines+markers", line=dict(color=color, width=2)))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="Monthly ADR 2024 vs 2025", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_occupancy_monthly() -> go.Figure:
    master = _load_master()
    fig = go.Figure()
    for year, color in [(2024, SLATE), (2025, ACCENT)]:
        df = master[master["business_date"].dt.year == year]
        m  = df.groupby(df["business_date"].dt.month)["occupancy_rate"].mean() * 100
        fig.add_trace(go.Scatter(name=str(year), x=[_MN[i-1] for i in m.index], y=m.values,
                                 mode="lines+markers", line=dict(color=color, width=2)))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="Monthly Occupancy % 2024 vs 2025", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_revpar_monthly() -> go.Figure:
    master = _load_master()
    fig = go.Figure()
    for year, color in [(2024, SLATE), (2025, ACCENT)]:
        df = master[master["business_date"].dt.year == year]
        m  = df.groupby(df["business_date"].dt.month)["revpar"].mean()
        fig.add_trace(go.Bar(name=str(year), x=[_MN[i-1] for i in m.index], y=m.values, marker_color=color))
    fig.update_layout(barmode="group")
    _apply_dark(fig)
    fig.update_layout(title=dict(text="Monthly RevPAR 2024 vs 2025", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_forecast_revenue() -> go.Figure:
    fc = _load_forecast()
    if fc.empty:
        return None
    m = fc.groupby(fc["business_date"].dt.month)["pred_revenue"].sum()
    fig = go.Figure(go.Bar(x=[_MN[i-1] for i in m.index], y=m.values, marker_color=ACCENT, name="2026 Forecast"))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="2026 Projected Revenue by Month", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_forecast_occupancy() -> go.Figure:
    fc = _load_forecast()
    if fc.empty:
        return None
    m = fc.groupby(fc["business_date"].dt.month)["pred_occupancy"].mean() * 100
    fig = go.Figure(go.Scatter(x=[_MN[i-1] for i in m.index], y=m.values,
                               mode="lines+markers", line=dict(color=GREEN, width=2), name="2026 Forecast"))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="2026 Projected Occupancy %", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_forecast_adr() -> go.Figure:
    fc = _load_forecast()
    if fc.empty:
        return None
    m = fc.groupby(fc["business_date"].dt.month)["pred_adr"].mean()
    fig = go.Figure(go.Scatter(x=[_MN[i-1] for i in m.index], y=m.values,
                               mode="lines+markers", line=dict(color=ACCENT, width=2), name="2026 Forecast"))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="2026 Projected ADR", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_adr_daily_2025() -> go.Figure:
    master = _load_master()
    df = master[master["business_date"].dt.year == 2025].sort_values("business_date")
    fig = go.Figure(go.Scatter(x=df["business_date"], y=df["adr"],
                               mode="lines", line=dict(color=ACCENT, width=1.5), name="ADR"))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="2025 Daily ADR Trend", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_occupancy_daily_2025() -> go.Figure:
    master = _load_master()
    df = master[master["business_date"].dt.year == 2025].sort_values("business_date")
    fig = go.Figure(go.Scatter(x=df["business_date"], y=df["occupancy_rate"]*100,
                               mode="lines", line=dict(color=GREEN, width=1.5), name="Occupancy %"))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="2025 Daily Occupancy Trend", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_revenue_daily_2025() -> go.Figure:
    master = _load_master()
    df = master[master["business_date"].dt.year == 2025].sort_values("business_date")
    fig = go.Figure(go.Scatter(x=df["business_date"], y=df["room_revenue"],
                               mode="lines", line=dict(color=ACCENT, width=1.5), name="Revenue"))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="2025 Daily Revenue Trend", font_color="rgba(245,245,240,0.9)"))
    return fig

# ── Room type charts ──────────────────────────────────────────────────────────
def _chart_room_revenue() -> go.Figure:
    rt  = _load_room_types()
    agg = rt.groupby("room_type")["room_revenue"].sum().sort_values()
    fig = go.Figure(go.Bar(x=agg.values, y=agg.index, orientation="h", marker_color=ACCENT))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="Total Revenue by Room Type (2024–2025)", font_color="rgba(245,245,240,0.9)"),
                      margin=dict(l=60, r=10, t=36, b=10))
    return fig

def _chart_room_ooo() -> go.Figure:
    rt  = _load_room_types()
    agg = rt.groupby("room_type")["ooo_rooms"].sum().sort_values()
    fig = go.Figure(go.Bar(x=agg.values, y=agg.index, orientation="h", marker_color=RED))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="Total OOO Nights by Room Type (2024–2025)", font_color="rgba(245,245,240,0.9)"),
                      margin=dict(l=60, r=10, t=36, b=10))
    return fig

def _chart_room_occupancy() -> go.Figure:
    rt  = _load_room_types()
    rt2 = rt.copy()
    rt2["avail"] = rt2["total_physical_rooms"] - rt2["ooo_rooms"]
    rt2["occ"]   = rt2.apply(lambda r: r["room_nights"] / r["avail"] if r["avail"] > 0 else None, axis=1)
    agg = rt2.groupby("room_type")["occ"].mean().dropna().sort_values() * 100
    fig = go.Figure(go.Bar(x=agg.values, y=agg.index, orientation="h", marker_color=GREEN))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="Average Occupancy % by Room Type (2024–2025)", font_color="rgba(245,245,240,0.9)"),
                      margin=dict(l=60, r=10, t=36, b=10))
    return fig

# ── Demand / booking pace charts ──────────────────────────────────────────────
def _chart_demand_pickup() -> go.Figure:
    ready = _load_ready()
    df = ready[ready["business_date"].dt.year == 2025].copy()
    df["month"] = df["business_date"].dt.month
    m = df.groupby("month")[["pickup_7d","pickup_14d","pickup_30d"]].mean()
    fig = go.Figure()
    for col, color, lbl in [("pickup_7d", GREEN, "7-day"), ("pickup_14d", SLATE, "14-day"), ("pickup_30d", ACCENT, "30-day")]:
        fig.add_trace(go.Scatter(x=[_MN[i-1] for i in m.index], y=m[col],
                                 mode="lines+markers", name=lbl, line=dict(color=color, width=2)))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="2025 Monthly Booking Pickup (rooms added)", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_demand_on_books() -> go.Figure:
    ready = _load_ready()
    df = ready[ready["business_date"].dt.year == 2025].copy()
    df["month"] = df["business_date"].dt.month
    m = df.groupby("month")["total_rooms_on_books"].mean()
    fig = go.Figure(go.Bar(x=[_MN[i-1] for i in m.index], y=m.values, marker_color=SLATE))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="2025 Avg Rooms on Books by Month", font_color="rgba(245,245,240,0.9)"))
    return fig

# ── Forecast vs actuals comparison ────────────────────────────────────────────
def _chart_forecast_vs_2025_occ() -> go.Figure:
    master = _load_master()
    fc     = _load_forecast()
    fig    = go.Figure()
    df25   = master[master["business_date"].dt.year == 2025]
    m25    = df25.groupby(df25["business_date"].dt.month)["occupancy_rate"].mean() * 100
    fig.add_trace(go.Scatter(x=[_MN[i-1] for i in m25.index], y=m25.values,
                             mode="lines+markers", name="2025 Actual", line=dict(color=SLATE, width=2)))
    if not fc.empty:
        mfc = fc.groupby(fc["business_date"].dt.month)["pred_occupancy"].mean() * 100
        fig.add_trace(go.Scatter(x=[_MN[i-1] for i in mfc.index], y=mfc.values,
                                 mode="lines+markers", name="2026 Forecast", line=dict(color=ACCENT, width=2, dash="dash")))
    _apply_dark(fig)
    fig.update_layout(title=dict(text="Occupancy: 2025 Actual vs 2026 Forecast", font_color="rgba(245,245,240,0.9)"))
    return fig

def _chart_forecast_vs_2025_rev() -> go.Figure:
    master = _load_master()
    fc     = _load_forecast()
    fig    = go.Figure()
    df25   = master[master["business_date"].dt.year == 2025]
    m25    = df25.groupby(df25["business_date"].dt.month)["room_revenue"].sum()
    fig.add_trace(go.Bar(x=[_MN[i-1] for i in m25.index], y=m25.values, name="2025 Actual", marker_color=SLATE))
    if not fc.empty:
        mfc = fc.groupby(fc["business_date"].dt.month)["pred_revenue"].sum()
        fig.add_trace(go.Bar(x=[_MN[i-1] for i in mfc.index], y=mfc.values, name="2026 Forecast", marker_color=ACCENT))
    fig.update_layout(barmode="group")
    _apply_dark(fig)
    fig.update_layout(title=dict(text="Revenue: 2025 Actual vs 2026 Forecast", font_color="rgba(245,245,240,0.9)"))
    return fig

_CHART_FNS = {
    # Performance
    "revenue_monthly":          _chart_revenue_monthly,
    "revenue_trend":            _chart_revenue_daily_2025,
    "adr_monthly":              _chart_adr_trend,
    "adr_trend":                _chart_adr_daily_2025,
    "occupancy_monthly":        _chart_occupancy_monthly,
    "occupancy_trend":          _chart_occupancy_daily_2025,
    "revpar_monthly":           _chart_revpar_monthly,
    # Room types
    "room_revenue":             _chart_room_revenue,
    "room_ooo":                 _chart_room_ooo,
    "room_occupancy":           _chart_room_occupancy,
    # Demand
    "demand_pickup":            _chart_demand_pickup,
    "demand_on_books":          _chart_demand_on_books,
    # Forecast
    "forecast_revenue":         _chart_forecast_revenue,
    "forecast_occupancy":       _chart_forecast_occupancy,
    "forecast_adr":             _chart_forecast_adr,
    "forecast_vs_2025_rev":     _chart_forecast_vs_2025_rev,
    "forecast_vs_2025_occ":     _chart_forecast_vs_2025_occ,
}

# ── Claude call ───────────────────────────────────────────────────────────────
_SYSTEM = """Hotel revenue analyst for Arlo Williamsburg (147 rooms, Brooklyn NY).

For CHART requests respond with exactly one line: CHART: [chart_name]
For TEXT requests respond with max 3 bullet points, max 12 words each, bold key numbers.
Never mix text and charts. Never output more than one chart name.

Available charts:
revenue_monthly — monthly revenue 2024 vs 2025
revenue_trend — daily revenue line 2025
adr_monthly — monthly ADR 2024 vs 2025
adr_trend — daily ADR line 2025
occupancy_monthly — monthly occupancy % 2024 vs 2025
occupancy_trend — daily occupancy line 2025
revpar_monthly — monthly RevPAR 2024 vs 2025
room_revenue — total revenue by room type (ranked)
room_ooo — total OOO nights by room type (ranked)
room_occupancy — average occupancy % by room type
demand_pickup — 2025 booking pickup 7d/14d/30d by month
demand_on_books — 2025 avg rooms on books by month
forecast_revenue — 2026 projected revenue by month
forecast_occupancy — 2026 projected occupancy
forecast_adr — 2026 projected ADR
forecast_vs_2025_rev — 2025 actual vs 2026 forecast revenue
forecast_vs_2025_occ — 2025 actual vs 2026 forecast occupancy"""

# Catalog shown to user in the UI
_CHART_CATALOG_DISPLAY = [
    ("📊 Performance", [
        ("revenue_monthly", "Monthly revenue 2024 vs 2025"),
        ("revenue_trend",   "Daily revenue trend 2025"),
        ("adr_monthly",     "Monthly ADR 2024 vs 2025"),
        ("adr_trend",       "Daily ADR trend 2025"),
        ("occupancy_monthly","Monthly occupancy % 2024 vs 2025"),
        ("occupancy_trend", "Daily occupancy trend 2025"),
        ("revpar_monthly",  "Monthly RevPAR 2024 vs 2025"),
    ]),
    ("🛏 Room Types", [
        ("room_revenue",    "Revenue by room type (ranked)"),
        ("room_ooo",        "OOO nights by room type (ranked)"),
        ("room_occupancy",  "Occupancy % by room type"),
    ]),
    ("📅 Demand", [
        ("demand_pickup",   "Booking pickup 7d/14d/30d by month"),
        ("demand_on_books", "Avg rooms on books by month"),
    ]),
    ("🔮 Forecast", [
        ("forecast_revenue",       "2026 projected revenue"),
        ("forecast_occupancy",     "2026 projected occupancy"),
        ("forecast_adr",           "2026 projected ADR"),
        ("forecast_vs_2025_rev",   "2025 actual vs 2026 forecast — revenue"),
        ("forecast_vs_2025_occ",   "2025 actual vs 2026 forecast — occupancy"),
    ]),
]

def _call_claude(question: str, context: str, history: list) -> str:
    import anthropic
    api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "⚠️ ANTHROPIC_API_KEY not set in Streamlit secrets."
    client = anthropic.Anthropic(api_key=api_key)
    messages = []
    for item in history[-2:]:
        messages.append({"role": "user",      "content": item["question"]})
        messages.append({"role": "assistant", "content": item["answer_raw"]})
    messages.append({"role": "user", "content": f"{question}\n\n---\n{context}"})
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=120,
        system=_SYSTEM,
        messages=messages,
    )
    return resp.content[0].text.strip()

# ── Parse response into segments ──────────────────────────────────────────────
def _parse_segments(response_text: str) -> list:
    segments = []
    # Check for CHART: directive
    m = re.search(r'CHART:\s*(\w+)', response_text, re.IGNORECASE)
    if m:
        chart_name = m.group(1).lower()
        fn = _CHART_FNS.get(chart_name)
        if fn:
            try:
                fig = fn()
                if fig is not None:
                    segments.append({"type": "chart", "content": fig.to_dict()})
                else:
                    segments.append({"type": "text", "content": "_Chart data not available._"})
            except Exception as e:
                segments.append({"type": "text", "content": f"_Chart error: {e}_"})
        else:
            segments.append({"type": "text", "content": f"_Unknown chart: {chart_name}_"})
    else:
        # Plain text answer
        text = response_text.strip()
        if text:
            segments.append({"type": "text", "content": text})
    return segments

# ── Render stored segments ────────────────────────────────────────────────────
def _render_item(item: dict):
    st.markdown(f'<div class="chat-q">{item["question"]}</div>', unsafe_allow_html=True)
    for seg in item.get("segments", []):
        if seg["type"] == "text":
            st.markdown(f'<div class="chat-answer">{seg["content"]}</div>', unsafe_allow_html=True)
        elif seg["type"] == "chart":
            try:
                st.plotly_chart(go.Figure(seg["content"]), use_container_width=True)
            except Exception:
                pass

# ── Login page ────────────────────────────────────────────────────────────────
def login_page():
    st.markdown("""
    <style>
        section[data-testid="stSidebar"] { display: none; }
        .block-container { padding-top: 2rem; }
        .arlo-title { text-align:center; font-size:2.4rem; font-weight:700; color:#f5f5f0; margin-bottom:0.2rem; }
        .confidential { text-align:center; font-size:0.75rem; color:rgba(245,245,240,0.3); letter-spacing:0.12em; margin-top:2rem; }
        div[data-testid="stForm"] { width:100% !important; }
        div[data-testid="stForm"] div[data-testid="stTextInput"],
        div[data-testid="stForm"] div[data-testid="stTextInput"] > div,
        div[data-testid="stForm"] div[data-testid="stTextInput"] input {
            width:100% !important; box-sizing:border-box !important;
        }
    </style>
    """, unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown('<div class="arlo-title">Arlo Williamsburg</div>', unsafe_allow_html=True)
        st.markdown(
            '<div style="text-align:center;font-size:0.85rem;color:#f5f5f0;margin-bottom:2rem;">Internal Intelligence Platform</div>',
            unsafe_allow_html=True,
        )
        authenticator.login(
            location="main",
            fields={"Form name": "", "Username": "Username", "Password": "Password", "Login": "Login"},
        )
        if st.session_state.get("authentication_status") is False:
            st.error("Incorrect username or password.")
        st.markdown('<div class="confidential">CONFIDENTIAL — INTERNAL USE ONLY</div>', unsafe_allow_html=True)


# ── Home page ─────────────────────────────────────────────────────────────────
def home_page():
    from components.nav import render_nav

    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    # ── Process a pending question before any rendering ───────────────────────
    pending_q = st.session_state.pop("pending_q", None)
    if pending_q:
        with st.spinner("Thinking..."):
            context = _build_hotel_context()
            raw     = _call_claude(pending_q, context, st.session_state["chat_history"])
        segments = _parse_segments(raw)
        st.session_state["chat_history"].append({
            "question":   pending_q,
            "answer_raw": raw,
            "segments":   segments,
        })
        st.rerun()  # Fresh render so chart appears inside the container, not mid-script

    name     = st.session_state.get("name", "")
    today    = date.today()
    date_str = f"{today.strftime('%A')}, {today.strftime('%B')} {today.day} · {today.year}"
    hour     = datetime.now().hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 18 else "Good evening"

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        greeting_name = f", {name.split()[0]}" if name.strip() else ""
        st.markdown(
            f'<div class="arlo-title" style="text-align:center;">{greeting}{greeting_name}.</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="arlo-date" style="text-align:center;">{date_str}</div>',
            unsafe_allow_html=True,
        )

        # Fixed-height scrollable chat box
        chat_box = st.container(height=420, border=False)
        with chat_box:
            if not st.session_state["chat_history"]:
                st.markdown(
                    '<div class="chat-empty">'
                    'Ask anything about the hotel<br>'
                    '"ADR trend 2025" · "Revenue monthly chart"<br>'
                    '"2026 forecast revenue" · "How is occupancy?"'
                    '</div>',
                    unsafe_allow_html=True,
                )
            for item in st.session_state["chat_history"]:
                _render_item(item)

        # Input always visible below the box
        with st.form("ai_form", clear_on_submit=True, border=False):
            question = st.text_input(
                label="",
                placeholder='Ask anything — "ADR trend 2025" or "How is occupancy tracking?"',
                label_visibility="collapsed",
                key="home_question",
            )
            submitted = st.form_submit_button("Ask", use_container_width=True, type="primary")

        st.markdown(
            '<div class="search-hint">Press Enter to ask · Powered by Claude AI</div>',
            unsafe_allow_html=True,
        )

        # Only trigger on actual form submission — not on reruns after processing
        if submitted and question.strip():
            st.session_state["pending_q"] = question.strip()
            st.rerun()

    render_nav()


# ── Navigation router ─────────────────────────────────────────────────────────
pg = st.navigation(
    [
        st.Page("pages/1_Dashboard.py",      title="Dashboard",      default=True),
        st.Page("pages/2_Forecast.py",       title="Forecast"),
        st.Page("pages/3_Performance.py",    title="Performance"),
        st.Page("pages/4_Demand.py",         title="Demand"),
        st.Page("pages/5_Competitive.py",    title="Competitive"),
        st.Page("pages/6_Model_Insights.py", title="Model Insights"),
        st.Page(home_page,                   title="AI Assistant",   url_path="ai"),
    ],
    position="hidden",
)

pg.run()
