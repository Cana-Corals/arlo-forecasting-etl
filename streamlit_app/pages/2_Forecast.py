import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import date, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.nav import render_nav

BASE = Path(__file__).resolve().parents[2]

# ── Page config ───────────────────────────────────────────────────────────────
st.markdown(
    '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@2.47.0/tabler-icons.min.css">',
    unsafe_allow_html=True,
)
st.markdown("""
<style>
:root {
  --dark:#111111; --dark2:#1a1a1a; --dark3:#222222;
  --border:rgba(255,255,255,0.08); --border2:rgba(255,255,255,0.14);
  --white:#f5f5f0; --muted:rgba(245,245,240,0.38); --muted2:rgba(245,245,240,0.6);
  --accent:#e8854a; --green:#3ecf8e; --red:#e05252; --slate:#4a6fa5;
}
*, *::before, *::after { font-family:'Inter',system-ui,sans-serif !important; -webkit-font-smoothing:antialiased; box-sizing:border-box; }
#MainMenu, footer, header { visibility:hidden; }
[data-testid="collapsedControl"] { display:none; }
section[data-testid="stSidebar"] { display:none; }
.stApp { background:var(--dark2) !important; }
.block-container { padding:0 !important; max-width:1300px !important; margin:0 auto !important; }
[data-testid="stAppViewContainer"] { background:var(--dark2) !important; }

/* topbar */
.fc-topbar {
  background:var(--dark); border-bottom:1px solid var(--border);
  height:52px; display:flex; align-items:center; padding:0 20px; gap:10px;
}
.fc-title { font-size:14px; font-weight:600; color:var(--white); }
.fc-sub   { font-size:11px; color:var(--muted); }
.fc-rule  { height:2px; background:linear-gradient(90deg,var(--accent) 0%,transparent 60%); margin-bottom:0; }

/* horizon bar */
.fc-horizon-wrap {
  background:var(--dark); border-bottom:1px solid var(--border);
  padding:0 20px; display:flex; align-items:center; gap:0; height:44px;
}

/* KPI cards — ALL in one markdown block so grid applies */
.fc-kpi-grid {
  display:grid; grid-template-columns:repeat(4,1fr); gap:12px;
  padding:18px 20px 6px;
}
.fc-kpi {
  background:var(--dark); border:1px solid var(--border); border-radius:6px;
  padding:14px 16px;
}
.fc-kpi-lbl  { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:6px; }
.fc-kpi-val  { font-size:22px; font-weight:600; color:var(--white); line-height:1; margin-bottom:4px; }
.fc-kpi-row  { display:flex; align-items:center; gap:8px; margin-top:6px; padding-top:6px; border-top:1px solid var(--border); }
.fc-kpi-py   { font-size:10px; color:var(--muted2); }
.fc-kpi-py span { font-weight:500; }
.fc-delta-up   { font-size:10px; font-weight:600; color:var(--green); }
.fc-delta-down { font-size:10px; font-weight:600; color:var(--red); }

/* section title */
.fc-section { padding:18px 20px 0; }
.fc-section-ttl { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:10px; }

/* custom date row */
.fc-custom-row { padding:8px 20px 0; display:flex; gap:12px; align-items:center; }
</style>
""", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_ml():
    df = pd.read_csv(BASE / "outputs" / "model_predictions_with_events.csv",
                     parse_dates=["business_date"])
    return df

@st.cache_data
def load_rt():
    df = pd.read_csv(BASE / "data" / "processed" / "daily_stats_room_type.csv",
                     parse_dates=["business_date"])
    return df

@st.cache_data
def load_src():
    df = pd.read_csv(BASE / "data" / "processed" / "daily_stats_source.csv",
                     parse_dates=["business_date"])
    return df

ml  = load_ml()
rt  = load_rt()
src = load_src()

# ── Horizon selector ──────────────────────────────────────────────────────────
st.markdown("""
<div class="fc-topbar">
  <span class="fc-title">Forecast</span>
  <span class="fc-sub">ML model · Nov–Dec 2025 true holdout</span>
</div>
<div class="fc-rule"></div>
""", unsafe_allow_html=True)

HORIZONS = ["1W", "1M", "Q1", "Q2", "Q3", "Q4", "Year", "Custom"]

if "fc_horizon" not in st.session_state:
    st.session_state["fc_horizon"] = "Q4"

_hz = st.segmented_control(
    "Horizon", HORIZONS,
    default=st.session_state["fc_horizon"],
    key="fc_hz_ctrl",
    label_visibility="collapsed",
)
if _hz:
    st.session_state["fc_horizon"] = _hz

horizon = st.session_state["fc_horizon"]

# Date bounds from data
DATA_MIN = date(2024, 1, 1)
DATA_MAX = date(2025, 12, 31)

def preset_dates(h):
    if h == "1W":    return date(2025, 12, 25), date(2025, 12, 31)
    if h == "1M":    return date(2025, 12, 1),  date(2025, 12, 31)
    if h == "Q1":    return date(2025, 1, 1),   date(2025, 3, 31)
    if h == "Q2":    return date(2025, 4, 1),   date(2025, 6, 30)
    if h == "Q3":    return date(2025, 7, 1),   date(2025, 9, 30)
    if h == "Q4":    return date(2025, 10, 1),  date(2025, 12, 31)
    if h == "Year":  return date(2025, 1, 1),   date(2025, 12, 31)
    return None, None  # Custom

if horizon == "Custom":
    _c1, _c2, _ = st.columns([2, 2, 5])
    with _c1:
        start = st.date_input("From", value=date(2025, 1, 1),
                              min_value=DATA_MIN, max_value=DATA_MAX,
                              key="fc_start", format="MM/DD/YYYY",
                              label_visibility="collapsed")
    with _c2:
        end = st.date_input("To", value=date(2025, 12, 31),
                            min_value=DATA_MIN, max_value=DATA_MAX,
                            key="fc_end", format="MM/DD/YYYY",
                            label_visibility="collapsed")
    if start > end:
        start, end = end, start
else:
    start, end = preset_dates(horizon)

# ── Filter data ───────────────────────────────────────────────────────────────
s_ts = pd.Timestamp(start)
e_ts = pd.Timestamp(end)

df = ml[(ml["business_date"] >= s_ts) & (ml["business_date"] <= e_ts)].copy()

try:
    s_py = date(start.year - 1, start.month, start.day)
    e_py = date(end.year - 1, end.month, end.day)
except ValueError:
    s_py = start.replace(day=28) - timedelta(days=365)
    e_py = end.replace(day=28)   - timedelta(days=365)

df_py = ml[
    (ml["business_date"] >= pd.Timestamp(s_py)) &
    (ml["business_date"] <= pd.Timestamp(e_py))
].copy()

# ── Auto-granularity ──────────────────────────────────────────────────────────
n_days = (end - start).days + 1
freq      = "D" if n_days <= 14 else ("W" if n_days <= 60 else "ME")
date_fmt  = "%b %d"  if n_days <= 14 else ("%b %d" if n_days <= 60 else "%b")

def agg(d, val_col, agg_fn):
    d = d.copy()
    d["period"] = d["business_date"].dt.to_period("D" if freq == "D" else ("W" if freq == "W" else "M"))
    return d.groupby("period")[val_col].agg(agg_fn).reset_index()

# ── KPI calculations ──────────────────────────────────────────────────────────
def safe_div(a, b):
    return a / b if b and b != 0 else 0.0

if df.empty:
    rev_fc = occ_fc = adr_fc = rp_fc = 0.0
    rev_py_v = occ_py_v = 0.0
else:
    rev_fc    = df["predicted_revenue"].sum()
    occ_fc    = df["predicted_occupancy"].mean() * 100
    adr_fc    = df["predicted_adr"].mean()
    rp_fc     = safe_div(df["predicted_revenue"].sum(), 147 * len(df))
    rev_py_v  = df_py["actual_revenue"].sum()   if not df_py.empty else 0.0
    occ_py_v  = df_py["actual_occupancy"].mean() * 100 if not df_py.empty else 0.0
    adr_py_v  = df_py["actual_adr"].mean()      if not df_py.empty else 0.0
    rp_py_v   = safe_div(df_py["actual_revenue"].sum(), 147 * len(df_py)) if not df_py.empty else 0.0

def delta_cls(v, base):
    d = v - base
    pct = (d / abs(base) * 100) if base else 0
    arrow = "▲" if d >= 0 else "▼"
    cls   = "fc-delta-up" if d >= 0 else "fc-delta-down"
    return cls, f"{arrow} {abs(pct):.1f}%"

rev_cls,  rev_dstr  = delta_cls(rev_fc,  rev_py_v)
occ_cls,  occ_dstr  = delta_cls(occ_fc,  occ_py_v)
adr_cls,  adr_dstr  = delta_cls(adr_fc,  adr_py_v  if not df_py.empty else adr_fc)
rp_cls,   rp_dstr   = delta_cls(rp_fc,   rp_py_v)

period_lbl = {
    "1W": "last 7 days", "1M": "last 30 days",
    "Q1": "Q1 2025", "Q2": "Q2 2025", "Q3": "Q3 2025", "Q4": "Q4 2025",
    "Year": "Full Year 2025", "Custom": f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}",
}.get(horizon, "")

# ── KPI cards (single markdown block) ────────────────────────────────────────
st.markdown(f"""
<div class="fc-kpi-grid">
  <div class="fc-kpi">
    <div class="fc-kpi-lbl">Revenue Forecast</div>
    <div class="fc-kpi-val">${rev_fc/1e3:,.0f}k</div>
    <div class="fc-kpi-row">
      <span class="{rev_cls}">{rev_dstr}</span>
      <span class="fc-kpi-py">vs <span>${rev_py_v/1e3:,.0f}k</span> prior year</span>
    </div>
  </div>
  <div class="fc-kpi">
    <div class="fc-kpi-lbl">Occupancy Forecast</div>
    <div class="fc-kpi-val">{occ_fc:.1f}%</div>
    <div class="fc-kpi-row">
      <span class="{occ_cls}">{occ_dstr}</span>
      <span class="fc-kpi-py">vs <span>{occ_py_v:.1f}%</span> prior year</span>
    </div>
  </div>
  <div class="fc-kpi">
    <div class="fc-kpi-lbl">ADR Forecast</div>
    <div class="fc-kpi-val">${adr_fc:,.0f}</div>
    <div class="fc-kpi-row">
      <span class="{adr_cls}">{adr_dstr}</span>
      <span class="fc-kpi-py">vs <span>${adr_py_v if not df_py.empty else 0:,.0f}</span> prior year</span>
    </div>
  </div>
  <div class="fc-kpi">
    <div class="fc-kpi-lbl">RevPAR Forecast</div>
    <div class="fc-kpi-val">${rp_fc:,.0f}</div>
    <div class="fc-kpi-row">
      <span class="{rp_cls}">{rp_dstr}</span>
      <span class="fc-kpi-py">vs <span>${rp_py_v:,.0f}</span> prior year</span>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Plotly theme helpers ──────────────────────────────────────────────────────
BG     = "#1a1a1a"
GRID   = "rgba(255,255,255,0.05)"
FONT   = "rgba(245,245,240,0.5)"
ACCENT = "#e8854a"
ACCENT_F = "rgba(232,133,74,0.25)"
GREEN  = "#3ecf8e"
GREEN_F  = "rgba(62,207,142,0.22)"

def base_layout(h=280):
    return dict(
        paper_bgcolor=BG, plot_bgcolor=BG,
        font=dict(family="Inter", color=FONT, size=10),
        margin=dict(l=4, r=4, t=28, b=4),
        height=h,
        legend=dict(
            bgcolor="rgba(0,0,0,0)", borderwidth=0,
            orientation="h", x=0, y=1.14,
            font=dict(size=10, color="rgba(245,245,240,0.6)"),
        ),
        xaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)", tickfont=dict(size=9), tickangle=-30),
        yaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)", tickfont=dict(size=9), zeroline=False),
        hovermode="x unified",
        bargap=0.18, bargroupgap=0.06,
    )

def period_x_labels(periods):
    out = []
    for p in periods:
        try:
            ts = p.start_time
            out.append(ts.strftime(date_fmt))
        except Exception:
            out.append(str(p))
    return out

# ── Revenue chart ─────────────────────────────────────────────────────────────
st.markdown('<div class="fc-section"><div class="fc-section-ttl">Revenue — Actual vs Forecast</div></div>', unsafe_allow_html=True)

if df.empty:
    st.info("No data for the selected period.")
else:
    rev_agg  = agg(df, "actual_revenue",    "sum")
    frev_agg = agg(df, "predicted_revenue", "sum")

    xlabels = period_x_labels(rev_agg["period"])

    fig_rev = go.Figure()
    fig_rev.add_trace(go.Bar(
        x=xlabels, y=rev_agg["actual_revenue"],
        name="Actual", marker_color=ACCENT,
        hovertemplate="Actual: $%{y:,.0f}<extra></extra>",
    ))
    fig_rev.add_trace(go.Bar(
        x=xlabels, y=frev_agg["predicted_revenue"],
        name="Forecast", marker_color=ACCENT_F,
        marker_line=dict(color=ACCENT, width=1),
        hovertemplate="Forecast: $%{y:,.0f}<extra></extra>",
    ))
    fig_rev.update_layout(**base_layout())
    fig_rev.update_yaxes(tickprefix="$", tickformat=",.0f")
    st.plotly_chart(fig_rev, use_container_width=True, config={"displayModeBar": False})

# ── Occupancy chart ───────────────────────────────────────────────────────────
st.markdown('<div class="fc-section"><div class="fc-section-ttl">Occupancy — Actual vs Forecast</div></div>', unsafe_allow_html=True)

if not df.empty:
    occ_agg  = agg(df, "actual_occupancy",    "mean")
    focc_agg = agg(df, "predicted_occupancy", "mean")

    xlabels_occ = period_x_labels(occ_agg["period"])

    fig_occ = go.Figure()
    fig_occ.add_trace(go.Bar(
        x=xlabels_occ, y=occ_agg["actual_occupancy"] * 100,
        name="Actual", marker_color=GREEN,
        hovertemplate="Actual: %{y:.1f}%<extra></extra>",
    ))
    fig_occ.add_trace(go.Bar(
        x=xlabels_occ, y=focc_agg["predicted_occupancy"] * 100,
        name="Forecast", marker_color=GREEN_F,
        marker_line=dict(color=GREEN, width=1),
        hovertemplate="Forecast: %{y:.1f}%<extra></extra>",
    ))
    fig_occ.update_layout(**base_layout())
    fig_occ.update_yaxes(ticksuffix="%", range=[0, 105])
    st.plotly_chart(fig_occ, use_container_width=True, config={"displayModeBar": False})

# ── Room type mix ─────────────────────────────────────────────────────────────
st.markdown('<div class="fc-section"><div class="fc-section-ttl">Room Type — Nights Sold</div></div>', unsafe_allow_html=True)

rt_period = rt[(rt["business_date"] >= s_ts) & (rt["business_date"] <= e_ts)]
rt_py_period = rt[(rt["business_date"] >= pd.Timestamp(s_py)) & (rt["business_date"] <= pd.Timestamp(e_py))]

if rt_period.empty:
    st.info("No room type data for this period.")
else:
    rt_cur = rt_period.groupby("room_type").agg(
        room_nights=("room_nights", "sum"),
        revenue=("room_revenue", "sum"),
    ).sort_values("room_nights", ascending=True).reset_index()

    rt_pyr = rt_py_period.groupby("room_type").agg(
        room_nights=("room_nights", "sum"),
    ).reset_index().rename(columns={"room_nights": "rn_py"})

    rt_cur = rt_cur.merge(rt_pyr, on="room_type", how="left").fillna(0)

    fig_rt = go.Figure()
    fig_rt.add_trace(go.Bar(
        y=rt_cur["room_type"], x=rt_cur["room_nights"],
        orientation="h", name=f"{start.year}",
        marker_color=ACCENT,
        hovertemplate="%{y}: %{x:,.0f} nights<extra></extra>",
    ))
    fig_rt.add_trace(go.Bar(
        y=rt_cur["room_type"], x=rt_cur["rn_py"],
        orientation="h", name=f"{start.year - 1} (same period)",
        marker_color=ACCENT_F,
        marker_line=dict(color=ACCENT, width=1),
        hovertemplate="%{y}: %{x:,.0f} nights (PY)<extra></extra>",
    ))
    rt_layout = base_layout(h=320)
    rt_layout["barmode"] = "group"
    rt_layout["xaxis"]["title"] = "Room Nights"
    rt_layout["margin"]["l"] = 50
    fig_rt.update_layout(**rt_layout)
    st.plotly_chart(fig_rt, use_container_width=True, config={"displayModeBar": False})

# ── OTA / Source mix ──────────────────────────────────────────────────────────
st.markdown('<div class="fc-section"><div class="fc-section-ttl">Booking Source — Revenue by Channel</div></div>', unsafe_allow_html=True)

src_period = src[(src["business_date"] >= s_ts) & (src["business_date"] <= e_ts)]
src_py_period = src[(src["business_date"] >= pd.Timestamp(s_py)) & (src["business_date"] <= pd.Timestamp(e_py))]

SOURCE_NAMES = {
    "EXTRA":     "Extranet / OTA",
    "WEBSITE":   "Direct – Website",
    "SLS":       "Sales / Groups",
    "EBL":       "EBL",
    "INP":       "In-Person / Walk-in",
    "WEBBOOK":   "Web Booking Engine",
    "GDSS":      "GDS / Travel Agents",
    "FD":        "Front Desk",
    "EXPDC":     "Expedia Direct Connect",
    "BOOKINGDC": "Booking.com",
    "MOBILE":    "Mobile App",
    "GLOB":      "Global Distribution",
    "MOBILEWEB": "Mobile Web",
    "HOT2N":     "Hotel Tonight",
}

if src_period.empty:
    st.info("No source data for this period.")
else:
    src_cur = src_period.groupby("source_code")["total_revenue"].sum().reset_index()
    src_cur["label"] = src_cur["source_code"].map(lambda x: SOURCE_NAMES.get(x, x))
    src_cur = src_cur.nlargest(10, "total_revenue").sort_values("total_revenue")

    src_pyr = src_py_period.groupby("source_code")["total_revenue"].sum().reset_index()
    src_pyr = src_pyr.rename(columns={"total_revenue": "rev_py"})
    src_cur = src_cur.merge(src_pyr, on="source_code", how="left").fillna(0)

    fig_src = go.Figure()
    fig_src.add_trace(go.Bar(
        y=src_cur["label"], x=src_cur["total_revenue"],
        orientation="h", name=f"{start.year}",
        marker_color="#4a6fa5",
        hovertemplate="%{y}: $%{x:,.0f}<extra></extra>",
    ))
    fig_src.add_trace(go.Bar(
        y=src_cur["label"], x=src_cur["rev_py"],
        orientation="h", name=f"{start.year - 1} (same period)",
        marker_color="rgba(74,111,165,0.25)",
        marker_line=dict(color="#4a6fa5", width=1),
        hovertemplate="%{y}: $%{x:,.0f} (PY)<extra></extra>",
    ))
    src_layout = base_layout(h=360)
    src_layout["barmode"] = "group"
    src_layout["xaxis"].update({"title": "Revenue ($)", "tickprefix": "$", "tickformat": ",.0f"})
    src_layout["margin"]["l"] = 140
    fig_src.update_layout(**src_layout)
    st.plotly_chart(fig_src, use_container_width=True, config={"displayModeBar": False})

# ── Bottom nav ────────────────────────────────────────────────────────────────
st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
render_nav()
