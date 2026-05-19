import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.sidebar import render_sidebar

BASE = Path(__file__).resolve().parents[2]

# ── CSS ───────────────────────────────────────────────────────────────────────
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
  --purple:#9b6fd4;
}
*, *::before, *::after { font-family:'Inter',system-ui,sans-serif !important; -webkit-font-smoothing:antialiased; box-sizing:border-box; }
#MainMenu, footer, header { visibility:hidden; }
[data-testid="collapsedControl"] { display:none; }
.stApp { background:var(--dark2) !important; }
.block-container { padding:0 !important; max-width:1300px !important; margin:0 auto !important; }
[data-testid="stAppViewContainer"] { background:var(--dark2) !important; }

.dm-topbar { background:var(--dark); border-bottom:1px solid var(--border); height:52px; display:flex; align-items:center; padding:0 20px; gap:10px; }
.dm-title  { font-size:14px; font-weight:600; color:var(--white); }
.dm-sub    { font-size:11px; color:var(--muted); }
.dm-rule   { height:2px; background:linear-gradient(90deg,var(--purple) 0%,transparent 60%); }

.dm-kpi-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; padding:18px 20px 6px; }
.dm-kpi { background:var(--dark); border:1px solid var(--border); border-radius:6px; padding:14px 16px; }
.dm-kpi-lbl { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:6px; }
.dm-kpi-val { font-size:22px; font-weight:600; color:var(--white); line-height:1; }
.dm-kpi-sub { font-size:10px; color:var(--muted2); margin-top:5px; }

.dm-section { padding:16px 20px 0; }
.dm-section-ttl { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }
</style>
""", unsafe_allow_html=True)

render_sidebar(active="demand")

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_res():
    df = pd.read_csv(BASE / "data" / "processed" / "res_main_clean.csv",
                     parse_dates=["created_date", "arrival_date", "departure_date"])
    df["lead_time"] = (df["arrival_date"] - df["created_date"]).dt.days
    return df

@st.cache_data
def load_ready():
    return pd.read_csv(BASE / "data" / "final" / "hotel_model_ready.csv",
                       parse_dates=["business_date"])

@st.cache_data
def load_source():
    return pd.read_csv(BASE / "data" / "processed" / "daily_stats_source.csv",
                       parse_dates=["business_date"])

res     = load_res()
ready   = load_ready()
src_df  = load_source()

# ── Topbar ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="dm-topbar">
  <span class="dm-title">Demand Drivers</span>
  <span class="dm-sub">Booking behavior, lead times & channel mix</span>
</div>
<div class="dm-rule"></div>
""", unsafe_allow_html=True)

# ── Controls ──────────────────────────────────────────────────────────────────
HORIZONS = ["Q1", "Q2", "Q3", "Q4", "Full Year", "Custom"]
if "dm_hz" not in st.session_state:
    st.session_state["dm_hz"] = "Full Year"

hz = st.segmented_control("Horizon", HORIZONS,
                           default=st.session_state["dm_hz"],
                           key="dm_hz_ctrl", label_visibility="collapsed")
if hz:
    st.session_state["dm_hz"] = hz
horizon = st.session_state["dm_hz"]

_yc1, _yc2, _ = st.columns([2, 2, 5])
with _yc1:
    sel_year = st.selectbox("Year", [2025, 2024], key="dm_year",
                            label_visibility="collapsed")

def preset_dates(h, yr):
    if h == "Q1":        return date(yr, 1, 1),  date(yr, 3, 31)
    if h == "Q2":        return date(yr, 4, 1),  date(yr, 6, 30)
    if h == "Q3":        return date(yr, 7, 1),  date(yr, 9, 30)
    if h == "Q4":        return date(yr, 10, 1), date(yr, 12, 31)
    if h == "Full Year": return date(yr, 1, 1),  date(yr, 12, 31)
    return None, None

if horizon == "Custom":
    _c1, _c2, _ = st.columns([2, 2, 4])
    with _c1:
        start = st.date_input("From", value=date(sel_year, 1, 1),
                              min_value=date(2024, 1, 1), max_value=date(2025, 12, 31),
                              key="dm_start", format="MM/DD/YYYY", label_visibility="collapsed")
    with _c2:
        end = st.date_input("To", value=date(sel_year, 12, 31),
                            min_value=date(2024, 1, 1), max_value=date(2025, 12, 31),
                            key="dm_end", format="MM/DD/YYYY", label_visibility="collapsed")
    if isinstance(start, date) and isinstance(end, date) and start > end:
        start, end = end, start
else:
    start, end = preset_dates(horizon, sel_year)

s_ts = pd.Timestamp(start)
e_ts = pd.Timestamp(end)

# Filter reservation data by arrival date
res_f = res[(res["arrival_date"] >= s_ts) & (res["arrival_date"] <= e_ts)].copy()
res_f = res_f[res_f["lead_time"] >= 0]

# Prior year reservation data
py_yr = sel_year - 1
try:
    py_s = pd.Timestamp(start.replace(year=py_yr))
    py_e = pd.Timestamp(end.replace(year=py_yr))
except ValueError:
    py_s = pd.Timestamp(start.replace(year=py_yr, day=28))
    py_e = pd.Timestamp(end.replace(year=py_yr, day=28))
res_py = res[(res["arrival_date"] >= py_s) & (res["arrival_date"] <= py_e)].copy()
res_py = res_py[res_py["lead_time"] >= 0]

# Ready / pickup data filter
ready_f  = ready[(ready["business_date"] >= s_ts) & (ready["business_date"] <= e_ts)].copy()
src_f    = src_df[(src_df["business_date"] >= s_ts) & (src_df["business_date"] <= e_ts)].copy()

# ── KPIs ──────────────────────────────────────────────────────────────────────
total_bookings  = len(res_f[~res_f["is_cancelled"]])
avg_lead        = res_f[~res_f["is_cancelled"]]["lead_time"].median()
cancel_rate     = (res_f["is_cancelled"].sum() / len(res_f) * 100) if len(res_f) else 0
avg_los         = res_f[~res_f["is_cancelled"]]["number_of_nights"].median()
direct_codes    = {"WEB", "WEBSITE", "WEBBOOK", "TEL", "CALL", "PHONE", "MOBILE"}
direct_n        = res_f[~res_f["is_cancelled"] & res_f["origin_code"].isin(direct_codes)]
direct_pct      = (len(direct_n) / total_bookings * 100) if total_bookings else 0

# PY KPIs
avg_lead_py   = res_py[~res_py["is_cancelled"]]["lead_time"].median() if not res_py.empty else 0
cancel_py     = (res_py["is_cancelled"].sum() / len(res_py) * 100) if len(res_py) else 0

def arrow(v, base, higher_is_better=True):
    if base == 0:
        return ""
    diff = v - base
    pct  = abs(diff / base * 100)
    up   = diff >= 0
    good = (up and higher_is_better) or (not up and not higher_is_better)
    cls  = "color:#3ecf8e" if good else "color:#e05252"
    sym  = "▲" if up else "▼"
    return f'<span style="{cls};font-size:10px;font-weight:600;">{sym} {pct:.1f}%</span>'

st.markdown(f"""
<div class="dm-kpi-grid">
  <div class="dm-kpi">
    <div class="dm-kpi-lbl">Median Lead Time</div>
    <div class="dm-kpi-val">{avg_lead:.0f} days</div>
    <div class="dm-kpi-sub">{arrow(avg_lead, avg_lead_py, higher_is_better=False)} vs {avg_lead_py:.0f}d in {py_yr}</div>
  </div>
  <div class="dm-kpi">
    <div class="dm-kpi-lbl">Cancellation Rate</div>
    <div class="dm-kpi-val">{cancel_rate:.1f}%</div>
    <div class="dm-kpi-sub">{arrow(cancel_rate, cancel_py, higher_is_better=False)} vs {cancel_py:.1f}% in {py_yr}</div>
  </div>
  <div class="dm-kpi">
    <div class="dm-kpi-lbl">Median Length of Stay</div>
    <div class="dm-kpi-val">{avg_los:.0f} nights</div>
    <div class="dm-kpi-sub">Confirmed bookings only</div>
  </div>
  <div class="dm-kpi">
    <div class="dm-kpi-lbl">Direct Booking Share</div>
    <div class="dm-kpi-val">{direct_pct:.1f}%</div>
    <div class="dm-kpi-sub">Web, phone &amp; mobile</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Chart helpers ─────────────────────────────────────────────────────────────
BG, GRID = "#1a1a1a", "rgba(255,255,255,0.05)"
FONT     = "rgba(245,245,240,0.5)"
ACCENT, ACCENT_F = "#e8854a", "rgba(232,133,74,0.22)"
GREEN,  GREEN_F  = "#3ecf8e", "rgba(62,207,142,0.20)"
SLATE,  SLATE_F  = "#4a6fa5", "rgba(74,111,165,0.22)"
PURPLE, PURPLE_F = "#9b6fd4", "rgba(155,111,212,0.22)"

def base_layout(h=280, ytitle="", yprefix="", ysuffix=""):
    return dict(
        paper_bgcolor=BG, plot_bgcolor=BG,
        font=dict(family="Inter", color=FONT, size=10),
        margin=dict(l=4, r=4, t=28, b=4), height=h,
        bargap=0.18, bargroupgap=0.08,
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0,
                    orientation="h", x=0, y=1.14,
                    font=dict(size=10, color="rgba(245,245,240,0.6)")),
        xaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)",
                   tickfont=dict(size=9), tickangle=0),
        yaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)",
                   tickfont=dict(size=9), zeroline=False,
                   title=ytitle, tickprefix=yprefix, ticksuffix=ysuffix),
        hovermode="x unified",
    )

# ── Booking Window & Length of Stay ──────────────────────────────────────────
st.markdown('<div class="dm-section"><div class="dm-section-ttl">Booking Behavior</div></div>',
            unsafe_allow_html=True)

_c1, _c2 = st.columns(2)

# ── Booking window histogram ──────────────────────────────────────────────────
with _c1:
    bins   = [0, 1, 8, 15, 31, 61, 91, 462]
    labels = ["Same Day", "1–7d", "8–14d", "15–30d", "31–60d", "61–90d", "90d+"]

    def bucket(df_r, yr):
        c = df_r[~df_r["is_cancelled"]]
        counts = pd.cut(c["lead_time"], bins=bins, labels=labels,
                        right=False, include_lowest=True).value_counts()
        return [int(counts.get(l, 0)) for l in labels]

    cy_vals = bucket(res_f, sel_year)
    py_vals = bucket(res_py, py_yr)

    fig_bw = go.Figure()
    fig_bw.add_trace(go.Bar(x=labels, y=cy_vals, name=str(sel_year),
                            marker_color=PURPLE,
                            hovertemplate="%{x}: %{y:,} bookings<extra>" + str(sel_year) + "</extra>"))
    fig_bw.add_trace(go.Bar(x=labels, y=py_vals, name=str(py_yr),
                            marker_color=PURPLE_F, marker_line=dict(color=PURPLE, width=1),
                            hovertemplate="%{x}: %{y:,} bookings<extra>" + str(py_yr) + "</extra>"))
    lay = base_layout(h=260)
    lay["title"] = dict(text="Booking Window Distribution",
                        font=dict(size=11, color="rgba(245,245,240,0.7)"),
                        x=0, xanchor="left", pad=dict(l=4))
    lay["barmode"] = "group"
    fig_bw.update_layout(**lay)
    st.plotly_chart(fig_bw, use_container_width=True, config={"displayModeBar": False})

# ── Length of stay distribution ───────────────────────────────────────────────
with _c2:
    los_bins   = [1, 2, 3, 4, 5, 8, 15, 400]
    los_labels = ["1 night", "2 nights", "3 nights", "4 nights", "5–7", "8–14", "15+"]

    def los_bucket(df_r):
        c = df_r[~df_r["is_cancelled"] & (df_r["number_of_nights"] >= 1)]
        counts = pd.cut(c["number_of_nights"], bins=los_bins, labels=los_labels,
                        right=False, include_lowest=True).value_counts()
        return [int(counts.get(l, 0)) for l in los_labels]

    los_cy = los_bucket(res_f)
    los_py = los_bucket(res_py)
    pct_cy = [v / sum(los_cy) * 100 if sum(los_cy) else 0 for v in los_cy]
    pct_py = [v / sum(los_py) * 100 if sum(los_py) else 0 for v in los_py]

    fig_los = go.Figure()
    fig_los.add_trace(go.Bar(x=los_labels, y=pct_cy, name=str(sel_year),
                             marker_color=GREEN,
                             hovertemplate="%{x}: %{y:.1f}%<extra>" + str(sel_year) + "</extra>"))
    fig_los.add_trace(go.Bar(x=los_labels, y=pct_py, name=str(py_yr),
                             marker_color=GREEN_F, marker_line=dict(color=GREEN, width=1),
                             hovertemplate="%{x}: %{y:.1f}%<extra>" + str(py_yr) + "</extra>"))
    lay2 = base_layout(h=260, ysuffix="%")
    lay2["title"] = dict(text="Length of Stay Distribution",
                         font=dict(size=11, color="rgba(245,245,240,0.7)"),
                         x=0, xanchor="left", pad=dict(l=4))
    lay2["barmode"] = "group"
    fig_los.update_layout(**lay2)
    st.plotly_chart(fig_los, use_container_width=True, config={"displayModeBar": False})

# ── Booking pace / pickup ─────────────────────────────────────────────────────
st.markdown('<div class="dm-section"><div class="dm-section-ttl">Booking Pace — Rooms Picked Up</div></div>',
            unsafe_allow_html=True)

n_days = (end - start).days + 1
freq   = "D" if n_days <= 14 else ("W" if n_days <= 60 else "ME")
fkey   = "D" if freq == "D" else ("W" if freq == "W" else "M")
dfmt   = "%b %d" if n_days <= 60 else "%b"

def pace_agg(col):
    d = ready_f.copy()
    d["p"] = d["business_date"].dt.to_period(fkey)
    g = d.groupby("p")[col].mean().reset_index()
    xs = []
    for p in g["p"]:
        try: xs.append(p.start_time.strftime(dfmt))
        except: xs.append(str(p))
    return xs, list(g[col])

xl7,  v7  = pace_agg("pickup_7d")
xl14, v14 = pace_agg("pickup_14d")
xl30, v30 = pace_agg("pickup_30d")
xl_ob, ob = pace_agg("total_rooms_on_books")

fig_pace = go.Figure()
fig_pace.add_trace(go.Scatter(x=xl7,  y=v7,  name="7-day pickup",
                              line=dict(color=ACCENT, width=2),
                              hovertemplate="%{x}: %{y:.0f} rooms<extra>7d</extra>"))
fig_pace.add_trace(go.Scatter(x=xl14, y=v14, name="14-day pickup",
                              line=dict(color=GREEN, width=2),
                              hovertemplate="%{x}: %{y:.0f} rooms<extra>14d</extra>"))
fig_pace.add_trace(go.Scatter(x=xl30, y=v30, name="30-day pickup",
                              line=dict(color=PURPLE, width=2),
                              hovertemplate="%{x}: %{y:.0f} rooms<extra>30d</extra>"))
fig_pace.add_trace(go.Scatter(x=xl_ob, y=ob, name="On books",
                              line=dict(color=SLATE, width=1.5, dash="dot"),
                              hovertemplate="%{x}: %{y:.0f} rooms<extra>On Books</extra>",
                              yaxis="y2"))

pace_lay = base_layout(h=260)
pace_lay["title"] = dict(text="Booking Pickup vs Rooms on Books",
                         font=dict(size=11, color="rgba(245,245,240,0.7)"),
                         x=0, xanchor="left", pad=dict(l=4))
pace_lay["yaxis2"] = dict(
    overlaying="y", side="right",
    gridcolor="transparent",
    tickfont=dict(size=9, color=SLATE),
    zeroline=False, showgrid=False,
)
fig_pace.update_layout(**pace_lay)
st.plotly_chart(fig_pace, use_container_width=True, config={"displayModeBar": False})

# ── Channel revenue & Cancellation trend ──────────────────────────────────────
st.markdown('<div class="dm-section"><div class="dm-section-ttl">Channel Analysis & Cancellation Trend</div></div>',
            unsafe_allow_html=True)

_c3, _c4 = st.columns(2)

ORIGIN_NAMES = {
    "WEB":     "Web (Direct)",
    "SYDC":    "Synxis / CRS",
    "TEL":     "Phone",
    "PMS":     "Walk-in / PMS",
    "GDS":     "GDS",
    "LIST":    "Listed Rate",
    "CALL":    "Call Center",
    "CRS":     "Central Reservations",
    "LCL":     "Local / Sales",
    "NG":      "Negotiated",
    "MOBILE":  "Mobile",
    "WEBSITE": "Website",
    "WEBBOOK": "Web Booking",
}

with _c3:
    ch_rev = (src_f.groupby("origin_code")["total_revenue"].sum()
              .nlargest(8).sort_values())
    ch_labels = [ORIGIN_NAMES.get(c, c) for c in ch_rev.index]

    fig_ch = go.Figure()
    fig_ch.add_trace(go.Bar(
        y=ch_labels, x=ch_rev.values, orientation="h",
        marker_color=ACCENT,
        hovertemplate="%{y}: $%{x:,.0f}<extra></extra>",
    ))
    ch_lay = base_layout(h=280)
    ch_lay["title"] = dict(text="Revenue by Booking Channel",
                           font=dict(size=11, color="rgba(245,245,240,0.7)"),
                           x=0, xanchor="left", pad=dict(l=4))
    ch_lay["margin"]["l"] = 130
    ch_lay["xaxis"].update({"tickprefix": "$", "tickformat": ",.0f"})
    ch_lay["bargap"] = 0.25
    fig_ch.update_layout(**ch_lay)
    st.plotly_chart(fig_ch, use_container_width=True, config={"displayModeBar": False})

with _c4:
    # Monthly cancellation rate
    res_all = res.copy()
    res_all["ym"] = res_all["arrival_date"].dt.to_period("M")
    res_all = res_all[
        (res_all["arrival_date"] >= s_ts) & (res_all["arrival_date"] <= e_ts)
    ]
    cancel_monthly = res_all.groupby("ym").apply(
        lambda g: g["is_cancelled"].sum() / len(g) * 100 if len(g) else 0
    ).reset_index(name="cancel_rate")
    cancel_monthly["ym_str"] = cancel_monthly["ym"].apply(
        lambda p: p.start_time.strftime("%b '%y")
    )

    # Prior year
    res_py_all = res[
        (res["arrival_date"] >= py_s) & (res["arrival_date"] <= py_e)
    ].copy()
    res_py_all["ym"] = res_py_all["arrival_date"].dt.to_period("M")
    cancel_py_m = res_py_all.groupby("ym").apply(
        lambda g: g["is_cancelled"].sum() / len(g) * 100 if len(g) else 0
    ).reset_index(name="cancel_rate")
    cancel_py_m["ym_str"] = cancel_py_m["ym"].apply(
        lambda p: p.start_time.strftime("%b '%y")
    )

    fig_can = go.Figure()
    fig_can.add_trace(go.Scatter(
        x=cancel_monthly["ym_str"], y=cancel_monthly["cancel_rate"],
        name=str(sel_year), line=dict(color=GREEN, width=2),
        fill="tozeroy", fillcolor="rgba(62,207,142,0.07)",
        hovertemplate="%{x}: %{y:.1f}%<extra>" + str(sel_year) + "</extra>",
    ))
    fig_can.add_trace(go.Scatter(
        x=cancel_py_m["ym_str"], y=cancel_py_m["cancel_rate"],
        name=str(py_yr), line=dict(color=GREEN_F, width=1.5, dash="dot"),
        hovertemplate="%{x}: %{y:.1f}%<extra>" + str(py_yr) + "</extra>",
    ))
    can_lay = base_layout(h=280, ysuffix="%")
    can_lay["title"] = dict(text="Cancellation Rate by Month",
                            font=dict(size=11, color="rgba(245,245,240,0.7)"),
                            x=0, xanchor="left", pad=dict(l=4))
    can_lay["xaxis"]["tickangle"] = -30
    fig_can.update_layout(**can_lay)
    st.plotly_chart(fig_can, use_container_width=True, config={"displayModeBar": False})

st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
