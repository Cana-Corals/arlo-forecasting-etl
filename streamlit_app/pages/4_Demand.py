import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.sidebar import render_sidebar

BASE = Path(__file__).resolve().parents[2]

# ── Channel classification ────────────────────────────────────────────────────
OTA_NAMES_SET = frozenset([
    "Expedia Collect - Williamsburg", "Booking.com",
    "HotelTonight", "Hopper Hotels",
])
DIRECT_ORIGIN = frozenset({"WEB", "WEBSITE", "WEBBOOK", "MOBILE"})
PHONE_ORIGIN  = frozenset({"TEL", "CALL", "PHONE", "CRS", "SYDC"})

def classify_channel(row) -> str:
    cn = str(row.get("company_name", ""))
    oc = str(row.get("origin_code", ""))
    if any(n in cn for n in OTA_NAMES_SET): return "OTA"
    if oc == "GDS":                          return "GDS"
    if oc in DIRECT_ORIGIN:                  return "Direct"
    if oc in PHONE_ORIGIN:                   return "Phone/CRS"
    return "Other/Corp"

def classify_ota_platform(rc) -> str | None:
    if isinstance(rc, str):
        if rc.startswith("OTEX"):  return "Expedia"
        if rc.startswith("OTBK"):  return "Booking.com"
        if rc.startswith("OTHTL"): return "HotelTonight"
    return None

LEAD_BINS   = [-1, 7, 14, 30, 60, 9_999]
LEAD_LABELS = ["0–7 days", "8–14 days", "15–30 days", "31–60 days", "60+ days"]
LOS_BINS    = [0, 1, 2, 3, 7, 9_999]
LOS_LABELS  = ["1 night", "2 nights", "3 nights", "4–7 nights", "7+ nights"]

CHANNEL_COLORS = {
    "Direct":     "#9b6fd4",
    "OTA":        "#38bdf8",
    "Phone/CRS":  "#e8854a",
    "GDS":        "#4a6fa5",
    "Other/Corp": "#555555",
}

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
.dm-sub    { font-size:22px; color:var(--muted); }
.dm-rule   { height:2px; background:linear-gradient(90deg,var(--purple) 0%,transparent 60%); }
.dm-home-btn { margin-left:auto; font-size:11px; color:rgba(245,245,240,.45); text-decoration:none; padding:5px 11px; border:1px solid rgba(255,255,255,.1); border-radius:4px; transition:color .15s,border-color .15s; }
.dm-home-btn:hover { color:rgba(245,245,240,.9); border-color:rgba(255,255,255,.25); }

.dm-kpi-grid { display:grid; grid-template-columns:repeat(5,1fr); gap:12px; padding:18px 20px 6px; }
.dm-kpi { background:var(--dark); border:1px solid var(--border); border-radius:6px; padding:14px 16px; }
.dm-kpi-lbl { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:6px; }
.dm-kpi-val { font-size:28px; font-weight:600; color:var(--white); line-height:1; }
.dm-kpi-sub { font-size:20px; color:var(--muted2); margin-top:5px; }

.dm-section { padding:16px 20px 0; }
.dm-section-ttl { font-size:18px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }
</style>
""", unsafe_allow_html=True)

render_sidebar(active="demand")

# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data
def load_res():
    df = pd.read_csv(BASE / "data" / "processed" / "res_main_clean.csv",
                     parse_dates=["created_date", "arrival_date", "departure_date"])
    df["lead_time"] = (df["arrival_date"] - df["created_date"]).dt.days
    df["channel"]   = df.apply(classify_channel, axis=1)
    return df

@st.cache_data
def load_res_daily():
    return pd.read_csv(
        BASE / "data" / "processed" / "reservations_daily.csv",
        parse_dates=["stay_date"],
        usecols=["confirmation_number", "stay_date", "rate_code", "rate",
                 "market_code", "source_code"],
    )

@st.cache_data
def load_ready():
    return pd.read_csv(BASE / "data" / "final" / "hotel_model_ready.csv",
                       parse_dates=["business_date"])

@st.cache_data
def load_source():
    return pd.read_csv(BASE / "data" / "processed" / "daily_stats_source.csv",
                       parse_dates=["business_date"])

@st.cache_data
def load_master():
    return pd.read_csv(BASE / "data" / "final" / "hotel_daily_master.csv",
                       parse_dates=["business_date"])

res     = load_res()
res_rn  = load_res_daily()
ready   = load_ready()
src_df  = load_source()
master  = load_master()

# ── Topbar ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="dm-topbar">
  <span class="dm-title">Demand Drivers</span>
  <span class="dm-sub">Booking behavior, lead times & channel mix</span>
  <a href="/" target="_self" class="dm-home-btn">⌂ Home</a>
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

_yc1, _ = st.columns([2, 7])
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

# Current period reservations
res_f = res[(res["arrival_date"] >= s_ts) & (res["arrival_date"] <= e_ts)].copy()
res_f = res_f[res_f["lead_time"] >= 0]

# Prior year date range
py_yr = sel_year - 1
try:
    py_s = pd.Timestamp(start.replace(year=py_yr))
    py_e = pd.Timestamp(end.replace(year=py_yr))
except ValueError:
    py_s = pd.Timestamp(start.replace(year=py_yr, day=28))
    py_e = pd.Timestamp(end.replace(year=py_yr, day=28))
res_py = res[(res["arrival_date"] >= py_s) & (res["arrival_date"] <= py_e)].copy()
res_py = res_py[res_py["lead_time"] >= 0]

# Ready / source filters
ready_f    = ready[(ready["business_date"] >= s_ts) & (ready["business_date"] <= e_ts)].copy()
ready_py_f = ready[(ready["business_date"] >= py_s) & (ready["business_date"] <= py_e)].copy()
src_f      = src_df[(src_df["business_date"] >= s_ts) & (src_df["business_date"] <= e_ts)].copy()

# ── KPIs ──────────────────────────────────────────────────────────────────────
confirmed    = res_f[~res_f["is_cancelled"]]
confirmed_py = res_py[~res_py["is_cancelled"]] if not res_py.empty else pd.DataFrame()

cancel_rate    = (res_f["is_cancelled"].sum() / len(res_f) * 100) if len(res_f) else 0
cancel_py_val  = (res_py["is_cancelled"].sum() / len(res_py) * 100) if len(res_py) else 0

avg_lead    = confirmed["lead_time"].median() if not confirmed.empty else 0
avg_lead_py = confirmed_py["lead_time"].median() if not confirmed_py.empty else 0

avg_los    = confirmed["number_of_nights"].median() if not confirmed.empty else 0
avg_los_py = confirmed_py["number_of_nights"].median() if not confirmed_py.empty else 0

direct_n      = confirmed[confirmed["channel"] == "Direct"]
direct_pct    = (len(direct_n) / len(confirmed) * 100) if len(confirmed) else 0
direct_n_py   = confirmed_py[confirmed_py["channel"] == "Direct"] if not confirmed_py.empty else pd.DataFrame()
direct_pct_py = (len(direct_n_py) / len(confirmed_py) * 100) if not confirmed_py.empty and len(confirmed_py) > 0 else 0

avg_pickup    = ready_f["pickup_7d"].mean() if not ready_f.empty else 0
avg_pickup_py = ready_py_f["pickup_7d"].mean() if not ready_py_f.empty else 0

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
    <div class="dm-kpi-sub">{arrow(cancel_rate, cancel_py_val, higher_is_better=False)} vs {cancel_py_val:.1f}% in {py_yr}</div>
  </div>
  <div class="dm-kpi">
    <div class="dm-kpi-lbl">Median Length of Stay</div>
    <div class="dm-kpi-val">{avg_los:.0f} nights</div>
    <div class="dm-kpi-sub">{arrow(avg_los, avg_los_py)} vs {avg_los_py:.0f} nights in {py_yr}</div>
  </div>
  <div class="dm-kpi">
    <div class="dm-kpi-lbl">Direct Booking Share</div>
    <div class="dm-kpi-val">{direct_pct:.1f}%</div>
    <div class="dm-kpi-sub">{arrow(direct_pct, direct_pct_py)} vs {direct_pct_py:.1f}% in {py_yr}</div>
  </div>
  <div class="dm-kpi">
    <div class="dm-kpi-lbl">Avg Daily Pickup (7d)</div>
    <div class="dm-kpi-val">{avg_pickup:.0f}</div>
    <div class="dm-kpi-sub">{arrow(avg_pickup, avg_pickup_py)} vs {avg_pickup_py:.0f} in {py_yr}</div>
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
TEAL             = "#38bdf8"

def base_layout(h=280, ytitle="", yprefix="", ysuffix=""):
    return dict(
        paper_bgcolor=BG, plot_bgcolor=BG,
        font=dict(family="Inter", color=FONT, size=20),
        margin=dict(l=4, r=4, t=48, b=4), height=h,
        bargap=0.18, bargroupgap=0.08,
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0,
                    orientation="h", x=0, y=1.14,
                    font=dict(size=20, color="rgba(245,245,240,0.6)")),
        xaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)",
                   tickfont=dict(size=18), tickangle=0),
        yaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)",
                   tickfont=dict(size=18), zeroline=False,
                   title=ytitle, tickprefix=yprefix, ticksuffix=ysuffix),
        hovermode="x unified",
    )

# ── Booking Behavior ──────────────────────────────────────────────────────────
st.markdown('<div class="dm-section"><div class="dm-section-ttl">Booking Behavior</div></div>',
            unsafe_allow_html=True)

_bb1, _bb2 = st.columns(2)

with _bb1:
    lt_cut = confirmed.copy()
    lt_cut["lt_bin"] = pd.cut(lt_cut["lead_time"], bins=LEAD_BINS, labels=LEAD_LABELS)
    lt_counts = lt_cut["lt_bin"].value_counts().reindex(LEAD_LABELS, fill_value=0)
    lt_pct = (lt_counts / lt_counts.sum() * 100) if lt_counts.sum() > 0 else lt_counts * 0.0

    fig_lt = go.Figure()
    fig_lt.add_trace(go.Bar(
        x=LEAD_LABELS, y=lt_pct.values, name=str(sel_year),
        marker_color=PURPLE,
        hovertemplate="%{x}: %{y:.1f}%<extra>" + str(sel_year) + "</extra>",
    ))

    if not confirmed_py.empty:
        lt_py_cut = confirmed_py.copy()
        lt_py_cut["lt_bin"] = pd.cut(lt_py_cut["lead_time"], bins=LEAD_BINS, labels=LEAD_LABELS)
        lt_py_counts = lt_py_cut["lt_bin"].value_counts().reindex(LEAD_LABELS, fill_value=0)
        lt_py_pct = (lt_py_counts / lt_py_counts.sum() * 100) if lt_py_counts.sum() > 0 else lt_py_counts * 0.0
        fig_lt.add_trace(go.Bar(
            x=LEAD_LABELS, y=lt_py_pct.values, name=str(py_yr),
            marker_color=PURPLE_F,
            marker_line=dict(color=PURPLE, width=1),
            hovertemplate="%{x}: %{y:.1f}%<extra>" + str(py_yr) + "</extra>",
        ))

    lt_lay = base_layout(h=260, ysuffix="%")
    lt_lay["title"] = dict(text="Lead Time Distribution",
                           font=dict(size=22, color="rgba(245,245,240,0.7)"),
                           x=0, xanchor="left", pad=dict(l=4))
    lt_lay["barmode"] = "group"
    lt_lay["xaxis"]["tickangle"] = -20
    fig_lt.update_layout(**lt_lay)
    st.plotly_chart(fig_lt, use_container_width=True, config={"displayModeBar": False})

with _bb2:
    los_cut = confirmed.copy()
    los_cut["los_bin"] = pd.cut(los_cut["number_of_nights"], bins=LOS_BINS, labels=LOS_LABELS)
    los_counts = los_cut["los_bin"].value_counts().reindex(LOS_LABELS, fill_value=0)
    los_pct = (los_counts / los_counts.sum() * 100) if los_counts.sum() > 0 else los_counts * 0.0

    fig_los = go.Figure()
    fig_los.add_trace(go.Bar(
        x=LOS_LABELS, y=los_pct.values, name=str(sel_year),
        marker_color=ACCENT,
        hovertemplate="%{x}: %{y:.1f}%<extra>" + str(sel_year) + "</extra>",
    ))

    if not confirmed_py.empty:
        los_py_cut = confirmed_py.copy()
        los_py_cut["los_bin"] = pd.cut(los_py_cut["number_of_nights"], bins=LOS_BINS, labels=LOS_LABELS)
        los_py_counts = los_py_cut["los_bin"].value_counts().reindex(LOS_LABELS, fill_value=0)
        los_py_pct = (los_py_counts / los_py_counts.sum() * 100) if los_py_counts.sum() > 0 else los_py_counts * 0.0
        fig_los.add_trace(go.Bar(
            x=LOS_LABELS, y=los_py_pct.values, name=str(py_yr),
            marker_color=ACCENT_F,
            marker_line=dict(color=ACCENT, width=1),
            hovertemplate="%{x}: %{y:.1f}%<extra>" + str(py_yr) + "</extra>",
        ))

    los_lay = base_layout(h=260, ysuffix="%")
    los_lay["title"] = dict(text="Length of Stay Distribution",
                            font=dict(size=22, color="rgba(245,245,240,0.7)"),
                            x=0, xanchor="left", pad=dict(l=4))
    los_lay["barmode"] = "group"
    fig_los.update_layout(**los_lay)
    st.plotly_chart(fig_los, use_container_width=True, config={"displayModeBar": False})

# ── Booking pace / pickup ── hidden ───────────────────────────────────────────

# ── Channel Mix ───────────────────────────────────────────────────────────────
st.markdown('<div class="dm-section"><div class="dm-section-ttl">Channel Mix</div></div>',
            unsafe_allow_html=True)

_cm1, _cm2, _cm3 = st.columns(3)

with _cm1:
    channel_order  = ["Direct", "OTA", "Phone/CRS", "GDS", "Other/Corp"]
    ch_counts      = confirmed["channel"].value_counts()
    ch_vals        = [ch_counts.get(ch, 0) for ch in channel_order]
    ch_colors      = [CHANNEL_COLORS.get(ch, "#555") for ch in channel_order]

    fig_donut = go.Figure()
    fig_donut.add_trace(go.Pie(
        labels=channel_order,
        values=ch_vals,
        hole=0.55,
        marker=dict(colors=ch_colors, line=dict(color=BG, width=2)),
        textinfo="percent",
        textfont=dict(size=18, color="#fff"),
        hovertemplate="%{label}: %{value:,} bookings (%{percent})<extra></extra>",
    ))
    fig_donut.update_layout(
        paper_bgcolor=BG, plot_bgcolor=BG,
        font=dict(family="Inter", color=FONT, size=20),
        margin=dict(l=4, r=4, t=48, b=4), height=280,
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0,
                    orientation="v", x=0.82, y=0.5,
                    font=dict(size=9, color="rgba(245,245,240,0.6)")),
        title=dict(text="Channel Mix — " + str(sel_year),
                   font=dict(size=22, color="rgba(245,245,240,0.7)"),
                   x=0, xanchor="left", pad=dict(l=4)),
    )
    st.plotly_chart(fig_donut, use_container_width=True, config={"displayModeBar": False})

with _cm2:
    # OTA platform trend (24-month, from reservations_daily rate_code)
    res_rn_ota = res_rn.copy()
    res_rn_ota["ota_platform"] = res_rn_ota["rate_code"].apply(classify_ota_platform)
    res_rn_ota = res_rn_ota[res_rn_ota["ota_platform"].notna()].copy()
    res_rn_ota["ym"] = res_rn_ota["stay_date"].dt.to_period("M")
    ota_monthly = (res_rn_ota.groupby(["ym", "ota_platform"])
                   .size().unstack(fill_value=0).sort_index())

    plat_colors = {"Expedia": TEAL, "Booking.com": GREEN, "HotelTonight": ACCENT}
    fig_ota = go.Figure()
    for plat, pcolor in plat_colors.items():
        if plat not in ota_monthly.columns:
            continue
        d  = ota_monthly[plat]
        xs = [p.start_time.strftime("%b '%y") for p in d.index]
        fig_ota.add_trace(go.Scatter(
            x=xs, y=d.values, name=plat,
            line=dict(color=pcolor, width=2),
            hovertemplate="%{x}: %{y:,} room-nights<extra>" + plat + "</extra>",
        ))
    ota_lay = base_layout(h=280)
    ota_lay["title"] = dict(text="OTA Platform Trend (room-nights)",
                            font=dict(size=22, color="rgba(245,245,240,0.7)"),
                            x=0, xanchor="left", pad=dict(l=4))
    ota_lay["xaxis"]["tickangle"] = -30
    fig_ota.update_layout(**ota_lay)
    st.plotly_chart(fig_ota, use_container_width=True, config={"displayModeBar": False})

with _cm3:
    # Direct vs OTA % trend — 24-month, all confirmed bookings
    trend_res = res[~res["is_cancelled"]].copy()
    trend_res["ym"] = trend_res["arrival_date"].dt.to_period("M")
    ch_trend = (trend_res.groupby(["ym", "channel"])
                .size().unstack(fill_value=0).sort_index())
    ch_trend["total"] = ch_trend.sum(axis=1)
    ch_trend["direct_pct"] = ch_trend.get("Direct", 0) / ch_trend["total"] * 100
    ch_trend["ota_pct"]    = ch_trend.get("OTA", 0) / ch_trend["total"] * 100

    xs_trend = [p.start_time.strftime("%b '%y") for p in ch_trend.index]
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=xs_trend, y=ch_trend["direct_pct"].values, name="Direct",
        line=dict(color=PURPLE, width=2),
        fill="tozeroy", fillcolor="rgba(155,111,212,0.10)",
        hovertemplate="%{x}: %{y:.1f}% Direct<extra></extra>",
    ))
    fig_trend.add_trace(go.Scatter(
        x=xs_trend, y=ch_trend["ota_pct"].values, name="OTA",
        line=dict(color=TEAL, width=2),
        fill="tozeroy", fillcolor="rgba(56,189,248,0.08)",
        hovertemplate="%{x}: %{y:.1f}% OTA<extra></extra>",
    ))
    trend_lay = base_layout(h=280, ysuffix="%")
    trend_lay["title"] = dict(text="Direct vs OTA Share (24-month)",
                              font=dict(size=22, color="rgba(245,245,240,0.7)"),
                              x=0, xanchor="left", pad=dict(l=4))
    trend_lay["xaxis"]["tickangle"] = -30
    fig_trend.update_layout(**trend_lay)
    st.plotly_chart(fig_trend, use_container_width=True, config={"displayModeBar": False})

# ── Cancellation Analysis ─────────────────────────────────────────────────────
st.markdown('<div class="dm-section"><div class="dm-section-ttl">Cancellation Analysis</div></div>',
            unsafe_allow_html=True)

_can1, _can2, _can3 = st.columns(3)

with _can1:
    # Monthly cancellation rate — current vs prior year
    res_can = res_f.copy()
    res_can["ym"] = res_can["arrival_date"].dt.to_period("M")
    cancel_monthly = (res_can.groupby("ym")
                      .apply(lambda g: g["is_cancelled"].sum() / len(g) * 100 if len(g) else 0,
                             include_groups=False)
                      .reset_index(name="cancel_rate"))
    cancel_monthly["ym_str"] = cancel_monthly["ym"].apply(
        lambda p: p.start_time.strftime("%b '%y"))

    res_py_can = res_py.copy()
    res_py_can["ym"] = res_py_can["arrival_date"].dt.to_period("M")
    cancel_py_m = (res_py_can.groupby("ym")
                   .apply(lambda g: g["is_cancelled"].sum() / len(g) * 100 if len(g) else 0,
                          include_groups=False)
                   .reset_index(name="cancel_rate"))
    cancel_py_m["ym_str"] = cancel_py_m["ym"].apply(
        lambda p: p.start_time.strftime("%b '%y"))

    fig_can = go.Figure()
    fig_can.add_trace(go.Scatter(
        x=cancel_monthly["ym_str"], y=cancel_monthly["cancel_rate"],
        name=str(sel_year), line=dict(color=GREEN, width=2),
        fill="tozeroy", fillcolor="rgba(62,207,142,0.07)",
        hovertemplate="%{x}: %{y:.1f}%<extra>" + str(sel_year) + "</extra>",
    ))
    if not cancel_py_m.empty:
        fig_can.add_trace(go.Scatter(
            x=cancel_py_m["ym_str"], y=cancel_py_m["cancel_rate"],
            name=str(py_yr), line=dict(color=GREEN_F, width=1.5, dash="dot"),
            hovertemplate="%{x}: %{y:.1f}%<extra>" + str(py_yr) + "</extra>",
        ))
    can_lay = base_layout(h=280, ysuffix="%")
    can_lay["title"] = dict(text="Monthly Cancellation Rate",
                            font=dict(size=22, color="rgba(245,245,240,0.7)"),
                            x=0, xanchor="left", pad=dict(l=4))
    can_lay["xaxis"]["tickangle"] = -30
    fig_can.update_layout(**can_lay)
    st.plotly_chart(fig_can, use_container_width=True, config={"displayModeBar": False})

with _can2:
    # Cancellation rate by booking channel
    chan_cancel = (res_f.groupby("channel")
                   .apply(lambda g: g["is_cancelled"].sum() / len(g) * 100 if len(g) > 0 else 0,
                          include_groups=False)
                   .reset_index(name="cancel_rate")
                   .sort_values("cancel_rate"))
    bar_colors = [CHANNEL_COLORS.get(c, "#555") for c in chan_cancel["channel"]]

    fig_cch = go.Figure()
    fig_cch.add_trace(go.Bar(
        y=chan_cancel["channel"], x=chan_cancel["cancel_rate"],
        orientation="h",
        marker_color=bar_colors,
        hovertemplate="%{y}: %{x:.1f}%<extra></extra>",
    ))
    cch_lay = base_layout(h=280)
    cch_lay["title"] = dict(text="Cancellation Rate by Channel",
                            font=dict(size=22, color="rgba(245,245,240,0.7)"),
                            x=0, xanchor="left", pad=dict(l=4))
    cch_lay["margin"]["l"] = 90
    cch_lay["xaxis"].update({"ticksuffix": "%"})
    cch_lay["bargap"] = 0.3
    fig_cch.update_layout(**cch_lay)
    st.plotly_chart(fig_cch, use_container_width=True, config={"displayModeBar": False})

with _can3:
    # Cancellation rate by lead time bucket
    res_f_lt = res_f.copy()
    res_f_lt["lt_bin"] = pd.cut(res_f_lt["lead_time"], bins=LEAD_BINS, labels=LEAD_LABELS)
    lt_cancel = (res_f_lt.groupby("lt_bin", observed=True)
                 .apply(lambda g: g["is_cancelled"].sum() / len(g) * 100 if len(g) > 0 else 0,
                        include_groups=False)
                 .reset_index(name="cancel_rate"))
    lt_cancel = lt_cancel.reindex(
        lt_cancel.index[
            lt_cancel["lt_bin"].map(lambda x: LEAD_LABELS.index(x) if x in LEAD_LABELS else 999)
            .argsort()
        ]
    )

    # Gradient: lighter purple → deeper red as lead time grows
    lt_gradient = ["rgba(155,111,212,0.7)", "rgba(180,100,180,0.7)",
                   "rgba(200,90,150,0.7)", "rgba(215,80,100,0.7)", "rgba(224,82,82,0.8)"]

    fig_ltc = go.Figure()
    fig_ltc.add_trace(go.Bar(
        x=lt_cancel["lt_bin"].astype(str),
        y=lt_cancel["cancel_rate"],
        marker_color=lt_gradient[:len(lt_cancel)],
        hovertemplate="%{x}: %{y:.1f}% cancelled<extra></extra>",
    ))
    ltc_lay = base_layout(h=280, ysuffix="%")
    ltc_lay["title"] = dict(text="Cancellation Rate by Lead Time",
                            font=dict(size=22, color="rgba(245,245,240,0.7)"),
                            x=0, xanchor="left", pad=dict(l=4))
    ltc_lay["xaxis"]["tickangle"] = -20
    ltc_lay["showlegend"] = False
    fig_ltc.update_layout(**ltc_lay)
    st.plotly_chart(fig_ltc, use_container_width=True, config={"displayModeBar": False})

# ── Sellout Nights ───────────────────────────────────────────────────────────
st.markdown('<div class="dm-section"><div class="dm-section-ttl">Sellout Nights — 100% Capacity Days</div></div>',
            unsafe_allow_html=True)

master["occ_pct"]  = master["occupancy_rate"] * 100
master["is_full"]  = master["occupancy_rate"] >= 0.99
master["year"]     = master["business_date"].dt.year
master["month"]    = master["business_date"].dt.month

_cs1, _cs2 = st.columns(2)

DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

with _cs1:
    dow_stats = (master.groupby(["year", "day_of_week"])
                 .apply(lambda g: g["is_full"].sum() / len(g) * 100, include_groups=False)
                 .reset_index(name="pct_full"))

    fig_dow = go.Figure()
    for yr, color in [(2024, SLATE), (2025, ACCENT)]:
        d = dow_stats[dow_stats["year"] == yr].sort_values("day_of_week")
        fig_dow.add_trace(go.Bar(
            x=[DOW_LABELS[i] for i in d["day_of_week"]],
            y=d["pct_full"],
            name=str(yr),
            marker_color=color if yr == 2025 else SLATE_F,
            marker_line=dict(color=SLATE, width=1) if yr == 2024 else dict(width=0),
            hovertemplate="%{x}: %{y:.1f}% sellout<extra>" + str(yr) + "</extra>",
        ))
    dow_lay = base_layout(h=260, ysuffix="%")
    dow_lay["title"] = dict(text="Sellout Rate by Day of Week",
                            font=dict(size=22, color="rgba(245,245,240,0.7)"),
                            x=0, xanchor="left", pad=dict(l=4))
    dow_lay["barmode"]  = "group"
    dow_lay["bargap"]   = 0.2
    dow_lay["xaxis"]["tickangle"] = 0
    fig_dow.update_layout(**dow_lay)
    st.plotly_chart(fig_dow, use_container_width=True, config={"displayModeBar": False})

with _cs2:
    month_stats = (master.groupby(["year", "month"])["is_full"]
                   .sum().reset_index(name="full_days"))
    MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    fig_mon = go.Figure()
    for yr, color in [(2024, SLATE_F), (2025, ACCENT)]:
        d = month_stats[month_stats["year"] == yr].sort_values("month")
        fig_mon.add_trace(go.Bar(
            x=[MONTH_LABELS[m - 1] for m in d["month"]],
            y=d["full_days"],
            name=str(yr),
            marker_color=color,
            marker_line=dict(color=SLATE, width=1) if yr == 2024 else dict(width=0),
            hovertemplate="%{x}: %{y} sellout days<extra>" + str(yr) + "</extra>",
        ))
    mon_lay = base_layout(h=260)
    mon_lay["title"] = dict(text="Sellout Days by Month",
                            font=dict(size=22, color="rgba(245,245,240,0.7)"),
                            x=0, xanchor="left", pad=dict(l=4))
    mon_lay["barmode"] = "group"
    mon_lay["bargap"]  = 0.15
    mon_lay["xaxis"]["tickangle"] = -30
    fig_mon.update_layout(**mon_lay)
    st.plotly_chart(fig_mon, use_container_width=True, config={"displayModeBar": False})

full_total   = int(master["is_full"].sum())
full_sat     = int(master[master["day_of_week"] == 5]["is_full"].sum())
sat_total    = int((master["day_of_week"] == 5).sum())
sat_pct      = full_sat / sat_total * 100
peak_month   = master.groupby("month")["is_full"].sum().idxmax()
peak_month_n = MONTH_LABELS[peak_month - 1]

st.markdown(f"""
<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;padding:10px 20px 0;">
  <div class="dm-kpi">
    <div class="dm-kpi-lbl">Total Sellout Days</div>
    <div class="dm-kpi-val">{full_total}</div>
    <div class="dm-kpi-sub">Across 2024–2025 ({full_total/730*100:.0f}% of all nights)</div>
  </div>
  <div class="dm-kpi">
    <div class="dm-kpi-lbl">Saturday Sellout Rate</div>
    <div class="dm-kpi-val">{sat_pct:.0f}%</div>
    <div class="dm-kpi-sub">Highest of any day of week</div>
  </div>
  <div class="dm-kpi">
    <div class="dm-kpi-lbl">Peak Month for Sellouts</div>
    <div class="dm-kpi-val">{peak_month_n}</div>
    <div class="dm-kpi-sub">{int(master.groupby("month")["is_full"].sum()[peak_month])} sellout days in {peak_month_n} (combined)</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Event Impact Analysis ─────────────────────────────────────────────────────
st.markdown('<div class="dm-section" style="padding-top:24px;"><div class="dm-section-ttl">Event Impact on Occupancy</div></div>',
            unsafe_allow_html=True)

_ce1, _ce2 = st.columns([3, 2])

EVENT_LABELS = {
    "is_us_open_event":        "US Open Tennis",
    "is_major_event_day":      "Any Major Event",
    "is_major_sports_event":   "Major Sports",
    "is_msg_event":            "Madison Sq. Garden",
    "is_barclays_event":       "Barclays Center",
}

with _ce1:
    rows = []
    for col, label in EVENT_LABELS.items():
        if col not in master.columns:
            continue
        event_days = master[master[col].astype(bool)]
        non_event  = master[~master[col].astype(bool)]
        n_event    = len(event_days)
        if n_event == 0:
            continue
        avg_ev = event_days["occ_pct"].mean()
        avg_no = non_event["occ_pct"].mean()
        lift   = avg_ev - avg_no
        rows.append({"label": label, "event_occ": avg_ev,
                     "base_occ": avg_no, "lift": lift, "n": n_event})

    df_ev = pd.DataFrame(rows).sort_values("lift")

    fig_ev = go.Figure()
    fig_ev.add_trace(go.Bar(
        y=df_ev["label"],
        x=df_ev["base_occ"],
        name="Non-event days",
        orientation="h",
        marker_color=SLATE_F,
        marker_line=dict(color=SLATE, width=1),
        hovertemplate="%{y}<br>No event: %{x:.1f}%<extra></extra>",
    ))
    fig_ev.add_trace(go.Bar(
        y=df_ev["label"],
        x=df_ev["event_occ"],
        name="Event days",
        orientation="h",
        marker_color=[ACCENT if v >= 0 else "#e05252" for v in df_ev["lift"]],
        hovertemplate="%{y}<br>Event day: %{x:.1f}%<extra></extra>",
    ))
    ev_lay = base_layout(h=300)
    ev_lay["title"] = dict(text="Avg Occupancy: Event Days vs Non-Event Days",
                           font=dict(size=22, color="rgba(245,245,240,0.7)"),
                           x=0, xanchor="left", pad=dict(l=4))
    ev_lay["barmode"]     = "overlay"
    ev_lay["bargap"]      = 0.3
    ev_lay["margin"]["l"] = 160
    ev_lay["xaxis"].update({"ticksuffix": "%", "range": [60, 100]})
    fig_ev.update_layout(**ev_lay)
    st.plotly_chart(fig_ev, use_container_width=True, config={"displayModeBar": False})

with _ce2:
    cards = ""
    for _, row in df_ev.sort_values("lift", ascending=False).iterrows():
        lift_v = row["lift"]
        color  = "#3ecf8e" if lift_v > 0 else "#eb2323"
        sym    = "▲" if lift_v > 0 else "▼"
        note   = ("Positive demand signal" if lift_v > 1
                  else "Minimal impact" if abs(lift_v) <= 1
                  else "Crowd displacement — fans don't stay nearby")
        cards += f"""
        <div class="dm-kpi" style="margin-bottom:8px;">
          <div class="dm-kpi-lbl">{row["label"]}</div>
          <div style="display:flex;align-items:baseline;gap:8px;">
            <span style="font-size:18px;font-weight:600;color:{color};">{sym} {abs(lift_v):.1f}pp</span>
            <span style="font-size:10px;color:rgba(245,245,240,0.4);">vs baseline</span>
          </div>
          <div class="dm-kpi-sub" style="margin-top:4px;">{note} ({row['n']:,} days)</div>
        </div>"""
    st.markdown(f'<div style="padding:0 0 0 8px;">{cards}</div>', unsafe_allow_html=True)

st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
