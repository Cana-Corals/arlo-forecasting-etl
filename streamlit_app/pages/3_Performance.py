import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import date, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.sidebar import render_sidebar
from components.data import load_source_stats, load_room_type_stats

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
}
*, *::before, *::after { font-family:'Inter',system-ui,sans-serif !important; -webkit-font-smoothing:antialiased; box-sizing:border-box; }
#MainMenu, footer, header { visibility:hidden; }
[data-testid="collapsedControl"] { display:none; }
.stApp { background:var(--dark2) !important; }
.block-container { padding:0 !important; max-width:1300px !important; margin:0 auto !important; }
[data-testid="stAppViewContainer"] { background:var(--dark2) !important; }

.pf-topbar { background:var(--dark); border-bottom:1px solid var(--border); height:52px; display:flex; align-items:center; padding:0 20px; gap:10px; }
.pf-title  { font-size:14px; font-weight:600; color:var(--white); }
.pf-sub    { font-size:22px; color:var(--muted); }
.pf-rule   { height:2px; background:linear-gradient(90deg,var(--slate) 0%,transparent 60%); }
.pf-home-btn { margin-left:auto; font-size:11px; color:rgba(245,245,240,.45); text-decoration:none; padding:5px 11px; border:1px solid rgba(255,255,255,.1); border-radius:4px; transition:color .15s,border-color .15s; }
.pf-home-btn:hover { color:rgba(245,245,240,.9); border-color:rgba(255,255,255,.25); }

.pf-kpi-grid { display:grid; grid-template-columns:repeat(5,1fr); gap:12px; padding:18px 20px 6px; }
.pf-kpi { background:var(--dark); border:1px solid var(--border); border-radius:6px; padding:14px 16px; }
.pf-kpi-lbl { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:6px; }
.pf-kpi-val { font-size:20px; font-weight:600; color:var(--white); line-height:1; margin-bottom:4px; }
.pf-kpi-row { display:flex; align-items:center; gap:8px; margin-top:6px; padding-top:6px; border-top:1px solid var(--border); }
.pf-kpi-py  { font-size:10px; color:var(--muted2); }
.pf-kpi-py span { font-weight:500; }
.up   { font-size:20px; font-weight:600; color:var(--green); }
.down { font-size:20px; font-weight:600; color:var(--red); }

.pf-section { padding:16px 20px 0; }
.pf-section-ttl { font-size:18px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }

.pf-medal-grid { display:grid; grid-template-columns:repeat(5,1fr); gap:10px; padding:16px 20px 0; }
.pf-medal { background:var(--dark); border:1px solid var(--border); border-radius:6px; padding:12px 14px; text-align:center; }
.pf-medal-score { font-size:22px; font-weight:700; color:var(--accent); }
.pf-medal-lbl   { font-size:9px; color:var(--muted); margin-top:4px; }
.pf-medal-bar   { height:3px; border-radius:2px; background:rgba(255,255,255,.08); margin-top:8px; }
.pf-medal-fill  { height:3px; border-radius:2px; background:var(--accent); }
</style>
""", unsafe_allow_html=True)

render_sidebar(active="performance")

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_master():
    return pd.read_csv(BASE / "data" / "final" / "hotel_daily_master.csv",
                       parse_dates=["business_date"])

@st.cache_data
def load_market():
    return pd.read_csv(BASE / "data" / "processed" / "daily_stats_market.csv",
                       parse_dates=["business_date"])

master = load_master()
mkt_df = load_market()

MARKET_NAMES = {
    "OTA": "OTA (Online Travel Agents)", "SP": "Special / Packages",
    "CO":  "Corporate",                  "HSE": "Hotel Sales / Events",
    "GC":  "Government / Corporate",     "NG":  "Negotiated / Contracted",
    "RA":  "Rack / Retail",              "GS":  "Group & Social",
    "PK":  "Packages",                   "CS":  "Consortia",
    "GE":  "Government / Extended",      "WH":  "Wholesale",
}

# ── Topbar ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="pf-topbar">
  <span class="pf-title">Performance</span>
  <span class="pf-sub">2024 – 2025 actuals</span>
  <a href="/" target="_self" class="pf-home-btn">⌂ Home</a>
</div>
<div class="pf-rule"></div>
""", unsafe_allow_html=True)

# ── Horizon selector ──────────────────────────────────────────────────────────
HORIZONS = ["Q1", "Q2", "Q3", "Q4", "Full Year", "All Years", "Custom"]
if "pf_hz" not in st.session_state:
    st.session_state["pf_hz"] = "All Years"

hz = st.segmented_control("Horizon", HORIZONS,
                           default=st.session_state["pf_hz"],
                           key="pf_hz_ctrl", label_visibility="collapsed")
if hz:
    st.session_state["pf_hz"] = hz
horizon    = st.session_state["pf_hz"]
_all_years = (horizon == "All Years")

# Year selector alongside horizon
_yc1, _yc2, _ = st.columns([2, 2, 5])
with _yc1:
    sel_year = st.selectbox("Year", [2025, 2024], key="pf_year",
                            label_visibility="collapsed",
                            disabled=_all_years)

def preset_dates(h, yr):
    if h == "1W":
        last_day = date(yr, 12, 31)
        sun = last_day - timedelta(days=(last_day.weekday() + 1) % 7)
        mon = sun - timedelta(days=6)
        return mon, sun
    if h == "1M":       return date(yr, 12, 1),  date(yr, 12, 31)
    if h == "Q1":       return date(yr, 1, 1),   date(yr, 3, 31)
    if h == "Q2":       return date(yr, 4, 1),   date(yr, 6, 30)
    if h == "Q3":       return date(yr, 7, 1),   date(yr, 9, 30)
    if h == "Q4":       return date(yr, 10, 1),  date(yr, 12, 31)
    if h == "Full Year":return date(yr, 1, 1),   date(yr, 12, 31)
    return None, None

if _all_years:
    # Show 2025 vs 2024 side-by-side, forced monthly
    start    = date(2025, 1, 1)
    end      = date(2025, 12, 31)
    sel_year = 2025
    py_yr    = 2024
    s_ts     = pd.Timestamp(start)
    e_ts     = pd.Timestamp(end)
    py_s     = pd.Timestamp(date(2024, 1, 1))
    py_e     = pd.Timestamp(date(2024, 12, 31))
elif horizon == "Custom":
    _c1, _c2, _ = st.columns([2, 2, 4])
    with _c1:
        start = st.date_input("From", value=date(sel_year, 1, 1),
                              min_value=date(2024, 1, 1), max_value=date(2025, 12, 31),
                              key="pf_start", format="MM/DD/YYYY", label_visibility="collapsed")
    with _c2:
        end = st.date_input("To", value=date(sel_year, 12, 31),
                            min_value=date(2024, 1, 1), max_value=date(2025, 12, 31),
                            key="pf_end", format="MM/DD/YYYY", label_visibility="collapsed")
    if isinstance(start, date) and isinstance(end, date) and start > end:
        start, end = end, start
    s_ts  = pd.Timestamp(start)
    e_ts  = pd.Timestamp(end)
    py_yr = sel_year - 1
    try:
        py_s = pd.Timestamp(start.replace(year=py_yr))
        py_e = pd.Timestamp(end.replace(year=py_yr))
    except ValueError:
        py_s = pd.Timestamp(start.replace(year=py_yr, day=28))
        py_e = pd.Timestamp(end.replace(year=py_yr, day=28))
else:
    start, end = preset_dates(horizon, sel_year)
    s_ts  = pd.Timestamp(start)
    e_ts  = pd.Timestamp(end)
    py_yr = sel_year - 1
    try:
        py_s = pd.Timestamp(start.replace(year=py_yr))
        py_e = pd.Timestamp(end.replace(year=py_yr))
    except ValueError:
        py_s = pd.Timestamp(start.replace(year=py_yr, day=28))
        py_e = pd.Timestamp(end.replace(year=py_yr, day=28))

df    = master[(master["business_date"] >= s_ts) & (master["business_date"] <= e_ts)].copy()
df_py = master[(master["business_date"] >= py_s) & (master["business_date"] <= py_e)].copy()

# For All Years, KPIs use the full 2024–2025 combined dataset
if _all_years:
    df_kpi = master[
        (master["business_date"] >= pd.Timestamp("2024-01-01")) &
        (master["business_date"] <= pd.Timestamp("2025-12-31"))
    ].copy()
else:
    df_kpi = df

# ── KPI helpers ───────────────────────────────────────────────────────────────
def safe(v, fallback=0.0):
    return float(v) if (v is not None and not np.isnan(v)) else fallback

def delta(v, base):
    d   = v - base
    pct = (d / abs(base) * 100) if base else 0
    cls = "up" if d >= 0 else "down"
    return cls, f"{'▲' if d>=0 else '▼'} {abs(pct):.1f}%"

rev_c  = df_kpi["total_revenue"].sum()
occ_c  = df_kpi["occupancy_rate"].mean() * 100
adr_c  = df_kpi["adr"].mean()
rp_c   = df_kpi["revpar"].mean()
trp_c  = safe(df_kpi["total_revenue"].sum() / (147 * len(df_kpi))) if len(df_kpi) else 0

if _all_years:
    # No single prior year — show "2024–2025" label, no delta
    _kpi_sub = '<span class="pf-kpi-py">2024 – 2025 combined</span>'
    _kpi_rows = {k: ("", _kpi_sub) for k in ["rev","occ","adr","rp","trp"]}
else:
    rev_p  = df_py["total_revenue"].sum()            if not df_py.empty else 0
    occ_p  = df_py["occupancy_rate"].mean() * 100    if not df_py.empty else 0
    adr_p  = df_py["adr"].mean()                     if not df_py.empty else 0
    rp_p   = df_py["revpar"].mean()                  if not df_py.empty else 0
    trp_p  = safe(df_py["total_revenue"].sum() / (147 * len(df_py))) if not df_py.empty else 0

    rc,rd   = delta(rev_c, rev_p)
    oc,od   = delta(occ_c, occ_p)
    ac,ad   = delta(adr_c, adr_p)
    rpc,rpd = delta(rp_c,  rp_p)
    tc,td   = delta(trp_c, trp_p)
    _kpi_rows = {
        "rev": (f'<span class="{rc}">{rd}</span>',
                f'<span class="pf-kpi-py">vs <span>{f"${rev_p/1e6:,.1f}M" if rev_p >= 1e6 else f"${rev_p/1e3:,.1f}k"}</span> in {py_yr}</span>'),
        "occ": (f'<span class="{oc}">{od}</span>',
                f'<span class="pf-kpi-py">vs <span>{occ_p:.1f}%</span> in {py_yr}</span>'),
        "adr": (f'<span class="{ac}">{ad}</span>',
                f'<span class="pf-kpi-py">vs <span>${adr_p:,.0f}</span> in {py_yr}</span>'),
        "rp":  (f'<span class="{rpc}">{rpd}</span>',
                f'<span class="pf-kpi-py">vs <span>${rp_p:,.0f}</span> in {py_yr}</span>'),
        "trp": (f'<span class="{tc}">{td}</span>',
                f'<span class="pf-kpi-py">vs <span>${trp_p:,.0f}</span> in {py_yr}</span>'),
    }

_rev_val = f"${rev_c/1e6:,.1f}M" if rev_c >= 1e6 else f"${rev_c/1e3:,.1f}k"

st.markdown(f"""
<div class="pf-kpi-grid">
  <div class="pf-kpi">
    <div class="pf-kpi-lbl">Total Revenue</div>
    <div class="pf-kpi-val">{_rev_val}</div>
    <div class="pf-kpi-row">{_kpi_rows["rev"][0]}{_kpi_rows["rev"][1]}</div>
  </div>
  <div class="pf-kpi">
    <div class="pf-kpi-lbl">Occupancy</div>
    <div class="pf-kpi-val">{occ_c:.1f}%</div>
    <div class="pf-kpi-row">{_kpi_rows["occ"][0]}{_kpi_rows["occ"][1]}</div>
  </div>
  <div class="pf-kpi">
    <div class="pf-kpi-lbl">ADR</div>
    <div class="pf-kpi-val">${adr_c:,.0f}</div>
    <div class="pf-kpi-row">{_kpi_rows["adr"][0]}{_kpi_rows["adr"][1]}</div>
  </div>
  <div class="pf-kpi">
    <div class="pf-kpi-lbl">RevPAR</div>
    <div class="pf-kpi-val">${rp_c:,.0f}</div>
    <div class="pf-kpi-row">{_kpi_rows["rp"][0]}{_kpi_rows["rp"][1]}</div>
  </div>
  <div class="pf-kpi">
    <div class="pf-kpi-lbl">TRevPAR</div>
    <div class="pf-kpi-val">${trp_c:,.0f}</div>
    <div class="pf-kpi-row">{_kpi_rows["trp"][0]}{_kpi_rows["trp"][1]}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Chart helpers ─────────────────────────────────────────────────────────────
BG, GRID = "#1a1a1a", "rgba(255,255,255,0.05)"
FONT     = "rgba(245,245,240,0.5)"
ACCENT, ACCENT_F = "#e8854a", "rgba(232,133,74,0.22)"
GREEN,  GREEN_F  = "#3ecf8e", "rgba(62,207,142,0.20)"
SLATE,  SLATE_F  = "#4a6fa5", "rgba(74,111,165,0.22)"

def base_layout(h=280, ytitle="", yprefix="", ysuffix=""):
    return dict(
        paper_bgcolor=BG, plot_bgcolor=BG,
        font=dict(family="Inter", color=FONT, size=20),
        margin=dict(l=4, r=4, t=48, b=4), height=h,
        bargap=0.2, bargroupgap=0.08,
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0,
                    orientation="h", x=0, y=1.14,
                    font=dict(size=20, color="rgba(245,245,240,0.6)")),
        xaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)",
                   tickfont=dict(size=18), tickangle=-30),
        yaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)",
                   tickfont=dict(size=18), zeroline=False,
                   title=ytitle, tickprefix=yprefix, ticksuffix=ysuffix),
        hovermode="x unified",
    )

# Auto-granularity (All Years forces monthly so both years compare on same month labels)
n_days   = (end - start).days + 1
freq     = "ME" if _all_years else ("D" if n_days <= 14 else ("W" if n_days <= 60 else "ME"))
dfmt     = "%b"  if _all_years else ("%b %d" if n_days <= 60 else "%b")
_show_py = _all_years  # prior year only shown in All Years mode
_cur_lbl = str(sel_year)
_py_lbl  = str(py_yr)

def agg(d, col, fn, f=None):
    d = d.copy()
    fkey = f or ("D" if freq=="D" else ("W" if freq=="W" else "M"))
    d["p"] = d["business_date"].dt.to_period(fkey)
    return d.groupby("p")[col].agg(fn).reset_index()

def xlabels(periods):
    out = []
    for p in periods:
        try: out.append(p.start_time.strftime(dfmt))
        except: out.append(str(p))
    return out

# ── Revenue & Occupancy charts (side by side) ─────────────────────────────────
st.markdown('<div class="pf-section"><div class="pf-section-ttl">Revenue & Occupancy Trend</div></div>',
            unsafe_allow_html=True)

_col1, _col2 = st.columns(2)

with _col1:
    _rl, _rt = st.columns([3, 1.5])
    with _rl:
        st.markdown('<div class="pf-section-ttl" style="padding:4px 0 2px;">Revenue</div>', unsafe_allow_html=True)
    with _rt:
        rev_t = st.segmented_control("", ["📊", "📈"], default="📊", key="pf_rev_t", label_visibility="collapsed")

    r_cur = agg(df,    "total_revenue", "sum")
    r_py  = agg(df_py, "total_revenue", "sum")
    xl = xlabels(r_cur["p"])
    py_v = (list(r_py["total_revenue"].values) + [0]*len(xl))[:len(xl)]

    fig = go.Figure()
    if rev_t == "📈":
        fig.add_trace(go.Scatter(x=xl, y=r_cur["total_revenue"], name=_cur_lbl,
                                 line=dict(color=ACCENT, width=2), mode="lines+markers",
                                 hovertemplate="$%{y:,.0f}<extra>" + _cur_lbl + "</extra>"))
        if _show_py:
            fig.add_trace(go.Scatter(x=xl, y=py_v, name=_py_lbl,
                                     line=dict(color=ACCENT_F, width=1.5, dash="dot"), mode="lines+markers",
                                     hovertemplate="$%{y:,.0f}<extra>" + _py_lbl + "</extra>"))
    else:
        fig.add_trace(go.Bar(x=xl, y=r_cur["total_revenue"], name=_cur_lbl,
                             marker_color=ACCENT,
                             hovertemplate="$%{y:,.0f}<extra>" + _cur_lbl + "</extra>"))
        if _show_py:
            fig.add_trace(go.Bar(x=xl, y=py_v, name=_py_lbl,
                                 marker_color=ACCENT_F, marker_line=dict(color=ACCENT, width=1),
                                 hovertemplate="$%{y:,.0f}<extra>" + _py_lbl + "</extra>"))
    lay = base_layout(h=260, yprefix="$")
    fig.update_layout(**lay)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

with _col2:
    _ol, _ot = st.columns([3, 1.5])
    with _ol:
        st.markdown('<div class="pf-section-ttl" style="padding:4px 0 2px;">Occupancy</div>', unsafe_allow_html=True)
    with _ot:
        occ_t = st.segmented_control("", ["📊", "📈"], default="📊", key="pf_occ_t", label_visibility="collapsed")

    o_cur = agg(df,    "occupancy_rate", "mean")
    o_py  = agg(df_py, "occupancy_rate", "mean")
    py_ov = (list((o_py["occupancy_rate"]*100).values) + [0]*len(xl))[:len(xl)]

    fig2 = go.Figure()
    if occ_t == "📈":
        fig2.add_trace(go.Scatter(x=xl, y=o_cur["occupancy_rate"]*100, name=_cur_lbl,
                                  line=dict(color=GREEN, width=2), mode="lines+markers",
                                  hovertemplate="%{y:.1f}%<extra>" + _cur_lbl + "</extra>"))
        if _show_py:
            fig2.add_trace(go.Scatter(x=xl, y=py_ov, name=_py_lbl,
                                      line=dict(color=GREEN_F, width=1.5, dash="dot"), mode="lines+markers",
                                      hovertemplate="%{y:.1f}%<extra>" + _py_lbl + "</extra>"))
    else:
        fig2.add_trace(go.Bar(x=xl, y=o_cur["occupancy_rate"]*100, name=_cur_lbl,
                              marker_color=GREEN,
                              hovertemplate="%{y:.1f}%<extra>" + _cur_lbl + "</extra>"))
        if _show_py:
            fig2.add_trace(go.Bar(x=xl, y=py_ov, name=_py_lbl,
                                  marker_color=GREEN_F, marker_line=dict(color=GREEN, width=1),
                                  hovertemplate="%{y:.1f}%<extra>" + _py_lbl + "</extra>"))
    lay2 = base_layout(h=260, ysuffix="%")
    lay2["yaxis"]["range"] = [0, 105]
    fig2.update_layout(**lay2)
    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

# ── ADR & RevPAR ──────────────────────────────────────────────────────────────
_col3, _col4 = st.columns(2)

with _col3:
    _al, _at = st.columns([3, 1.5])
    with _al:
        st.markdown('<div class="pf-section-ttl" style="padding:4px 0 2px;">ADR</div>', unsafe_allow_html=True)
    with _at:
        adr_t = st.segmented_control("", ["📊", "📈"], default="📈", key="pf_adr_t", label_visibility="collapsed")

    a_cur = agg(df,    "adr", "mean")
    a_py  = agg(df_py, "adr", "mean")
    py_av = (list(a_py["adr"].values) + [0]*len(xl))[:len(xl)]

    fig3 = go.Figure()
    if adr_t == "📊":
        fig3.add_trace(go.Bar(x=xl, y=a_cur["adr"], name=_cur_lbl,
                              marker_color=ACCENT,
                              hovertemplate="$%{y:,.0f}<extra>" + _cur_lbl + "</extra>"))
        if _show_py:
            fig3.add_trace(go.Bar(x=xl, y=py_av, name=_py_lbl,
                                  marker_color=ACCENT_F, marker_line=dict(color=ACCENT, width=1),
                                  hovertemplate="$%{y:,.0f}<extra>" + _py_lbl + "</extra>"))
    else:
        fig3.add_trace(go.Scatter(x=xl, y=a_cur["adr"], name=_cur_lbl,
                                  line=dict(color=ACCENT, width=2),
                                  hovertemplate="$%{y:,.0f}<extra>" + _cur_lbl + "</extra>"))
        if _show_py:
            fig3.add_trace(go.Scatter(x=xl, y=py_av, name=_py_lbl,
                                      line=dict(color=ACCENT_F, width=1.5, dash="dot"),
                                      hovertemplate="$%{y:,.0f}<extra>" + _py_lbl + "</extra>"))
    lay3 = base_layout(h=220, yprefix="$")
    fig3.update_layout(**lay3)
    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})

with _col4:
    _rl2, _rt2 = st.columns([3, 1.5])
    with _rl2:
        st.markdown('<div class="pf-section-ttl" style="padding:4px 0 2px;">RevPAR</div>', unsafe_allow_html=True)
    with _rt2:
        rp_t = st.segmented_control("", ["📊", "📈"], default="📈", key="pf_rp_t", label_visibility="collapsed")

    rp_cur = agg(df,    "revpar", "mean")
    rp_py  = agg(df_py, "revpar", "mean")
    py_rpv = (list(rp_py["revpar"].values) + [0]*len(xl))[:len(xl)]

    fig4 = go.Figure()
    if rp_t == "📊":
        fig4.add_trace(go.Bar(x=xl, y=rp_cur["revpar"], name=_cur_lbl,
                              marker_color=SLATE,
                              hovertemplate="$%{y:,.0f}<extra>" + _cur_lbl + "</extra>"))
        if _show_py:
            fig4.add_trace(go.Bar(x=xl, y=py_rpv, name=_py_lbl,
                                  marker_color=SLATE_F, marker_line=dict(color=SLATE, width=1),
                                  hovertemplate="$%{y:,.0f}<extra>" + _py_lbl + "</extra>"))
    else:
        fig4.add_trace(go.Scatter(x=xl, y=rp_cur["revpar"], name=_cur_lbl,
                                  line=dict(color=SLATE, width=2),
                                  hovertemplate="$%{y:,.0f}<extra>" + _cur_lbl + "</extra>"))
        if _show_py:
            fig4.add_trace(go.Scatter(x=xl, y=py_rpv, name=_py_lbl,
                                      line=dict(color=SLATE_F, width=1.5, dash="dot"),
                                      hovertemplate="$%{y:,.0f}<extra>" + _py_lbl + "</extra>"))
    lay4 = base_layout(h=220, yprefix="$")
    fig4.update_layout(**lay4)
    st.plotly_chart(fig4, use_container_width=True, config={"displayModeBar": False})

# ── Day of week occupancy ─────────────────────────────────────────────────────
st.markdown('<div class="pf-section"><div class="pf-section-ttl">Occupancy by Day of Week</div></div>',
            unsafe_allow_html=True)

_dow_col, _ = st.columns(2)
with _dow_col:
    _dl, _dt = st.columns([3, 1.5])
    with _dl:
        st.markdown('<div class="pf-section-ttl" style="padding:4px 0 2px;">By Day of Week</div>', unsafe_allow_html=True)
    with _dt:
        dow_t = st.segmented_control("", ["📊", "📈"], default="📊", key="pf_dow_t", label_visibility="collapsed")

    dow_cur = df.groupby("day_of_week")["occupancy_rate"].mean() * 100
    dow_py  = df_py.groupby("day_of_week")["occupancy_rate"].mean() * 100
    DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    days = list(range(7))
    xl_dow = [DOW_LABELS[i] for i in days]
    yc     = [float(dow_cur.get(i, 0)) for i in days]
    yp     = [float(dow_py.get(i, 0)) for i in days]

    fig_dow = go.Figure()
    if dow_t == "📈":
        fig_dow.add_trace(go.Scatter(x=xl_dow, y=yc, name=_cur_lbl,
                                     line=dict(color=GREEN, width=2), mode="lines+markers",
                                     hovertemplate="%{x}: %{y:.1f}%<extra>" + _cur_lbl + "</extra>"))
        if _show_py:
            fig_dow.add_trace(go.Scatter(x=xl_dow, y=yp, name=_py_lbl,
                                         line=dict(color=GREEN_F, width=1.5, dash="dot"), mode="lines+markers",
                                         hovertemplate="%{x}: %{y:.1f}%<extra>" + _py_lbl + "</extra>"))
    else:
        fig_dow.add_trace(go.Bar(x=xl_dow, y=yc, name=_cur_lbl, marker_color=GREEN,
                                 hovertemplate="%{x}: %{y:.1f}%<extra>" + _cur_lbl + "</extra>"))
        if _show_py:
            fig_dow.add_trace(go.Bar(x=xl_dow, y=yp, name=_py_lbl, marker_color=GREEN_F,
                                     marker_line=dict(color=GREEN, width=1),
                                     hovertemplate="%{x}: %{y:.1f}%<extra>" + _py_lbl + "</extra>"))
    dow_lay = base_layout(h=220, ysuffix="%")
    dow_lay["bargroupgap"] = 0.1
    dow_lay["xaxis"]["tickangle"] = 0
    fig_dow.update_layout(**dow_lay)
    st.plotly_chart(fig_dow, use_container_width=True, config={"displayModeBar": False})

# ── Guest satisfaction ────────────────────────────────────────────────────────
_MED_METRICS = {
    "Overall":     ("medallia_overall_satisfaction",      ACCENT),
    "Cleanliness": ("medallia_hotel_cleanliness",         GREEN),
    "Recommend":   ("medallia_likelihood_to_recommend",   SLATE),
    "Value":       ("medallia_value_for_price",           "#9b6fd4"),
    "Return":      ("medallia_likelihood_to_return",      "#d4903a"),
}

_sat_master = master[master["medallia_overall_satisfaction"].notna()].copy()

if not _sat_master.empty:
    st.markdown(
        '<div style="padding:22px 20px 10px;">'
        '<div class="pf-section-ttl">Guest Satisfaction — Medallia · click a card to analyze</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.markdown("""<style>
    @keyframes satFadeIn {
      from { opacity:0; transform:translateY(-8px); }
      to   { opacity:1; transform:translateY(0); }
    }
    .sat-expanded {
      animation: satFadeIn .2s ease;
      margin: 10px 0 0;
      padding: 16px 18px;
      background: #111111;
      border-radius: 6px;
    }
    </style>""", unsafe_allow_html=True)

    if "pf_sat_open" not in st.session_state:
        st.session_state["pf_sat_open"] = None
    if "pf_sat_exp_hz" not in st.session_state:
        st.session_state["pf_sat_exp_hz"] = "Full Year"

    # Mini sparkline always uses full 2024-2025 monthly data
    def _mini_series(col):
        d = _sat_master[_sat_master[col].notna()].copy()
        d["mo"] = d["business_date"].dt.to_period("M")
        return list(d.groupby("mo")[col].mean().values)

    # ── 5 stock-style mini cards ──────────────────────────────────────────────
    _sat_cols = st.columns(5)
    for _i, (label, (col, color)) in enumerate(_MED_METRICS.items()):
        if col not in _sat_master.columns:
            continue
        with _sat_cols[_i]:
            _score = _sat_master[col].mean()
            _is_open = st.session_state["pf_sat_open"] == label
            _bdr     = f"border:2px solid {color};" if _is_open else "border:1px solid rgba(255,255,255,0.08);"

            # Score header
            st.markdown(
                f'<div style="background:#111111;{_bdr}border-radius:6px 6px 0 0;'
                f'padding:12px 14px 2px;text-align:center;">'
                f'<div style="font-size:26px;font-weight:300;color:{color};letter-spacing:-.02em;">{_score:.1f}</div>'
                f'<div style="font-size:8px;font-weight:600;letter-spacing:.14em;text-transform:uppercase;'
                f'color:rgba(245,245,240,0.35);margin-top:2px;">{label}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

            # Stock-style mini chart — full year trend, no axes
            _yv = _mini_series(col)
            if _yv:
                _color_fill = color.replace(")", ", 0.15)").replace("rgb", "rgba") if "rgb" in color else color
                _fig_mini = go.Figure()
                _fig_mini.add_trace(go.Scatter(
                    y=_yv, mode="lines",
                    line=dict(color=color, width=1.8),
                    fill="tozeroy",
                    fillcolor=f"rgba(255,255,255,0.04)",
                    hoverinfo="skip",
                ))
                _fig_mini.update_layout(
                    paper_bgcolor="#111111",
                    plot_bgcolor="#111111",
                    margin=dict(l=0, r=0, t=0, b=0),
                    height=70,
                    showlegend=False,
                    xaxis=dict(visible=False, fixedrange=True),
                    yaxis=dict(visible=False, fixedrange=True, range=[4, 10]),
                )
                st.plotly_chart(_fig_mini, use_container_width=True,
                                config={"displayModeBar": False}, key=f"mini_{label}")

            # Expand toggle
            _btn_lbl = "▲ Close" if _is_open else "▼ Analyze"
            if st.button(_btn_lbl, key=f"sat_open_{label}", use_container_width=True):
                st.session_state["pf_sat_open"] = None if _is_open else label
                st.rerun()

    # ── Expanded chart with independent controls ──────────────────────────────
    _open_metric = st.session_state.get("pf_sat_open")
    if _open_metric and _open_metric in _MED_METRICS:
        _open_col, _open_color = _MED_METRICS[_open_metric]

        st.markdown(f'<div class="sat-expanded" style="border-top:2px solid {_open_color};">',
                    unsafe_allow_html=True)

        # Independent controls — horizon only, year fixed to 2025
        st.markdown('<div style="height:6px;"></div>', unsafe_allow_html=True)
        _exp_hz = st.segmented_control(
            "Horizon", ["Q1","Q2","Q3","Q4","Full Year","All Time"],
            default=st.session_state["pf_sat_exp_hz"],
            key="pf_sat_exp_hz_ctrl", label_visibility="collapsed",
        ) or "Full Year"
        st.session_state["pf_sat_exp_hz"] = _exp_hz
        _exp_yr = 2025

        # Compute date range for expanded chart
        def _exp_dates(h, yr):
            if h == "Q1":       return date(yr, 1, 1),   date(yr, 3, 31)
            if h == "Q2":       return date(yr, 4, 1),   date(yr, 6, 30)
            if h == "Q3":       return date(yr, 7, 1),   date(yr, 9, 30)
            if h == "Q4":       return date(yr, 10, 1),  date(yr, 12, 31)
            return date(yr, 1, 1), date(yr, 12, 31)

        if _exp_hz == "All Time":
            # Full 24-month continuous timeline, no prior-year overlay
            _es       = _sat_master["business_date"].min().date()
            _ee       = _sat_master["business_date"].max().date()
            _eyr      = int(_ee.year)
            _eyr_py   = _eyr - 1
            _eps, _epe = date(1900, 1, 1), date(1900, 1, 1)   # yields empty prior year
            _en_days  = (_ee - _es).days + 1
            _efkey, _edfmt = "M", "%b '%y"
        else:
            _es, _ee = _exp_dates(_exp_hz, _exp_yr)
            _eyr, _eyr_py = _exp_yr, _exp_yr - 1
            _eps, _epe = date(_eyr_py, _es.month, _es.day), date(_eyr_py, _ee.month, _ee.day)
            _en_days = (_ee - _es).days + 1
            _efkey   = "D" if _en_days <= 14 else ("W" if _en_days <= 60 else "M")
            _edfmt   = "%b %d" if _en_days <= 60 else "%b"

        def _exp_agg(yr_start, yr_end):
            d = _sat_master[
                (_sat_master["business_date"] >= pd.Timestamp(yr_start)) &
                (_sat_master["business_date"] <= pd.Timestamp(yr_end)) &
                (_sat_master[_open_col].notna())
            ].copy()
            if d.empty:
                return [], []
            d["p"] = d["business_date"].dt.to_period(_efkey)
            g = d.groupby("p")[_open_col].mean()
            lbs = []
            for p in g.index:
                try: lbs.append(p.start_time.strftime(_edfmt))
                except: lbs.append(str(p))
            return list(g.values), lbs

        _yv_cur, _xl_cur = _exp_agg(_es, _ee)
        _yv_py,  _xl_py  = _exp_agg(_eps, _epe)

        # Faded version of the metric color for prior year
        def _to_faded(hex_c, alpha=0.38):
            h = hex_c.lstrip("#")
            r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
            return f"rgba({r},{g},{b},{alpha})"
        _faded_color = _to_faded(_open_color)

        _lbl_cur = "All Time" if _exp_hz == "All Time" else str(_eyr)
        _lbl_py  = str(_eyr_py)
        _show_py_exp = bool(_yv_py) and _exp_hz != "All Time"

        _fig_exp = go.Figure()
        _fig_exp.add_hline(y=8.0, line_dash="dot",
                           line_color="rgba(255,255,255,0.12)",
                           annotation_text="8.0", annotation_position="right",
                           annotation_font=dict(size=18, color="rgba(255,255,255,0.3)"))

        # Thin dashed trend lines connecting the numbers
        if _yv_cur:
            _fig_exp.add_trace(go.Scatter(
                x=_xl_cur, y=_yv_cur, mode="lines",
                line=dict(color=_open_color, width=1, dash="dot"),
                showlegend=False, hoverinfo="skip", connectgaps=True,
            ))
        if _show_py_exp:
            _fig_exp.add_trace(go.Scatter(
                x=_xl_cur[:len(_yv_py)], y=_yv_py, mode="lines",
                line=dict(color=_faded_color, width=1, dash="dot"),
                showlegend=False, hoverinfo="skip", connectgaps=True,
            ))

        # Current year — large numbers, full color, above the dot
        _fig_exp.add_trace(go.Scatter(
            x=_xl_cur, y=_yv_cur,
            mode="markers+text",
            text=[f"{v:.1f}" if v is not None else "" for v in _yv_cur],
            textposition="top center",
            textfont=dict(size=15, color=_open_color, family="Inter"),
            marker=dict(size=5, color=_open_color),
            name=_lbl_cur,
            hovertemplate=f"{_open_metric}: %{{y:.2f}}<extra>{_lbl_cur}</extra>",
            connectgaps=True,
        ))

        # Prior year — small numbers, faded, below the dot
        if _show_py_exp:
            _fig_exp.add_trace(go.Scatter(
                x=_xl_cur[:len(_yv_py)], y=_yv_py,
                mode="markers+text",
                text=[f"{v:.1f}" if v is not None else "" for v in _yv_py],
                textposition="bottom center",
                textfont=dict(size=20, color=_faded_color, family="Inter"),
                marker=dict(size=3, color=_faded_color),
                name=_lbl_py,
                hovertemplate=f"{_open_metric}: %{{y:.2f}}<extra>{_lbl_py}</extra>",
                connectgaps=True,
            ))

        _exp_lay = base_layout(h=320)
        _exp_lay["margin"]["t"] = 70
        _exp_lay["yaxis"]["range"]     = [3.5, 11.5]
        _exp_lay["yaxis"]["tickvals"]  = [4, 5, 6, 7, 8, 9, 10]
        _exp_lay["xaxis"]["tickangle"] = -30 if _en_days > 60 else 0
        _exp_lay["title"] = dict(
            text=f"{_open_metric} — score / 10  ·  {_lbl_cur} (full) vs {_lbl_py} (faded)"
                 if _show_py_exp else f"{_open_metric} — score / 10",
            font=dict(size=20, color="rgba(245,245,240,0.45)"),
            x=0, xanchor="left", pad=dict(l=4),
        )
        _fig_exp.update_layout(**_exp_lay)
        st.plotly_chart(_fig_exp, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

# ── Market segment ────────────────────────────────────────────────────────────
st.markdown('<div class="pf-section"><div class="pf-section-ttl">Revenue by Market Segment</div></div>',
            unsafe_allow_html=True)

mkt_cur = (mkt_df[(mkt_df["business_date"] >= s_ts) & (mkt_df["business_date"] <= e_ts)]
           .groupby("market_code")["total_revenue"].sum()
           .nlargest(10).sort_values())
mkt_py  = (mkt_df[(mkt_df["business_date"] >= py_s) & (mkt_df["business_date"] <= py_e)]
           .groupby("market_code")["total_revenue"].sum())

mkt_labels = [MARKET_NAMES.get(c, c) for c in mkt_cur.index]
mkt_py_v   = [float(mkt_py.get(c, 0)) for c in mkt_cur.index]

fig_mkt = go.Figure()
fig_mkt.add_trace(go.Bar(y=mkt_labels, x=mkt_cur.values, orientation="h",
                         name=_cur_lbl, marker_color=SLATE,
                         hovertemplate="%{y}: $%{x:,.0f}<extra>" + _cur_lbl + "</extra>"))
if _show_py:
    fig_mkt.add_trace(go.Bar(y=mkt_labels, x=mkt_py_v, orientation="h",
                             name=_py_lbl, marker_color=SLATE_F,
                             marker_line=dict(color=SLATE, width=1),
                             hovertemplate="%{y}: $%{x:,.0f}<extra>" + _py_lbl + "</extra>"))
mkt_lay = base_layout(h=320)
mkt_lay["barmode"] = "group"
mkt_lay["margin"]["l"] = 160
mkt_lay["xaxis"].update({"tickprefix": "$", "tickformat": ",.0f"})
fig_mkt.update_layout(**mkt_lay)
st.plotly_chart(fig_mkt, use_container_width=True, config={"displayModeBar": False})

st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
