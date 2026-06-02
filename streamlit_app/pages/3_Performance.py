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
.pf-sub    { font-size:11px; color:var(--muted); }
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
.up   { font-size:10px; font-weight:600; color:var(--green); }
.down { font-size:10px; font-weight:600; color:var(--red); }

.pf-section { padding:16px 20px 0; }
.pf-section-ttl { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }

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
HORIZONS = ["1W", "1M", "Q1", "Q2", "Q3", "Q4", "Full Year", "All Years", "Custom"]
if "pf_hz" not in st.session_state:
    st.session_state["pf_hz"] = "Full Year"

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
    s_ts     = master["business_date"].min()
    e_ts     = master["business_date"].max()
    start    = s_ts.date()
    end      = e_ts.date()
    sel_year = int(e_ts.year)
    py_yr    = None
    py_s     = pd.Timestamp("2099-01-01")  # no prior year — will yield empty df_py
    py_e     = pd.Timestamp("2099-01-01")
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

# ── KPI helpers ───────────────────────────────────────────────────────────────
def safe(v, fallback=0.0):
    return float(v) if (v is not None and not np.isnan(v)) else fallback

def delta(v, base):
    d   = v - base
    pct = (d / abs(base) * 100) if base else 0
    cls = "up" if d >= 0 else "down"
    return cls, f"{'▲' if d>=0 else '▼'} {abs(pct):.1f}%"

rev_c  = df["total_revenue"].sum()
occ_c  = df["occupancy_rate"].mean() * 100
adr_c  = df["adr"].mean()
rp_c   = df["revpar"].mean()
trp_c  = safe(df["total_revenue"].sum() / (147 * len(df))) if len(df) else 0

rev_p  = df_py["total_revenue"].sum()   if not df_py.empty else 0
occ_p  = df_py["occupancy_rate"].mean() * 100 if not df_py.empty else 0
adr_p  = df_py["adr"].mean()            if not df_py.empty else 0
rp_p   = df_py["revpar"].mean()         if not df_py.empty else 0
trp_p  = safe(df_py["total_revenue"].sum() / (147 * len(df_py))) if not df_py.empty else 0

if _all_years:
    _kpi_sub = '<span class="pf-kpi-py">2024 – 2025 combined</span>'
    _kpi_rows = {k: ("", _kpi_sub) for k in ["rev","occ","adr","rp","trp"]}
else:
    rc,rd   = delta(rev_c, rev_p)
    oc,od   = delta(occ_c, occ_p)
    ac,ad   = delta(adr_c, adr_p)
    rpc,rpd = delta(rp_c,  rp_p)
    tc,td   = delta(trp_c, trp_p)
    _py_lbl = str(py_yr)
    _kpi_rows = {
        "rev": (f'<span class="{rc}">{rd}</span>',
                f'<span class="pf-kpi-py">vs <span>{f"${rev_p/1e6:,.1f}M" if rev_p >= 1e6 else f"${rev_p/1e3:,.1f}k"}</span> in {_py_lbl}</span>'),
        "occ": (f'<span class="{oc}">{od}</span>',
                f'<span class="pf-kpi-py">vs <span>{occ_p:.1f}%</span> in {_py_lbl}</span>'),
        "adr": (f'<span class="{ac}">{ad}</span>',
                f'<span class="pf-kpi-py">vs <span>${adr_p:,.0f}</span> in {_py_lbl}</span>'),
        "rp":  (f'<span class="{rpc}">{rpd}</span>',
                f'<span class="pf-kpi-py">vs <span>${rp_p:,.0f}</span> in {_py_lbl}</span>'),
        "trp": (f'<span class="{tc}">{td}</span>',
                f'<span class="pf-kpi-py">vs <span>${trp_p:,.0f}</span> in {_py_lbl}</span>'),
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
        font=dict(family="Inter", color=FONT, size=10),
        margin=dict(l=4, r=4, t=28, b=4), height=h,
        bargap=0.2, bargroupgap=0.08,
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0,
                    orientation="h", x=0, y=1.14,
                    font=dict(size=10, color="rgba(245,245,240,0.6)")),
        xaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)",
                   tickfont=dict(size=9), tickangle=-30),
        yaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)",
                   tickfont=dict(size=9), zeroline=False,
                   title=ytitle, tickprefix=yprefix, ticksuffix=ysuffix),
        hovermode="x unified",
    )

# Auto-granularity
n_days = (end - start).days + 1
freq   = "D" if n_days <= 14 else ("W" if n_days <= 60 else "ME")
dfmt   = "%b %d" if n_days <= 60 else "%b"

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
        fig.add_trace(go.Scatter(x=xl, y=r_cur["total_revenue"], name=str(sel_year),
                                 line=dict(color=ACCENT, width=2), mode="lines+markers",
                                 hovertemplate="$%{y:,.0f}<extra>" + str(sel_year) + "</extra>"))
        fig.add_trace(go.Scatter(x=xl, y=py_v, name=str(py_yr),
                                 line=dict(color=ACCENT_F, width=1.5, dash="dot"), mode="lines+markers",
                                 hovertemplate="$%{y:,.0f}<extra>" + str(py_yr) + "</extra>"))
    else:
        fig.add_trace(go.Bar(x=xl, y=r_cur["total_revenue"], name=str(sel_year),
                             marker_color=ACCENT,
                             hovertemplate="$%{y:,.0f}<extra>" + str(sel_year) + "</extra>"))
        fig.add_trace(go.Bar(x=xl, y=py_v, name=str(py_yr),
                             marker_color=ACCENT_F, marker_line=dict(color=ACCENT, width=1),
                             hovertemplate="$%{y:,.0f}<extra>" + str(py_yr) + "</extra>"))
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
        fig2.add_trace(go.Scatter(x=xl, y=o_cur["occupancy_rate"]*100, name=str(sel_year),
                                  line=dict(color=GREEN, width=2), mode="lines+markers",
                                  hovertemplate="%{y:.1f}%<extra>" + str(sel_year) + "</extra>"))
        fig2.add_trace(go.Scatter(x=xl, y=py_ov, name=str(py_yr),
                                  line=dict(color=GREEN_F, width=1.5, dash="dot"), mode="lines+markers",
                                  hovertemplate="%{y:.1f}%<extra>" + str(py_yr) + "</extra>"))
    else:
        fig2.add_trace(go.Bar(x=xl, y=o_cur["occupancy_rate"]*100, name=str(sel_year),
                              marker_color=GREEN,
                              hovertemplate="%{y:.1f}%<extra>" + str(sel_year) + "</extra>"))
        fig2.add_trace(go.Bar(x=xl, y=py_ov, name=str(py_yr),
                              marker_color=GREEN_F, marker_line=dict(color=GREEN, width=1),
                              hovertemplate="%{y:.1f}%<extra>" + str(py_yr) + "</extra>"))
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
        fig3.add_trace(go.Bar(x=xl, y=a_cur["adr"], name=str(sel_year),
                              marker_color=ACCENT,
                              hovertemplate="$%{y:,.0f}<extra>" + str(sel_year) + "</extra>"))
        fig3.add_trace(go.Bar(x=xl, y=py_av, name=str(py_yr),
                              marker_color=ACCENT_F, marker_line=dict(color=ACCENT, width=1),
                              hovertemplate="$%{y:,.0f}<extra>" + str(py_yr) + "</extra>"))
    else:
        fig3.add_trace(go.Scatter(x=xl, y=a_cur["adr"], name=str(sel_year),
                                  line=dict(color=ACCENT, width=2),
                                  hovertemplate="$%{y:,.0f}<extra>" + str(sel_year) + "</extra>"))
        fig3.add_trace(go.Scatter(x=xl, y=py_av, name=str(py_yr),
                                  line=dict(color=ACCENT_F, width=1.5, dash="dot"),
                                  hovertemplate="$%{y:,.0f}<extra>" + str(py_yr) + "</extra>"))
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
        fig4.add_trace(go.Bar(x=xl, y=rp_cur["revpar"], name=str(sel_year),
                              marker_color=SLATE,
                              hovertemplate="$%{y:,.0f}<extra>" + str(sel_year) + "</extra>"))
        fig4.add_trace(go.Bar(x=xl, y=py_rpv, name=str(py_yr),
                              marker_color=SLATE_F, marker_line=dict(color=SLATE, width=1),
                              hovertemplate="$%{y:,.0f}<extra>" + str(py_yr) + "</extra>"))
    else:
        fig4.add_trace(go.Scatter(x=xl, y=rp_cur["revpar"], name=str(sel_year),
                                  line=dict(color=SLATE, width=2),
                                  hovertemplate="$%{y:,.0f}<extra>" + str(sel_year) + "</extra>"))
        fig4.add_trace(go.Scatter(x=xl, y=py_rpv, name=str(py_yr),
                                  line=dict(color=SLATE_F, width=1.5, dash="dot"),
                                  hovertemplate="$%{y:,.0f}<extra>" + str(py_yr) + "</extra>"))
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
        fig_dow.add_trace(go.Scatter(x=xl_dow, y=yc, name=str(sel_year),
                                     line=dict(color=GREEN, width=2), mode="lines+markers",
                                     hovertemplate="%{x}: %{y:.1f}%<extra>" + str(sel_year) + "</extra>"))
        fig_dow.add_trace(go.Scatter(x=xl_dow, y=yp, name=str(py_yr),
                                     line=dict(color=GREEN_F, width=1.5, dash="dot"), mode="lines+markers",
                                     hovertemplate="%{x}: %{y:.1f}%<extra>" + str(py_yr) + "</extra>"))
    else:
        fig_dow.add_trace(go.Bar(x=xl_dow, y=yc, name=str(sel_year), marker_color=GREEN,
                                 hovertemplate="%{x}: %{y:.1f}%<extra>" + str(sel_year) + "</extra>"))
        fig_dow.add_trace(go.Bar(x=xl_dow, y=yp, name=str(py_yr), marker_color=GREEN_F,
                                 marker_line=dict(color=GREEN, width=1),
                                 hovertemplate="%{x}: %{y:.1f}%<extra>" + str(py_yr) + "</extra>"))
    dow_lay = base_layout(h=220, ysuffix="%")
    dow_lay["bargroupgap"] = 0.1
    dow_lay["xaxis"]["tickangle"] = 0
    fig_dow.update_layout(**dow_lay)
    st.plotly_chart(fig_dow, use_container_width=True, config={"displayModeBar": False})

# ── OOO by room type ─────────────────────────────────────────────────────────
st.markdown('<div class="pf-section"><div class="pf-section-ttl">Out of Order Rooms — Trend by Room Type</div></div>',
            unsafe_allow_html=True)

_rt_df = load_room_type_stats()
if not _rt_df.empty:
    _rt_f = _rt_df[
        (_rt_df["business_date"] >= s_ts) & (_rt_df["business_date"] <= e_ts)
    ].copy()

    _all_rts = sorted(_rt_f["room_type"].unique())

    # Room type selector + view toggle
    _sel_col, _view_col, _ = st.columns([3, 2, 3])
    with _sel_col:
        _sel_rts = st.multiselect(
            "Room types", _all_rts, default=_all_rts, key="ooo_rts",
            label_visibility="collapsed",
        )
    with _view_col:
        _ooo_view = st.segmented_control(
            "View", ["Stacked", "Per Room"], default="Stacked",
            key="ooo_view", label_visibility="collapsed",
        )

    _sel_rts = _sel_rts or _all_rts

    # Aggregate to the same time granularity as other charts
    _rt_f["p"] = _rt_f["business_date"].dt.to_period(
        "D" if n_days <= 14 else ("W" if n_days <= 60 else "M")
    )
    _rt_agg = (
        _rt_f[_rt_f["room_type"].isin(_sel_rts)]
        .groupby(["room_type", "p"])["ooo_rooms"]
        .sum()
        .reset_index()
    )
    _xl_ooo = sorted(_rt_agg["p"].unique())
    _xl_labels = []
    for p in _xl_ooo:
        try:    _xl_labels.append(p.start_time.strftime(dfmt))
        except: _xl_labels.append(str(p))

    # Colour palette for room types
    _RT_COLORS = [
        "#e8854a","#3ecf8e","#4a6fa5","#d4903a","#e05252",
        "#9b59b6","#1abc9c","#f39c12","#2980b9","#c0392b",
        "#27ae60","#8e44ad",
    ]
    _rt_color = {rt: _RT_COLORS[i % len(_RT_COLORS)] for i, rt in enumerate(_all_rts)}

    _ooo_col1, _ooo_col2 = st.columns([2, 1])

    with _ooo_col1:
        fig_ooo = go.Figure()
        for rt in _sel_rts:
            _rt_vals = _rt_agg[_rt_agg["room_type"] == rt]
            _yvals = []
            for p in _xl_ooo:
                row = _rt_vals[_rt_vals["p"] == p]
                _yvals.append(float(row["ooo_rooms"].values[0]) if len(row) else 0)

            _trace_kw = dict(
                x=_xl_labels, y=_yvals, name=rt,
                marker_color=_rt_color[rt],
                hovertemplate=f"{rt}: %{{y:.0f}} rooms<extra></extra>",
            )
            if _ooo_view == "Stacked":
                fig_ooo.add_trace(go.Bar(**_trace_kw))
            else:
                fig_ooo.add_trace(go.Scatter(
                    x=_xl_labels, y=_yvals, name=rt,
                    line=dict(color=_rt_color[rt], width=1.8),
                    hovertemplate=f"{rt}: %{{y:.0f}} rooms<extra></extra>",
                ))

        _ooo_lay = base_layout(h=280, ytitle="OOO Rooms")
        _ooo_lay["title"] = dict(
            text="Out of Order Rooms",
            font=dict(size=11, color="rgba(245,245,240,0.7)"), x=0, xanchor="left", pad=dict(l=4),
        )
        if _ooo_view == "Stacked":
            _ooo_lay["barmode"] = "stack"
        fig_ooo.update_layout(**_ooo_lay)
        st.plotly_chart(fig_ooo, use_container_width=True, config={"displayModeBar": False})

    with _ooo_col2:
        # Summary table: avg daily OOO, max OOO, % of physical rooms
        _rt_summary = (
            _rt_f[_rt_f["room_type"].isin(_sel_rts)]
            .groupby("room_type")
            .agg(
                Physical=("total_physical_rooms", "mean"),
                Avg_OOO=("ooo_rooms", "mean"),
                Max_OOO=("ooo_rooms", "max"),
                OOO_Days=("ooo_rooms", lambda x: int((x > 0).sum())),
            )
            .round(1)
            .reset_index()
            .rename(columns={"room_type": "Room", "Avg_OOO": "Avg OOO",
                              "Max_OOO": "Max OOO", "OOO_Days": "Days w/ OOO"})
            .sort_values("Avg OOO", ascending=False)
        )
        _rt_summary["Physical"] = _rt_summary["Physical"].astype(int)
        _rt_summary["Max OOO"]  = _rt_summary["Max OOO"].astype(int)

        # % of physical rooms column
        _rt_summary["% Phys"] = (
            _rt_summary["Avg OOO"] / _rt_summary["Physical"].replace(0, float("nan")) * 100
        ).round(1)

        # Totals row
        _tot_phys    = int(_rt_summary["Physical"].sum())
        _tot_avg_ooo = round(float(_rt_summary["Avg OOO"].sum()), 1)
        _tot_max_ooo = int(_rt_summary["Max OOO"].sum())
        _tot_days    = int(_rt_summary["Days w/ OOO"].sum())
        _tot_pct     = round(_tot_avg_ooo / _tot_phys * 100, 1) if _tot_phys else 0.0

        _rt_display = pd.concat([
            _rt_summary,
            pd.DataFrame([{
                "Room": "TOTAL",
                "Physical": _tot_phys,
                "Avg OOO": _tot_avg_ooo,
                "Max OOO": _tot_max_ooo,
                "Days w/ OOO": _tot_days,
                "% Phys": _tot_pct,
            }]),
        ], ignore_index=True)

        st.dataframe(
            _rt_display,
            column_config={
                "Room":        st.column_config.TextColumn("Room"),
                "Physical":    st.column_config.NumberColumn("Physical", format="%d"),
                "Avg OOO":     st.column_config.NumberColumn("Avg OOO", format="%.1f"),
                "Max OOO":     st.column_config.NumberColumn("Max OOO", format="%d"),
                "Days w/ OOO": st.column_config.NumberColumn("Days w/ OOO", format="%d"),
                "% Phys":      st.column_config.NumberColumn("% Phys", format="%.1f%%"),
            },
            use_container_width=True,
            hide_index=True,
            height=400,
        )

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
                         name=str(sel_year), marker_color=SLATE,
                         hovertemplate="%{y}: $%{x:,.0f}<extra>" + str(sel_year) + "</extra>"))
fig_mkt.add_trace(go.Bar(y=mkt_labels, x=mkt_py_v, orientation="h",
                         name=str(py_yr), marker_color=SLATE_F,
                         marker_line=dict(color=SLATE, width=1),
                         hovertemplate="%{y}: $%{x:,.0f}<extra>" + str(py_yr) + "</extra>"))
mkt_lay = base_layout(h=320)
mkt_lay["barmode"] = "group"
mkt_lay["margin"]["l"] = 160
mkt_lay["xaxis"].update({"tickprefix": "$", "tickformat": ",.0f"})
fig_mkt.update_layout(**mkt_lay)
st.plotly_chart(fig_mkt, use_container_width=True, config={"displayModeBar": False})

# ── Channel mix ───────────────────────────────────────────────────────────────
st.markdown('<div class="pf-section"><div class="pf-section-ttl">Channel Mix by Bookings Received</div></div>',
            unsafe_allow_html=True)

_src = load_source_stats()
if not _src.empty:
    _src_f = _src[(_src["business_date"] >= s_ts) & (_src["business_date"] <= e_ts)]
    _ch = (
        _src_f.groupby("channel")
              .agg(Bookings=("room_nights", "sum"), Revenue=("room_revenue", "sum"))
              .reset_index()
              .rename(columns={"channel": "Channel"})
    )
    _total_bk = float(_ch["Bookings"].sum()) or 1.0
    _ch["Share %"] = (_ch["Bookings"] / _total_bk * 100).round(1)
    _ch["ADR ($)"] = (_ch["Revenue"] / _ch["Bookings"].replace(0, float("nan"))).round(0)
    _ch = _ch.sort_values("Bookings", ascending=False)[
        ["Channel", "Bookings", "Share %", "Revenue", "ADR ($)"]
    ].reset_index(drop=True)

    _ch_col, _ = st.columns([1, 1])
    with _ch_col:
        st.dataframe(
            _ch,
            column_config={
                "Channel":  st.column_config.TextColumn("Channel"),
                "Bookings": st.column_config.NumberColumn("Bookings", format="%d"),
                "Share %":  st.column_config.NumberColumn("Share %",  format="%.1f%%"),
                "Revenue":  st.column_config.NumberColumn("Revenue",  format="$%,.0f"),
                "ADR ($)":  st.column_config.NumberColumn("ADR ($)",  format="$%.0f"),
            },
            use_container_width=True,
            hide_index=True,
        )

# ── Guest satisfaction ────────────────────────────────────────────────────────
med = df[df["medallia_overall_satisfaction"].notna()]
if not med.empty:
    scores = {
        "Overall":     med["medallia_overall_satisfaction"].mean(),
        "Cleanliness": med["medallia_hotel_cleanliness"].mean()          if "medallia_hotel_cleanliness"      in med else None,
        "Value":       med["medallia_value_for_price"].mean()            if "medallia_value_for_price"        in med else None,
        "Recommend":   med["medallia_likelihood_to_recommend"].mean()    if "medallia_likelihood_to_recommend" in med else None,
        "Return":      med["medallia_likelihood_to_return"].mean()       if "medallia_likelihood_to_return"   in med else None,
    }
    scores = {k: v for k, v in scores.items() if v is not None}

    st.markdown('<div class="pf-section"><div class="pf-section-ttl">Guest Satisfaction — Medallia</div></div>',
                unsafe_allow_html=True)

    # Score cards
    cards_html = '<div class="pf-medal-grid">'
    for label, score in scores.items():
        pct = min(score / 10 * 100, 100)
        cards_html += f"""
        <div class="pf-medal">
          <div class="pf-medal-score">{score:.1f}</div>
          <div class="pf-medal-lbl">{label}</div>
          <div class="pf-medal-bar"><div class="pf-medal-fill" style="width:{pct:.0f}%"></div></div>
        </div>"""
    cards_html += '</div>'
    st.markdown(cards_html, unsafe_allow_html=True)

    # ── Satisfaction trend chart ───────────────────────────────────────────────
    st.markdown('<div class="pf-section"><div class="pf-section-ttl">Satisfaction Trend — Monthly Average</div></div>',
                unsafe_allow_html=True)

    _MED_METRICS = {
        "Overall":     ("medallia_overall_satisfaction",      ACCENT),
        "Cleanliness": ("medallia_hotel_cleanliness",         GREEN),
        "Recommend":   ("medallia_likelihood_to_recommend",   SLATE),
        "Value":       ("medallia_value_for_price",           "#9b6fd4"),
        "Return":      ("medallia_likelihood_to_return",      "#d4903a"),
    }
    _MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    # Controls row: metrics · years · chart type
    _sc1, _sc2, _sc3 = st.columns([3, 2, 1.5])
    with _sc1:
        sat_metrics = st.multiselect(
            "Metrics", list(_MED_METRICS.keys()),
            default=["Overall"], key="pf_sat_metrics", label_visibility="collapsed",
        ) or ["Overall"]
    with _sc2:
        sat_years = st.multiselect(
            "Years", [2024, 2025], default=[2024, 2025],
            key="pf_sat_years", label_visibility="collapsed",
        ) or [2024, 2025]
    with _sc3:
        sat_t = st.segmented_control("", ["📈", "📊"], default="📈",
                                     key="pf_sat_t", label_visibility="collapsed")

    # Pull monthly averages straight from master — independent of page horizon
    _sat_master = master[master["medallia_overall_satisfaction"].notna()].copy()
    _sat_master["yr"] = _sat_master["business_date"].dt.year
    _sat_master["mo"] = _sat_master["business_date"].dt.month

    # One color per year, dimmed for 2024
    _YEAR_STYLES = {
        2024: ("rgba(232,133,74,0.5)",  1.5, "dot"),
        2025: (ACCENT,                  2.0, "solid"),
    }

    fig_sat = go.Figure()
    fig_sat.add_hline(y=8.0, line_dash="dot", line_color="rgba(255,255,255,0.12)",
                      annotation_text="8.0", annotation_position="right",
                      annotation_font=dict(size=9, color="rgba(255,255,255,0.3)"))

    for label in sat_metrics:
        col, base_color = _MED_METRICS[label]
        if col not in _sat_master.columns:
            continue
        for yr in sorted(sat_years):
            yr_data = _sat_master[_sat_master["yr"] == yr]
            monthly  = yr_data.groupby("mo")[col].mean()
            y_vals   = [float(monthly.loc[m]) if m in monthly.index else None for m in range(1, 13)]
            yr_color, lw, dash = _YEAR_STYLES.get(yr, (base_color, 1.5, "solid"))
            # If multiple metrics, use metric color; if single, use year style color
            trace_color = base_color if len(sat_metrics) > 1 else yr_color
            opacity     = 1.0 if yr == max(sat_years) else 0.55

            if sat_t == "📊":
                fig_sat.add_trace(go.Bar(
                    x=_MONTH_LABELS, y=y_vals,
                    name=f"{label} {yr}", marker_color=trace_color, opacity=opacity,
                    hovertemplate=f"{label}: %{{y:.2f}}<extra>{yr}</extra>",
                ))
            else:
                fig_sat.add_trace(go.Scatter(
                    x=_MONTH_LABELS, y=y_vals,
                    name=f"{label} {yr}",
                    line=dict(color=trace_color, width=lw,
                              dash="dot" if yr != max(sat_years) else "solid"),
                    mode="lines+markers", marker=dict(size=5 if yr == max(sat_years) else 4),
                    opacity=opacity,
                    hovertemplate=f"{label}: %{{y:.2f}}<extra>{yr}</extra>",
                    connectgaps=True,
                ))

    sat_lay = base_layout(h=260)
    if sat_t == "📊":
        sat_lay["barmode"] = "group"
    sat_lay["yaxis"]["range"]     = [0, 10.5]
    sat_lay["yaxis"]["tickvals"]  = [0, 2, 4, 6, 8, 10]
    sat_lay["xaxis"]["tickangle"] = 0
    sat_lay["title"] = dict(
        text="Score / 10 — monthly average · solid = most recent year selected",
        font=dict(size=10, color="rgba(245,245,240,0.4)"),
        x=0, xanchor="left", pad=dict(l=4),
    )
    fig_sat.update_layout(**sat_lay)
    st.plotly_chart(fig_sat, use_container_width=True, config={"displayModeBar": False})

st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
