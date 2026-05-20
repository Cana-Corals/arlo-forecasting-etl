import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import date, timedelta
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
HORIZONS = ["1M", "Q1", "Q2", "Q3", "Q4", "Full Year", "Custom"]
if "pf_hz" not in st.session_state:
    st.session_state["pf_hz"] = "Full Year"

hz = st.segmented_control("Horizon", HORIZONS,
                           default=st.session_state["pf_hz"],
                           key="pf_hz_ctrl", label_visibility="collapsed")
if hz:
    st.session_state["pf_hz"] = hz
horizon = st.session_state["pf_hz"]

# Year selector alongside horizon
_yc1, _yc2, _ = st.columns([2, 2, 5])
with _yc1:
    sel_year = st.selectbox("Year", [2025, 2024], key="pf_year",
                            label_visibility="collapsed")

def preset_dates(h, yr):
    if h == "1M":       return date(yr, 12, 1),  date(yr, 12, 31)
    if h == "Q1":       return date(yr, 1, 1),   date(yr, 3, 31)
    if h == "Q2":       return date(yr, 4, 1),   date(yr, 6, 30)
    if h == "Q3":       return date(yr, 7, 1),   date(yr, 9, 30)
    if h == "Q4":       return date(yr, 10, 1),  date(yr, 12, 31)
    if h == "Full Year":return date(yr, 1, 1),   date(yr, 12, 31)
    return None, None

if horizon == "Custom":
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
else:
    start, end = preset_dates(horizon, sel_year)

s_ts = pd.Timestamp(start)
e_ts = pd.Timestamp(end)

# Prior year (comparison)
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

rc,rd = delta(rev_c, rev_p)
oc,od = delta(occ_c, occ_p)
ac,ad = delta(adr_c, adr_p)
rpc,rpd = delta(rp_c, rp_p)
tc,td = delta(trp_c, trp_p)

st.markdown(f"""
<div class="pf-kpi-grid">
  <div class="pf-kpi">
    <div class="pf-kpi-lbl">Total Revenue</div>
    <div class="pf-kpi-val">${rev_c/1e3:,.0f}k</div>
    <div class="pf-kpi-row"><span class="{rc}">{rd}</span><span class="pf-kpi-py">vs <span>${rev_p/1e3:,.0f}k</span></span></div>
  </div>
  <div class="pf-kpi">
    <div class="pf-kpi-lbl">Occupancy</div>
    <div class="pf-kpi-val">{occ_c:.1f}%</div>
    <div class="pf-kpi-row"><span class="{oc}">{od}</span><span class="pf-kpi-py">vs <span>{occ_p:.1f}%</span></span></div>
  </div>
  <div class="pf-kpi">
    <div class="pf-kpi-lbl">ADR</div>
    <div class="pf-kpi-val">${adr_c:,.0f}</div>
    <div class="pf-kpi-row"><span class="{ac}">{ad}</span><span class="pf-kpi-py">vs <span>${adr_p:,.0f}</span></span></div>
  </div>
  <div class="pf-kpi">
    <div class="pf-kpi-lbl">RevPAR</div>
    <div class="pf-kpi-val">${rp_c:,.0f}</div>
    <div class="pf-kpi-row"><span class="{rpc}">{rpd}</span><span class="pf-kpi-py">vs <span>${rp_p:,.0f}</span></span></div>
  </div>
  <div class="pf-kpi">
    <div class="pf-kpi-lbl">TRevPAR</div>
    <div class="pf-kpi-val">${trp_c:,.0f}</div>
    <div class="pf-kpi-row"><span class="{tc}">{td}</span><span class="pf-kpi-py">vs <span>${trp_p:,.0f}</span></span></div>
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
    r_cur = agg(df,    "total_revenue", "sum")
    r_py  = agg(df_py, "total_revenue", "sum")
    xl = xlabels(r_cur["p"])
    py_v = (list(r_py["total_revenue"].values) + [0]*len(xl))[:len(xl)]

    fig = go.Figure()
    fig.add_trace(go.Bar(x=xl, y=r_cur["total_revenue"], name=str(sel_year),
                         marker_color=ACCENT,
                         hovertemplate="$%{y:,.0f}<extra>" + str(sel_year) + "</extra>"))
    fig.add_trace(go.Bar(x=xl, y=py_v, name=str(py_yr),
                         marker_color=ACCENT_F, marker_line=dict(color=ACCENT, width=1),
                         hovertemplate="$%{y:,.0f}<extra>" + str(py_yr) + "</extra>"))
    lay = base_layout(h=260, yprefix="$")
    lay["title"] = dict(text="Revenue", font=dict(size=11, color="rgba(245,245,240,0.7)"), x=0, xanchor="left", pad=dict(l=4))
    fig.update_layout(**lay)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

with _col2:
    o_cur = agg(df,    "occupancy_rate", "mean")
    o_py  = agg(df_py, "occupancy_rate", "mean")
    py_ov = (list((o_py["occupancy_rate"]*100).values) + [0]*len(xl))[:len(xl)]

    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=xl, y=o_cur["occupancy_rate"]*100, name=str(sel_year),
                          marker_color=GREEN,
                          hovertemplate="%{y:.1f}%<extra>" + str(sel_year) + "</extra>"))
    fig2.add_trace(go.Bar(x=xl, y=py_ov, name=str(py_yr),
                          marker_color=GREEN_F, marker_line=dict(color=GREEN, width=1),
                          hovertemplate="%{y:.1f}%<extra>" + str(py_yr) + "</extra>"))
    lay2 = base_layout(h=260, ysuffix="%")
    lay2["title"] = dict(text="Occupancy", font=dict(size=11, color="rgba(245,245,240,0.7)"), x=0, xanchor="left", pad=dict(l=4))
    lay2["yaxis"]["range"] = [0, 105]
    fig2.update_layout(**lay2)
    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

# ── ADR & RevPAR ──────────────────────────────────────────────────────────────
_col3, _col4 = st.columns(2)

with _col3:
    a_cur = agg(df,    "adr", "mean")
    a_py  = agg(df_py, "adr", "mean")
    py_av = (list(a_py["adr"].values) + [0]*len(xl))[:len(xl)]

    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(x=xl, y=a_cur["adr"], name=str(sel_year),
                              line=dict(color=ACCENT, width=2),
                              hovertemplate="$%{y:,.0f}<extra>" + str(sel_year) + "</extra>"))
    fig3.add_trace(go.Scatter(x=xl, y=py_av, name=str(py_yr),
                              line=dict(color=ACCENT_F, width=1.5, dash="dot"),
                              hovertemplate="$%{y:,.0f}<extra>" + str(py_yr) + "</extra>"))
    lay3 = base_layout(h=220, yprefix="$")
    lay3["title"] = dict(text="ADR", font=dict(size=11, color="rgba(245,245,240,0.7)"), x=0, xanchor="left", pad=dict(l=4))
    fig3.update_layout(**lay3)
    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})

with _col4:
    rp_cur = agg(df,    "revpar", "mean")
    rp_py  = agg(df_py, "revpar", "mean")
    py_rpv = (list(rp_py["revpar"].values) + [0]*len(xl))[:len(xl)]

    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=xl, y=rp_cur["revpar"], name=str(sel_year),
                              line=dict(color=SLATE, width=2),
                              hovertemplate="$%{y:,.0f}<extra>" + str(sel_year) + "</extra>"))
    fig4.add_trace(go.Scatter(x=xl, y=py_rpv, name=str(py_yr),
                              line=dict(color=SLATE_F, width=1.5, dash="dot"),
                              hovertemplate="$%{y:,.0f}<extra>" + str(py_yr) + "</extra>"))
    lay4 = base_layout(h=220, yprefix="$")
    lay4["title"] = dict(text="RevPAR", font=dict(size=11, color="rgba(245,245,240,0.7)"), x=0, xanchor="left", pad=dict(l=4))
    fig4.update_layout(**lay4)
    st.plotly_chart(fig4, use_container_width=True, config={"displayModeBar": False})

# ── Day of week occupancy ─────────────────────────────────────────────────────
st.markdown('<div class="pf-section"><div class="pf-section-ttl">Occupancy by Day of Week</div></div>',
            unsafe_allow_html=True)

dow_cur = df.groupby("day_of_week")["occupancy_rate"].mean() * 100
dow_py  = df_py.groupby("day_of_week")["occupancy_rate"].mean() * 100
DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
days = list(range(7))

fig_dow = go.Figure()
fig_dow.add_trace(go.Bar(
    x=[DOW_LABELS[i] for i in days],
    y=[float(dow_cur.get(i, 0)) for i in days],
    name=str(sel_year), marker_color=GREEN,
    hovertemplate="%{x}: %{y:.1f}%<extra>" + str(sel_year) + "</extra>",
))
fig_dow.add_trace(go.Bar(
    x=[DOW_LABELS[i] for i in days],
    y=[float(dow_py.get(i, 0)) for i in days],
    name=str(py_yr), marker_color=GREEN_F,
    marker_line=dict(color=GREEN, width=1),
    hovertemplate="%{x}: %{y:.1f}%<extra>" + str(py_yr) + "</extra>",
))
dow_lay = base_layout(h=220, ysuffix="%")
dow_lay["bargroupgap"] = 0.1
dow_lay["xaxis"]["tickangle"] = 0
fig_dow.update_layout(**dow_lay)
st.plotly_chart(fig_dow, use_container_width=True, config={"displayModeBar": False})

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

# ── Guest satisfaction ────────────────────────────────────────────────────────
med = df[df["medallia_overall_satisfaction"].notna()]
if not med.empty:
    scores = {
        "Overall": med["medallia_overall_satisfaction"].mean(),
        "Cleanliness": med["medallia_hotel_cleanliness"].mean()       if "medallia_hotel_cleanliness" in med else None,
        "Value": med["medallia_value_for_price"].mean()               if "medallia_value_for_price"  in med else None,
        "Recommend": med["medallia_likelihood_to_recommend"].mean()   if "medallia_likelihood_to_recommend" in med else None,
        "Return": med["medallia_likelihood_to_return"].mean()         if "medallia_likelihood_to_return" in med else None,
    }
    scores = {k: v for k, v in scores.items() if v is not None}

    st.markdown('<div class="pf-section"><div class="pf-section-ttl">Guest Satisfaction — Medallia</div></div>',
                unsafe_allow_html=True)

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

st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
