import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import date
from pathlib import Path
import sys
import html as _he
import streamlit.components.v1 as components

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
.dm-home-btn { margin-left:auto; font-size:11px; color:rgba(245,245,240,.45); text-decoration:none; padding:5px 11px; border:1px solid rgba(255,255,255,.1); border-radius:4px; transition:color .15s,border-color .15s; }
.dm-home-btn:hover { color:rgba(245,245,240,.9); border-color:rgba(255,255,255,.25); }

.dm-kpi-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; padding:18px 20px 6px; }
.dm-kpi { background:var(--dark); border:1px solid var(--border); border-radius:6px; padding:14px 16px; }
.dm-kpi-lbl { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:6px; }
.dm-kpi-val { font-size:28px; font-weight:600; color:var(--white); line-height:1; }
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

@st.cache_data
def load_room_types():
    return pd.read_csv(BASE / "data" / "processed" / "daily_stats_room_type.csv",
                       parse_dates=["business_date"])

@st.cache_data
def load_master():
    return pd.read_csv(BASE / "data" / "final" / "hotel_daily_master.csv",
                       parse_dates=["business_date"])

res     = load_res()
ready   = load_ready()
src_df  = load_source()
master  = load_master()
rt_df   = load_room_types()

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

# ── Room Type Performance Table ───────────────────────────────────────────────
st.markdown('<div class="dm-section"><div class="dm-section-ttl">Room Type Performance</div></div>',
            unsafe_allow_html=True)

ROOM_TYPE_NAMES = {
    "LS":   "Loft Suite",
    "SS":   "Studio Suite",
    "SSRS": "Accessible Studio Suite",
    "PLS":  "City View Loft Suite",
    "PSS":  "City Viiew Studio Suite",
    "CVQ":  "City View Queen",
    "Q":    "Standard Queen",
    "QT":   "Queen Terrace",
    "QTRS": "Accessible Queen Terrace",
    "SKT":  "King Terrace",
    "PKT":  "City View King Terrace",
    "PQT":  "City View Queen Terrace",
}

rt_f = rt_df[(rt_df["business_date"] >= s_ts) & (rt_df["business_date"] <= e_ts)].copy()
rt_f["available"] = rt_f["total_physical_rooms"] - rt_f["ooo_rooms"]
rt_f["is_full"]   = (rt_f["available"] > 0) & (rt_f["room_nights"] >= rt_f["available"])
rt_f["occupancy"] = rt_f.apply(
    lambda r: r["room_nights"] / r["available"] if r["available"] > 0 else None, axis=1
)

# include all rows (not just available>0) for OOO calc, then filter for main stats
rt_f["lost_revenue_est"] = rt_f["ooo_rooms"] * rt_f["adr"].fillna(0)

ooo_stats = (rt_f.groupby("room_type")
             .agg(
                 ooo_room_nights = ("ooo_rooms",         "sum"),
                 days_any_ooo    = ("ooo_rooms",         lambda x: (x > 0).sum()),
                 lost_revenue    = ("lost_revenue_est",  "sum"),
                 _n_rooms        = ("total_physical_rooms", "first"),
                 _total_days     = ("business_date",     "count"),
             )
             .reset_index())
ooo_stats["total_room_nights_possible"] = ooo_stats["_n_rooms"] * ooo_stats["_total_days"]
ooo_stats["ooo_pct"] = ooo_stats["ooo_room_nights"] / ooo_stats["total_room_nights_possible"] * 100

rt_stats = (rt_f[rt_f["available"] > 0]
            .groupby("room_type")
            .agg(
                n_rooms       = ("total_physical_rooms", "first"),
                room_nights   = ("room_nights",          "sum"),
                room_revenue  = ("room_revenue",         "sum"),
                total_revenue = ("total_revenue",        "sum"),
                avg_adr       = ("adr",                  "mean"),
                avg_occ       = ("occupancy",            "mean"),
                sellout_days  = ("is_full",              "sum"),
                total_days    = ("is_full",              "count"),
            )
            .reset_index())

rt_stats = rt_stats.merge(
    ooo_stats[["room_type", "ooo_room_nights", "days_any_ooo", "ooo_pct", "lost_revenue"]],
    on="room_type", how="left"
).fillna(0)

rt_stats["sellout_pct"] = rt_stats["sellout_days"] / rt_stats["total_days"] * 100
n_days_period           = (e_ts - s_ts).days + 1
rt_stats["revpar"]      = rt_stats["room_revenue"] / (rt_stats["n_rooms"] * n_days_period)
rt_stats["label"]       = rt_stats["room_type"].map(ROOM_TYPE_NAMES).fillna(rt_stats["room_type"])


display_df = rt_stats[[
    "room_type", "label", "n_rooms", "room_nights",
    "avg_occ", "avg_adr", "revpar", "total_revenue",
    "sellout_days", "sellout_pct",
    "ooo_room_nights", "ooo_pct", "lost_revenue",
]].copy()
display_df["avg_occ"] = display_df["avg_occ"] * 100

_RT_COLS = [
    ("room_type",       "Code",           "text",
     "Short PMS code that identifies the room type in the property management system."),
    ("label",           "Room Type",      "text",
     "Full name of the room category."),
    ("n_rooms",         "Rooms",          "int",
     "Total number of physical rooms in this category."),
    ("room_nights",     "Nights Sold",    "int",
     "Total room-nights occupied during the selected period. OOO nights are excluded."),
    ("avg_occ",         "Occupancy %",    "pct1",
     "Average nightly occupancy — rooms sold ÷ rooms available. OOO nights are excluded from the available base."),
    ("avg_adr",         "ADR",            "usd0",
     "Average Daily Rate — total room revenue ÷ rooms sold. Measures the average price paid per occupied room."),
    ("revpar",          "RevPAR",         "usd0",
     "Revenue Per Available Room — room revenue ÷ (rooms × days in period). Combines occupancy and rate into one efficiency metric."),
    ("total_revenue",   "Revenue",        "usdc",
     "Total revenue generated by this room type, including room revenue and any ancillary charges."),
    ("sellout_days",    "Sellout Nights", "int",
     "Number of nights where every available room in this category was occupied."),
    ("sellout_pct",     "Sellout %",      "pct0",
     "Share of nights in the period when this room type fully sold out (Sellout Nights ÷ total nights)."),
    ("ooo_room_nights", "OOO Nights",     "int",
     "Total room-nights Out of Order — unavailable due to maintenance, renovation, or other operational holds."),
    ("ooo_pct",         "OOO %",          "pct1",
     "OOO room-nights as a % of total possible room-nights in the period (OOO Nights ÷ Rooms × Days)."),
    ("lost_revenue",    "Lost Revenue",   "usdc",
     "Estimated revenue lost to Out-of-Order rooms — OOO rooms × ADR per night. Represents the opportunity cost of rooms unavailable for sale."),
]

_RT_CSS = """
* { font-family: Inter, system-ui, sans-serif; box-sizing: border-box; margin: 0; padding: 0; -webkit-font-smoothing: antialiased; }
body { background: transparent; overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
thead tr { border-bottom: 1px solid rgba(255,255,255,0.14); }
th {
  color: rgba(245,245,240,0.38); font-size: 9px; font-weight: 600;
  letter-spacing: .10em; text-transform: uppercase;
  padding: 8px 6px; text-align: right;
  position: relative; white-space: nowrap; vertical-align: bottom;
  cursor: pointer; user-select: none;
}
th:first-child, th:nth-child(2) { text-align: left; }
.th-inner { display: inline-flex; align-items: center; gap: 3px; border-bottom: 1px dashed rgba(245,245,240,0.28); padding-bottom: 1px; }
.sort-icon { font-size: 7px; color: #e8854a; min-width: 7px; }
.tip {
  display: none; position: absolute;
  top: calc(100% + 4px); left: 50%; transform: translateX(-50%);
  background: #2b2b2b; border: 1px solid rgba(255,255,255,0.14); border-radius: 5px;
  padding: 6px 9px; width: 200px; font-size: 9px; color: rgba(245,245,240,0.85);
  font-weight: 400; letter-spacing: normal; text-transform: none;
  text-align: left; line-height: 1.4; z-index: 100; pointer-events: none; white-space: normal;
}
.tip::after {
  content: ''; position: absolute; bottom: 100%; left: 50%; transform: translateX(-50%);
  border: 5px solid transparent; border-bottom-color: #2b2b2b;
}
th:hover .tip { display: block; }
th.sorted { color: rgba(245,245,240,0.75); }
td {
  padding: 6px 6px; border-bottom: 1px solid rgba(255,255,255,0.08);
  color: #f5f5f0; text-align: right; font-size: 12px; white-space: nowrap;
}
td:first-child { color: rgba(245,245,240,0.6); text-align: left; }
td:nth-child(2) { text-align: left; max-width: 150px; overflow: hidden; text-overflow: ellipsis; }
tbody tr:hover td { background: rgba(255,255,255,0.03); }
tbody tr:last-child td { border-bottom: none; }
tfoot tr td {
  border-top: 2px solid rgba(255,255,255,0.18);
  font-weight: 600; color: rgba(245,245,240,0.9);
  background: rgba(255,255,255,0.02); border-bottom: none;
}
tfoot tr td:first-child { color: #e8854a; }
"""

_RT_JS = """
function sort(idx, th) {
  const tbody = document.querySelector('tbody');
  const rows  = Array.from(tbody.querySelectorAll('tr'));
  const dir   = th.dataset.dir === 'asc' ? 'desc' : 'asc';
  document.querySelectorAll('th').forEach(h => {
    h.dataset.dir = 'none';
    h.classList.remove('sorted');
    h.querySelector('.sort-icon').textContent = '';
  });
  th.dataset.dir = dir;
  th.classList.add('sorted');
  th.querySelector('.sort-icon').textContent = dir === 'asc' ? ' ▲' : ' ▼';
  rows.sort((a, b) => {
    const av = a.cells[idx].dataset.raw;
    const bv = b.cells[idx].dataset.raw;
    const an = parseFloat(av), bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return dir === 'asc' ? an - bn : bn - an;
    return dir === 'asc' ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
  });
  rows.forEach(r => tbody.appendChild(r));
}
"""

def _fmt(val, kind):
    try:
        v = float(val)
    except (TypeError, ValueError):
        return str(val) if val else "—"
    if kind == "text":  return str(val)
    if kind == "int":   return f"{int(v):,}"
    if kind == "pct1":  return f"{v:.1f}%"
    if kind == "pct0":  return f"{v:.0f}%"
    if kind == "usd0":  return f"${v:,.0f}"
    if kind == "usdc":  return f"${v:,.0f}"
    return str(val)

def _build_rt_component(df, cols, totals=None):
    ths = ""
    for i, (_, label, _, tip) in enumerate(cols):
        safe_tip = _he.escape(tip)
        ths += (
            f'<th onclick="sort({i}, this)" data-dir="none">'
            f'<div class="th-inner"><span>{label}</span>'
            f'<span class="sort-icon"></span></div>'
            f'<div class="tip">{safe_tip}</div>'
            f'</th>'
        )
    trs = ""
    for _, row in df.iterrows():
        tds = ""
        for col, _, kind, _ in cols:
            val = row[col]
            raw = _he.escape(str(val)) if kind == "text" else (
                float(val) if str(val) not in ("", "nan") else 0
            )
            tds += f'<td data-raw="{raw}">{_he.escape(_fmt(val, kind))}</td>'
        trs += f"<tr>{tds}</tr>"
    tfoot = ""
    if totals is not None:
        tds = ""
        for col, _, kind, _ in cols:
            val = totals.get(col, "")
            raw = _he.escape(str(val)) if kind == "text" else (
                float(val) if str(val) not in ("", "nan") else 0
            )
            tds += f'<td data-raw="{raw}">{_he.escape(_fmt(val, kind))}</td>'
        tfoot = f"<tfoot><tr>{tds}</tr></tfoot>"
    height = len(df) * 36 + (120 if totals else 70)
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'>"
        f"<style>{_RT_CSS}</style></head><body>"
        f"<table><thead><tr>{ths}</tr></thead><tbody>{trs}</tbody>{tfoot}</table>"
        f"<script>{_RT_JS}</script></body></html>"
    ), height

_tot_rn = float(rt_stats["room_nights"].sum())
_tot_rv = float(rt_stats["room_revenue"].sum())
_tot_nr = float(rt_stats["n_rooms"].sum())
_tot_row = {
    "room_type":       "TOTAL",
    "label":           "All Types",
    "n_rooms":         int(_tot_nr),
    "room_nights":     int(_tot_rn),
    "avg_occ":         float(display_df["avg_occ"].mean()),
    "avg_adr":         _tot_rv / _tot_rn if _tot_rn else 0.0,
    "revpar":          _tot_rv / (_tot_nr * n_days_period) if (_tot_nr * n_days_period) else 0.0,
    "total_revenue":   float(rt_stats["total_revenue"].sum()),
    "sellout_days":    int(rt_stats["sellout_days"].sum()),
    "sellout_pct":     float(display_df["sellout_pct"].mean()),
    "ooo_room_nights": int(rt_stats["ooo_room_nights"].sum()),
    "ooo_pct":         float(rt_stats["ooo_room_nights"].sum() / (_tot_nr * n_days_period) * 100) if (_tot_nr * n_days_period) else 0.0,
    "lost_revenue":    float(rt_stats["lost_revenue"].sum()),
}

_rt_html, _rt_height = _build_rt_component(display_df, _RT_COLS, totals=_tot_row)
components.html(_rt_html, height=_rt_height, scrolling=True)

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

# ── Sellout Nights ───────────────────────────────────────────────────────────
st.markdown('<div class="dm-section"><div class="dm-section-ttl">Sellout Nights — 100% Capacity Days</div></div>',
            unsafe_allow_html=True)

# Use full master data (all years) for the historical sellout analysis
master["occ_pct"]  = master["occupancy_rate"] * 100
master["is_full"]  = master["occupancy_rate"] >= 0.99
master["year"]     = master["business_date"].dt.year
master["month"]    = master["business_date"].dt.month

_cs1, _cs2 = st.columns(2)

DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

with _cs1:
    # % of days per DOW that hit 100%, split by year
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
                            font=dict(size=11, color="rgba(245,245,240,0.7)"),
                            x=0, xanchor="left", pad=dict(l=4))
    dow_lay["barmode"]  = "group"
    dow_lay["bargap"]   = 0.2
    dow_lay["xaxis"]["tickangle"] = 0
    fig_dow.update_layout(**dow_lay)
    st.plotly_chart(fig_dow, use_container_width=True, config={"displayModeBar": False})

with _cs2:
    # Full-cap days per month (stacked by year)
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
                            font=dict(size=11, color="rgba(245,245,240,0.7)"),
                            x=0, xanchor="left", pad=dict(l=4))
    mon_lay["barmode"] = "group"
    mon_lay["bargap"]  = 0.15
    mon_lay["xaxis"]["tickangle"] = -30
    fig_mon.update_layout(**mon_lay)
    st.plotly_chart(fig_mon, use_container_width=True, config={"displayModeBar": False})

# Sellout insight cards
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
    # Avg occupancy: event day vs non-event day for each event type
    rows = []
    for col, label in EVENT_LABELS.items():
        if col not in master.columns:
            continue
        event_days    = master[master[col].astype(bool)]
        non_event     = master[~master[col].astype(bool)]
        n_event       = len(event_days)
        if n_event == 0:
            continue
        avg_ev  = event_days["occ_pct"].mean()
        avg_no  = non_event["occ_pct"].mean()
        lift    = avg_ev - avg_no
        rows.append({"label": label, "event_occ": avg_ev,
                     "base_occ": avg_no, "lift": lift, "n": n_event})

    df_ev = pd.DataFrame(rows).sort_values("lift")

    # Grouped bar: event vs no-event avg occupancy
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
                           font=dict(size=11, color="rgba(245,245,240,0.7)"),
                           x=0, xanchor="left", pad=dict(l=4))
    ev_lay["barmode"]     = "overlay"
    ev_lay["bargap"]      = 0.3
    ev_lay["margin"]["l"] = 160
    ev_lay["xaxis"].update({"ticksuffix": "%", "range": [60, 100]})
    fig_ev.update_layout(**ev_lay)
    st.plotly_chart(fig_ev, use_container_width=True, config={"displayModeBar": False})

with _ce2:
    # Insight cards per event type showing lift
    cards = ""
    for _, row in df_ev.sort_values("lift", ascending=False).iterrows():
        lift_v  = row["lift"]
        color   = "#3ecf8e" if lift_v > 0 else "#eb2323"
        sym     = "▲" if lift_v > 0 else "▼"
        note    = ("Positive demand signal" if lift_v > 1
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
