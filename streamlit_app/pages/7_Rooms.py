import streamlit as st
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
import sys
import html as _he
import streamlit.components.v1 as components

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.sidebar import render_sidebar
from components.forecast_engine import generate_future_predictions

BASE  = Path(__file__).resolve().parents[2]
TODAY = date.today()

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown(
    '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@2.47.0/tabler-icons.min.css">',
    unsafe_allow_html=True,
)
st.markdown("""
<style>
:root {
  --dark:#111111; --dark2:#1a1a1a;
  --border:rgba(255,255,255,0.08);
  --white:#f5f5f0; --muted:rgba(245,245,240,0.38); --muted2:rgba(245,245,240,0.6);
  --rm-accent:#38bdf8; --green:#3ecf8e; --red:#e05252; --orange:#e8854a;
}
*, *::before, *::after { font-family:'Inter',system-ui,sans-serif !important; -webkit-font-smoothing:antialiased; box-sizing:border-box; }
#MainMenu, footer, header { visibility:hidden; }
[data-testid="collapsedControl"] { display:none; }
.stApp { background:var(--dark2) !important; }
.block-container { padding:0 !important; max-width:1300px !important; margin:0 auto !important; }
[data-testid="stAppViewContainer"] { background:var(--dark2) !important; }

.rm-topbar { background:var(--dark); border-bottom:1px solid var(--border); height:52px; display:flex; align-items:center; padding:0 20px; gap:10px; }
.rm-title  { font-size:14px; font-weight:600; color:var(--white); }
.rm-sub    { font-size:11px; color:var(--muted); }
.rm-rule   { height:2px; background:linear-gradient(90deg,var(--rm-accent) 0%,transparent 60%); }
.rm-home-btn { margin-left:auto; font-size:11px; color:rgba(245,245,240,.45); text-decoration:none; padding:5px 11px; border:1px solid rgba(255,255,255,.1); border-radius:4px; transition:color .15s,border-color .15s; }
.rm-home-btn:hover { color:rgba(245,245,240,.9); border-color:rgba(255,255,255,.25); }

.rm-kpi-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; padding:18px 20px 6px; }
.rm-kpi {
  background:var(--dark); border:1px solid var(--border);
  border-top:2px solid var(--rm-accent);
  border-radius:6px; padding:14px 16px;
}
.rm-kpi-lbl   { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:6px; }
.rm-kpi-val   { font-size:24px; font-weight:600; color:var(--white); line-height:1; margin-bottom:4px; }
.rm-kpi-delta { font-size:10px; color:var(--muted2); margin-top:5px; }
.rm-kpi-desc  { font-size:9px; color:var(--muted); margin-top:3px; line-height:1.4; }

.rm-section { padding:16px 20px 0; }
.rm-section-ttl { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }
.rm-live-bar { display:flex; align-items:center; gap:10px; padding:0 20px 10px; }
.rm-live-dot { width:7px; height:7px; border-radius:50%; background:#3ecf8e; box-shadow:0 0 6px #3ecf8e; animation:pulse 1.8s ease-in-out infinite; flex-shrink:0; }
.rm-live-lbl { font-size:9px; font-weight:700; letter-spacing:.14em; text-transform:uppercase; color:#3ecf8e; }
.rm-live-dates { font-size:10px; color:rgba(245,245,240,0.5); }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }
</style>
""", unsafe_allow_html=True)

# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data
def load_rt():
    return pd.read_csv(BASE / "data" / "processed" / "daily_stats_room_type.csv",
                       parse_dates=["business_date"])

@st.cache_data(show_spinner="Generating 2026 forecast…")
def load_forecast():
    return generate_future_predictions(2026)

@st.cache_data
def load_actuals():
    return pd.read_csv(BASE / "outputs" / "model_predictions_with_events.csv",
                       parse_dates=["business_date"])

render_sidebar(active="rooms")
rt_df    = load_rt()
forecast = load_forecast()
actuals  = load_actuals()

# ── Topbar ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="rm-topbar">
  <span class="rm-title">Rooms</span>
  <span class="rm-sub">Room type KPIs, performance &amp; revenue forecast</span>
  <a href="/" target="_self" class="rm-home-btn">&#8962; Home</a>
</div>
<div class="rm-rule"></div>
""", unsafe_allow_html=True)

# ── Controls ──────────────────────────────────────────────────────────────────
HORIZONS = ["Q1", "Q2", "Q3", "Q4", "All Years", "Custom"]
if "rm_hz" not in st.session_state:
    st.session_state["rm_hz"] = "All Years"

hz = st.segmented_control("Horizon", HORIZONS,
                           default=st.session_state["rm_hz"],
                           key="rm_hz_ctrl", label_visibility="collapsed")
if hz:
    st.session_state["rm_hz"] = hz
horizon = st.session_state["rm_hz"]

_all_years = (horizon == "All Years")

_yc1, _ = st.columns([2, 7])
with _yc1:
    sel_year = st.selectbox("Year", [2025, 2024], key="rm_year",
                            label_visibility="collapsed",
                            disabled=_all_years)

_yr_int = 2025 if _all_years else int(sel_year)

def preset_dates(h, yr):
    if h == "Q1":        return date(yr, 1, 1),  date(yr, 3, 31)
    if h == "Q2":        return date(yr, 4, 1),  date(yr, 6, 30)
    if h == "Q3":        return date(yr, 7, 1),  date(yr, 9, 30)
    if h == "Q4":        return date(yr, 10, 1), date(yr, 12, 31)
    if h == "Full Year": return date(yr, 1, 1),  date(yr, 12, 31)
    return None, None

if _all_years:
    start = rt_df["business_date"].min().date()
    end   = rt_df["business_date"].max().date()
elif horizon == "Custom":
    _c1, _c2, _ = st.columns([2, 2, 4])
    with _c1:
        start = st.date_input("From", value=date(_yr_int, 1, 1),
                              min_value=date(2024, 1, 1), max_value=date(2025, 12, 31),
                              key="rm_start", format="MM/DD/YYYY", label_visibility="collapsed")
    with _c2:
        end = st.date_input("To", value=date(_yr_int, 12, 31),
                            min_value=date(2024, 1, 1), max_value=date(2025, 12, 31),
                            key="rm_end", format="MM/DD/YYYY", label_visibility="collapsed")
    if isinstance(start, date) and isinstance(end, date) and start > end:
        start, end = end, start
else:
    start, end = preset_dates(horizon, _yr_int)

s_ts = pd.Timestamp(start)
e_ts = pd.Timestamp(end)
n_days_period = (end - start).days + 1

# ── KPI computations ──────────────────────────────────────────────────────────
rt_f = rt_df[(rt_df["business_date"] >= s_ts) & (rt_df["business_date"] <= e_ts)].copy()
rt_f["available"] = rt_f["total_physical_rooms"] - rt_f["ooo_rooms"]
rt_f["lost_rev"]  = rt_f["ooo_rooms"] * rt_f["adr"].fillna(0)

n_capacity_rooms = int(rt_f.groupby("room_type")["total_physical_rooms"].max().sum())
total_capacity   = int(rt_f["total_physical_rooms"].sum())
total_ooo        = int(rt_f["ooo_rooms"].sum())
total_available  = total_capacity - total_ooo
total_sold       = int(rt_f["room_nights"].sum())
occ_pct          = total_sold / total_available * 100 if total_available else 0.0
total_room_rev   = float(rt_f["room_revenue"].sum())
avg_adr          = total_room_rev / total_sold if total_sold else 0.0
revpar           = total_room_rev / total_capacity if total_capacity else 0.0
total_lost_rev   = float(rt_f["lost_rev"].sum())

if _all_years:
    rt_py = pd.DataFrame()
    py_yr = None
    py_capacity = py_ooo = py_available = py_sold = 0
    py_occ_pct = py_adr = py_revpar = py_lost_rev = 0.0
else:
    py_yr = _yr_int - 1
    try:
        py_s = pd.Timestamp(start.replace(year=py_yr))
        py_e = pd.Timestamp(end.replace(year=py_yr))
    except ValueError:
        py_s = pd.Timestamp(start.replace(year=py_yr, day=28))
        py_e = pd.Timestamp(end.replace(year=py_yr, day=28))

    rt_py = rt_df[(rt_df["business_date"] >= py_s) & (rt_df["business_date"] <= py_e)].copy()

    if not rt_py.empty:
        rt_py["available"] = rt_py["total_physical_rooms"] - rt_py["ooo_rooms"]
        rt_py["lost_rev"]  = rt_py["ooo_rooms"] * rt_py["adr"].fillna(0)
        py_capacity  = int(rt_py["total_physical_rooms"].sum())
        py_ooo       = int(rt_py["ooo_rooms"].sum())
        py_available = py_capacity - py_ooo
        py_sold      = int(rt_py["room_nights"].sum())
        py_occ_pct   = py_sold / py_available * 100 if py_available else 0.0
        py_room_rev  = float(rt_py["room_revenue"].sum())
        py_adr       = py_room_rev / py_sold if py_sold else 0.0
        py_revpar    = py_room_rev / py_capacity if py_capacity else 0.0
        py_lost_rev  = float(rt_py["lost_rev"].sum())
    else:
        py_capacity = py_ooo = py_available = py_sold = 0
        py_occ_pct = py_adr = py_revpar = py_lost_rev = 0.0

def _delta_html(v, base, invert=False):
    if base == 0 or rt_py.empty:
        return '<span style="color:rgba(245,245,240,0.35);font-size:10px;">&#8212;</span>'
    d = (v - base) / abs(base) * 100
    good  = (d >= 0 and not invert) or (d < 0 and invert)
    color = "#3ecf8e" if good else "#e05252"
    arrow = "&#9650;" if d >= 0 else "&#9660;"
    sign  = "+" if d >= 0 else ""
    return f'<span style="color:{color};font-size:10px;font-weight:600;">{arrow} {sign}{d:.1f}% vs {py_yr}</span>'

st.markdown(f"""
<div class="rm-kpi-grid">
  <div class="rm-kpi" style="border-top-color:rgba(56,189,248,0.35);">
    <div class="rm-kpi-lbl">Capacity</div>
    <div class="rm-kpi-val">{total_capacity:,}</div>
    <div class="rm-kpi-delta">{n_capacity_rooms} rooms &middot; {n_days_period} nights</div>
    <div class="rm-kpi-desc">Max room-nights that could ever be sold</div>
  </div>
  <div class="rm-kpi">
    <div class="rm-kpi-lbl">Rooms Available</div>
    <div class="rm-kpi-val">{total_available:,}</div>
    <div class="rm-kpi-delta">{_delta_html(total_available, py_available)}</div>
    <div class="rm-kpi-desc">Capacity minus Out-of-Order nights</div>
  </div>
  <div class="rm-kpi" style="border-top-color:#e05252;">
    <div class="rm-kpi-lbl">Rooms OOO</div>
    <div class="rm-kpi-val">{total_ooo:,}</div>
    <div class="rm-kpi-delta">{_delta_html(total_ooo, py_ooo, invert=True)}</div>
    <div class="rm-kpi-desc">Room-nights blocked &mdash; maintenance &amp; holds</div>
  </div>
  <div class="rm-kpi" style="border-top-color:#3ecf8e;">
    <div class="rm-kpi-lbl">Rooms Sold</div>
    <div class="rm-kpi-val">{total_sold:,}</div>
    <div class="rm-kpi-delta">{_delta_html(total_sold, py_sold)}</div>
    <div class="rm-kpi-desc">Room-nights occupied by guests</div>
  </div>
</div>
<div class="rm-kpi-grid" style="padding-top:6px;">
  <div class="rm-kpi">
    <div class="rm-kpi-lbl">Occupancy %</div>
    <div class="rm-kpi-val">{occ_pct:.1f}%</div>
    <div class="rm-kpi-delta">{_delta_html(occ_pct, py_occ_pct)}</div>
    <div class="rm-kpi-desc">Sold &divide; Available &mdash; OOO excluded from base</div>
  </div>
  <div class="rm-kpi">
    <div class="rm-kpi-lbl">ADR</div>
    <div class="rm-kpi-val">${avg_adr:,.0f}</div>
    <div class="rm-kpi-delta">{_delta_html(avg_adr, py_adr)}</div>
    <div class="rm-kpi-desc">Room revenue &divide; rooms sold</div>
  </div>
  <div class="rm-kpi">
    <div class="rm-kpi-lbl">RevPAR</div>
    <div class="rm-kpi-val">${revpar:,.0f}</div>
    <div class="rm-kpi-delta">{_delta_html(revpar, py_revpar)}</div>
    <div class="rm-kpi-desc">Room revenue &divide; total inventory (incl. OOO)</div>
  </div>
  <div class="rm-kpi" style="border-top-color:#e8854a;">
    <div class="rm-kpi-lbl">Lost Revenue (OOO)</div>
    <div class="rm-kpi-val">${total_lost_rev:,.0f}</div>
    <div class="rm-kpi-delta">{_delta_html(total_lost_rev, py_lost_rev, invert=True)}</div>
    <div class="rm-kpi-desc">OOO nights &times; ADR &mdash; opportunity cost</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Room Type Performance Table ───────────────────────────────────────────────
st.markdown('<div class="rm-section"><div class="rm-section-ttl">Room Type Performance</div></div>',
            unsafe_allow_html=True)

RT_NAMES = {
    "LS":   "Loft Suite",
    "SS":   "Studio Suite",
    "SSRS": "Accessible Studio Suite",
    "PLS":  "City View Loft Suite",
    "PSS":  "City View Studio Suite",
    "CVQ":  "City View Queen",
    "Q":    "Standard Queen",
    "QT":   "Queen Terrace",
    "QTRS": "Accessible Queen Terrace",
    "SKT":  "King Terrace",
    "PKT":  "City View King Terrace",
    "PQT":  "City View Queen Terrace",
}

rt_f["is_full"]          = (rt_f["available"] > 0) & (rt_f["room_nights"] >= rt_f["available"])
rt_f["occupancy"]        = rt_f.apply(
    lambda r: r["room_nights"] / r["available"] if r["available"] > 0 else None, axis=1
)
rt_f["lost_revenue_est"] = rt_f["ooo_rooms"] * rt_f["adr"].fillna(0)

ooo_stats = (rt_f.groupby("room_type")
             .agg(
                 ooo_room_nights=("ooo_rooms",            "sum"),
                 lost_revenue   =("lost_revenue_est",     "sum"),
                 _n_rooms       =("total_physical_rooms", "first"),
                 _total_days    =("business_date",        "count"),
             )
             .reset_index())
ooo_stats["total_room_nights_possible"] = ooo_stats["_n_rooms"] * ooo_stats["_total_days"]
ooo_stats["ooo_pct"] = ooo_stats["ooo_room_nights"] / ooo_stats["total_room_nights_possible"] * 100

rt_stats = (rt_f[rt_f["available"] > 0]
            .groupby("room_type")
            .agg(
                n_rooms      =("total_physical_rooms", "first"),
                room_nights  =("room_nights",          "sum"),
                room_revenue =("room_revenue",         "sum"),
                total_revenue=("total_revenue",        "sum"),
                avg_adr      =("adr",                  "mean"),
                avg_occ      =("occupancy",            "mean"),
                sellout_days =("is_full",              "sum"),
                total_days   =("is_full",              "count"),
            )
            .reset_index())

rt_stats = rt_stats.merge(
    ooo_stats[["room_type", "ooo_room_nights", "ooo_pct", "lost_revenue"]],
    on="room_type", how="left"
).fillna(0)

rt_stats["sellout_pct"] = rt_stats["sellout_days"] / rt_stats["total_days"] * 100
rt_stats["revpar"]      = rt_stats["room_revenue"] / (rt_stats["n_rooms"] * n_days_period)
rt_stats["label"]       = rt_stats["room_type"].map(RT_NAMES).fillna(rt_stats["room_type"])

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
     "Estimated revenue lost to OOO rooms — OOO rooms × ADR per night. Represents the opportunity cost of rooms unavailable for sale."),
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
.sort-icon { font-size: 7px; color: #38bdf8; min-width: 7px; }
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
tfoot tr td:first-child { color: #38bdf8; }
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
    height = (len(df) + (1 if totals else 0)) * 36 + 70
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
components.html(_rt_html, height=_rt_height, scrolling=False)

# ── Room Type Revenue Forecast ─────────────────────────────────────────────────
st.markdown('<div class="rm-section"><div class="rm-section-ttl">Room Type Revenue Forecast</div></div>',
            unsafe_allow_html=True)
st.markdown(
    f'<div class="rm-live-bar">'
    f'<div class="rm-live-dot"></div>'
    f'<span class="rm-live-lbl">Live</span>'
    f'<span class="rm-live-dates">Today is {TODAY.strftime("%A, %b %-d, %Y")} &nbsp;·&nbsp; '
    f'select a window below to see which rooms to prioritize</span>'
    f'</div>',
    unsafe_allow_html=True,
)

_rw_col, _ = st.columns([4, 5])
with _rw_col:
    rt_hz = st.segmented_control(
        "Room window",
        ["This Week", "This Weekend", "Next Week", "Next 30d"],
        default="This Week",
        key="rm_rt_hz",
        label_visibility="collapsed",
    )
rt_base_yr = 2025

def _rt_window(w):
    if w == "This Week":
        return TODAY, TODAY + timedelta(days=6)
    if w == "This Weekend":
        days_to_fri = (4 - TODAY.weekday()) % 7 or 7
        fri = TODAY + timedelta(days=days_to_fri)
        return fri, fri + timedelta(days=2)
    if w == "Next Week":
        days_to_mon = (7 - TODAY.weekday()) % 7 or 7
        mon = TODAY + timedelta(days=days_to_mon)
        return mon, mon + timedelta(days=6)
    return TODAY, TODAY + timedelta(days=29)

rt_s, rt_e = _rt_window(rt_hz or "This Week")

try:
    rt_s_base = rt_s.replace(year=rt_base_yr)
    rt_e_base = rt_e.replace(year=rt_base_yr)
except ValueError:
    rt_s_base = rt_s.replace(year=rt_base_yr, day=28)
    rt_e_base = rt_e.replace(year=rt_base_yr, day=28)

rt_w = rt_df[
    (rt_df["business_date"] >= pd.Timestamp(rt_s_base)) &
    (rt_df["business_date"] <= pd.Timestamp(rt_e_base))
].copy()

fc_win = forecast[
    (forecast["business_date"] >= pd.Timestamp(rt_s)) &
    (forecast["business_date"] <= pd.Timestamp(rt_e))
]["pred_revenue"].sum()
act_win = actuals[
    (actuals["business_date"] >= pd.Timestamp(rt_s_base)) &
    (actuals["business_date"] <= pd.Timestamp(rt_e_base))
]["actual_revenue"].sum()
gf = fc_win / act_win if act_win > 0 else 1.0

if not rt_w.empty:
    rt_agg = (
        rt_w.groupby("room_type")
        .agg(
            nights_sold=("room_nights",          "sum"),
            base_rev   =("total_revenue",         "sum"),
            base_adr   =("adr",                   "mean"),
            n_rooms    =("total_physical_rooms",  "first"),
        )
        .reset_index()
    )
    rt_agg["proj_rev"] = rt_agg["base_rev"] * gf
    rt_agg["proj_adr"] = rt_agg["base_adr"] * gf
    rt_agg["label"]    = rt_agg["room_type"].map(RT_NAMES).fillna(rt_agg["room_type"])
    rt_agg = rt_agg.sort_values("proj_rev", ascending=False).reset_index(drop=True)
    total_proj = rt_agg["proj_rev"].sum() or 1.0
    rt_agg["share"] = rt_agg["proj_rev"] / total_proj * 100

    RCOLORS  = ["#38bdf8", "#0ea5e9", "#4a6fa5", "#3ecf8e"] + ["rgba(245,245,240,0.22)"] * 20
    date_rng = f"{rt_s.strftime('%b %-d')} – {rt_e.strftime('%b %-d, %Y')}"
    gf_note  = f"{gf:.2f}× YoY growth factor applied"

    rows_html = ""
    for idx, r in rt_agg.iterrows():
        rk = int(idx) + 1
        rc = RCOLORS[min(rk - 1, len(RCOLORS) - 1)]
        rows_html += (
            f'<div style="display:flex;align-items:center;gap:10px;padding:8px 0;'
            f'border-bottom:1px solid rgba(255,255,255,0.06);">'
            f'<div style="width:22px;height:22px;border-radius:3px;background:{rc};display:flex;'
            f'align-items:center;justify-content:center;font-size:9px;font-weight:700;'
            f'color:#111;flex-shrink:0;">{rk}</div>'
            f'<div style="width:34px;font-size:9px;font-weight:600;letter-spacing:.06em;'
            f'color:rgba(245,245,240,0.4);flex-shrink:0;text-transform:uppercase;">{r["room_type"]}</div>'
            f'<div style="flex:1;min-width:80px;">'
            f'<div style="font-size:11px;color:#f5f5f0;white-space:nowrap;overflow:hidden;'
            f'text-overflow:ellipsis;">{r["label"]}</div>'
            f'<div style="height:3px;border-radius:2px;background:rgba(255,255,255,0.06);margin-top:4px;">'
            f'<div style="height:3px;border-radius:2px;background:{rc};width:{r["share"]:.1f}%;"></div>'
            f'</div></div>'
            f'<div style="text-align:right;flex-shrink:0;min-width:72px;">'
            f'<div style="font-size:12px;font-weight:600;color:#f5f5f0;">${r["proj_rev"]:,.0f}</div>'
            f'<div style="font-size:9px;color:rgba(245,245,240,0.38);">{r["share"]:.1f}% of total</div>'
            f'</div>'
            f'<div style="text-align:right;flex-shrink:0;min-width:52px;">'
            f'<div style="font-size:12px;color:rgba(245,245,240,0.6);">${r["proj_adr"]:,.0f}</div>'
            f'<div style="font-size:9px;color:rgba(245,245,240,0.38);">ADR</div>'
            f'</div>'
            f'<div style="text-align:right;flex-shrink:0;min-width:40px;">'
            f'<div style="font-size:12px;color:rgba(245,245,240,0.45);">{int(r["nights_sold"])}</div>'
            f'<div style="font-size:9px;color:rgba(245,245,240,0.38);">nights</div>'
            f'</div>'
            f'</div>'
        )

    _ra, _rb = st.columns([3, 2])

    with _ra:
        st.markdown(
            f'<div style="padding:0 0 0 20px;">'
            f'<div style="background:#111111;border:1px solid rgba(255,255,255,0.08);'
            f'border-radius:6px;padding:16px 18px;">'
            f'<div style="font-size:10px;color:rgba(245,245,240,0.35);margin-bottom:14px;">'
            f'{date_rng} &nbsp;&middot;&nbsp; {gf_note}</div>'
            f'{rows_html}'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    with _rb:
        top3  = rt_agg.head(3)
        cards = ""
        for idx, r in top3.iterrows():
            rk = int(idx) + 1
            rc = RCOLORS[rk - 1]
            cards += (
                f'<div style="background:#111111;border:1px solid rgba(255,255,255,0.08);'
                f'border-left:3px solid {rc};border-radius:6px;padding:14px 16px;margin-bottom:8px;">'
                f'<div style="font-size:8px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;'
                f'color:{rc};margin-bottom:5px;">#{rk} Priority</div>'
                f'<div style="font-size:14px;font-weight:600;color:#f5f5f0;margin-bottom:1px;">{r["label"]}</div>'
                f'<div style="font-size:9px;color:rgba(245,245,240,0.38);margin-bottom:10px;">'
                f'{r["room_type"]} &nbsp;&middot;&nbsp; {int(r["n_rooms"])} rooms</div>'
                f'<div style="display:flex;gap:18px;">'
                f'<div><div style="font-size:17px;font-weight:600;color:{rc};">${r["proj_rev"]:,.0f}</div>'
                f'<div style="font-size:8px;text-transform:uppercase;letter-spacing:.1em;'
                f'color:rgba(245,245,240,0.3);">Est. Revenue</div></div>'
                f'<div><div style="font-size:17px;font-weight:600;color:rgba(245,245,240,0.7);">'
                f'${r["proj_adr"]:,.0f}</div>'
                f'<div style="font-size:8px;text-transform:uppercase;letter-spacing:.1em;'
                f'color:rgba(245,245,240,0.3);">ADR</div></div>'
                f'</div></div>'
            )

        top1 = top3.iloc[0]["label"] if len(top3) > 0 else "—"
        top2 = top3.iloc[1]["label"] if len(top3) > 1 else "—"
        st.markdown(
            f'<div style="padding:0 20px 0 0;">{cards}'
            f'<div style="background:rgba(56,189,248,0.07);border:1px solid rgba(56,189,248,0.2);'
            f'border-radius:6px;padding:12px 14px;">'
            f'<div style="font-size:9px;font-weight:600;letter-spacing:.12em;text-transform:uppercase;'
            f'color:#38bdf8;margin-bottom:6px;">HSK &amp; ENG Priority</div>'
            f'<div style="font-size:10px;color:rgba(245,245,240,0.6);line-height:1.7;">'
            f'Schedule deep-clean and preventive maintenance for '
            f'<strong style="color:#f5f5f0;">{top1}</strong> and '
            f'<strong style="color:#f5f5f0;">{top2}</strong> '
            f'before this window opens. These rooms generate the highest revenue per available room '
            f'and guest experience impact is greatest here.'
            f'</div></div></div>',
            unsafe_allow_html=True,
        )
else:
    st.markdown(
        '<div style="padding:0 20px;font-size:10px;color:rgba(245,245,240,0.4);">'
        'No room-type data found for the equivalent prior-year period.</div>',
        unsafe_allow_html=True,
    )

st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
