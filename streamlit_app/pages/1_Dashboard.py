import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.data import load_master, load_events, load_str
from components.sidebar import render_sidebar

# ── Session state ─────────────────────────────────────────────────────────────
if "db_dow_filter" not in st.session_state:
    st.session_state["db_dow_filter"] = None


# ── CDN: Inter font + Tabler Icons ────────────────────────────────────────────
st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@2.47.0/tabler-icons.min.css">
""", unsafe_allow_html=True)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
:root {{
  --arlo-black:#0a0a0a; --arlo-dark:#111111; --arlo-dark2:#1a1a1a; --arlo-dark3:#222222;
  --arlo-border:rgba(255,255,255,0.08); --arlo-border2:rgba(255,255,255,0.14);
  --arlo-white:#f5f5f0; --arlo-muted:rgba(245,245,240,0.38); --arlo-muted2:rgba(245,245,240,0.6);
  --arlo-accent:#e8854a; --arlo-accent2:rgba(232,133,74,0.15); --arlo-accent3:rgba(232,133,74,0.08);
  --arlo-green:#3ecf8e; --arlo-green2:rgba(62,207,142,0.12);
  --arlo-red:#e05252;   --arlo-red2:rgba(224,82,82,0.12);
  --arlo-amber:#d4903a; --arlo-amber2:rgba(212,144,58,0.12);
  --arlo-slate:#4a6fa5; --arlo-slate2:rgba(74,111,165,0.12);
  --r:4px;
}}
*, *::before, *::after {{ font-family:'Inter',system-ui,sans-serif !important; -webkit-font-smoothing:antialiased; box-sizing:border-box; }}
.material-symbols-rounded {{ font-family:'Material Symbols Rounded' !important; }}
#MainMenu, footer, header {{ visibility:hidden; }}
[data-testid="collapsedControl"],
[data-testid="stSidebarCollapsedControl"],
button[title="Collapse sidebar"],
button[aria-label="Collapse sidebar"],
section[data-testid="stSidebar"] > div > div > div > button:first-child {{ display:none !important; }}
.stApp {{ background:var(--arlo-dark2) !important; }}
.block-container {{ padding:0 !important; max-width:1300px !important; margin:0 auto !important; }}
[data-testid="stAppViewContainer"] {{ background:var(--arlo-dark2) !important; }}

/* ── Date input dark theme ── */
.arlo-topbar-row {{ background:var(--arlo-dark); border-bottom:1px solid var(--arlo-border); }}
.arlo-topbar-row [data-testid="stHorizontalBlock"] {{ gap:0 !important; }}
.arlo-topbar-row [data-testid="column"] {{ padding:0 !important; }}
.arlo-topbar-row .stDateInput > label {{ display:none !important; }}
.arlo-topbar-row .stDateInput div[data-baseweb="input"] {{
  background:rgba(255,255,255,.04) !important; border:1px solid var(--arlo-border2) !important;
  border-radius:4px !important; height:30px !important;
}}
.arlo-topbar-row .stDateInput input {{
  color:var(--arlo-muted2) !important; font-size:11px !important;
  caret-color:var(--arlo-accent) !important;
}}
.arlo-topbar-row .stDateInput [data-baseweb="calendar"] {{
  background:var(--arlo-dark3) !important; border:1px solid var(--arlo-border2) !important;
}}

/* ── Main content ── */
.arlo-topbar {{
  background:var(--arlo-dark); border-bottom:1px solid var(--arlo-border);
  height:52px; display:flex; align-items:center; padding:0 20px; gap:12px;
}}
.arlo-top-title {{ font-size:42px; font-weight:700; color:var(--arlo-white); letter-spacing:-.02em; }}
.arlo-live-chip {{
  display:flex; align-items:center; gap:5px;
  padding:3px 9px; border-radius:20px;
  border:1px solid rgba(62,207,142,0.25); background:rgba(62,207,142,0.08);
  font-size:9px; font-weight:600; color:var(--arlo-green);
  letter-spacing:.1em; text-transform:uppercase;
}}
.arlo-live-dot {{
  width:5px; height:5px; border-radius:50%; background:var(--arlo-green);
  animation:arlo-pulse 2s infinite;
}}
@keyframes arlo-pulse {{ 0%,100%{{opacity:1}}50%{{opacity:.4}} }}
.arlo-date-pill {{
  font-size:10px; color:var(--arlo-muted2); padding:5px 11px;
  border:1px solid var(--arlo-border); border-radius:var(--r);
  background:rgba(255,255,255,0.03); white-space:nowrap;
}}
.arlo-top-btn {{
  width:30px; height:30px; border-radius:var(--r);
  background:rgba(255,255,255,0.04); border:1px solid var(--arlo-border);
  display:inline-flex; align-items:center; justify-content:center;
  color:var(--arlo-muted); font-size:14px;
}}
.arlo-accent-rule {{
  height:1px;
  background:linear-gradient(90deg,var(--arlo-accent) 0%,rgba(232,133,74,.3) 40%,transparent 70%);
  opacity:.6;
}}
.arlo-body {{ padding:18px 20px; display:flex; flex-direction:column; gap:14px; }}

/* Section headers */
.sec-hd {{ display:flex; align-items:center; gap:10px; margin-bottom:9px; }}
.sec-lbl {{ font-size:27px; font-weight:600; letter-spacing:.2em; text-transform:uppercase; color:var(--arlo-muted); white-space:nowrap; }}
.sec-line {{ flex:1; height:1px; background:var(--arlo-border); }}
.sec-line.accent {{ background:linear-gradient(90deg,rgba(232,133,74,.4),transparent); }}

/* AI box */
.ai-box {{
  background:var(--arlo-dark3); border:1px solid rgba(232,133,74,.2);
  border-left:2px solid var(--arlo-accent); border-radius:var(--r);
  padding:12px 16px; display:flex; gap:12px; align-items:flex-start;
}}
.ai-icon {{ font-size:14px; color:var(--arlo-accent); flex-shrink:0; margin-top:1px; }}
.ai-txt {{ font-size:11px; color:var(--arlo-muted2); line-height:1.65; }}
.ai-txt strong {{ color:var(--arlo-white); font-weight:600; }}

/* KPI cards */
.kpi5 {{ display:grid; grid-template-columns:repeat(4,1fr); gap:8px; }}
.kc {{
  background:var(--arlo-dark3); border:1px solid var(--arlo-border);
  border-radius:var(--r); padding:14px 12px; position:relative; overflow:hidden;
}}
.kc::before {{ content:''; position:absolute; top:0; left:0; right:0; height:2px; background:var(--arlo-border2); }}
.kc.hero {{ background:var(--arlo-dark); border-color:rgba(232,133,74,.25); }}
.kc.hero::before {{ background:var(--arlo-accent); opacity:.8; }}
.kc.kup::before {{ background:var(--arlo-green); opacity:.5; }}
.kc.kdn::before {{ background:var(--arlo-red); opacity:.5; }}
.kpi-lbl {{ font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--arlo-muted); margin-bottom:8px; }}
.kpi-val {{ font-size:22px; font-weight:300; color:var(--arlo-white); line-height:1; margin-bottom:6px; letter-spacing:-.02em; }}
.kpi-delta {{ font-size:10px; font-weight:500; color:var(--arlo-green); display:flex; align-items:center; gap:3px; }}
.kpi-delta.neg {{ color:var(--arlo-red); }}
.kpi-sub {{ font-size:9px; color:var(--arlo-muted); margin-top:3px; }}
.spark {{ margin-top:10px; }}

/* Mini cards */
.mini3 {{ display:grid; grid-template-columns:repeat(3,1fr); gap:8px; }}
.mc {{ background:var(--arlo-dark3); border:1px solid var(--arlo-border); border-radius:var(--r); padding:12px; }}
.mv {{ font-size:16px; font-weight:300; color:var(--arlo-white); }}
.ml {{ font-size:9px; font-weight:600; letter-spacing:.14em; text-transform:uppercase; color:var(--arlo-muted); margin-top:4px; }}
.md {{ font-size:10px; font-weight:500; margin-top:5px; }}

/* Grid layouts */
.row2  {{ display:grid; grid-template-columns:minmax(0,1.5fr) minmax(0,1fr); gap:10px; }}
.row3  {{ display:grid; grid-template-columns:minmax(0,1.3fr) minmax(0,1fr) minmax(0,1fr); gap:10px; }}
.row22 {{ display:grid; grid-template-columns:minmax(0,1fr) minmax(0,1fr); gap:10px; }}
.panel {{ background:var(--arlo-dark3); border:1px solid var(--arlo-border); border-radius:var(--r); padding:16px; }}
.pt {{ font-size:27px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--arlo-muted); margin-bottom:2px; }}
.ps {{ font-size:10px; color:var(--arlo-muted); margin-bottom:12px; }}

/* Booking pace */
.pace-row {{ display:flex; align-items:center; gap:7px; padding:7px 0; border-bottom:1px solid var(--arlo-border); }}
.pace-row:last-child {{ border:none; }}
.pace-per  {{ font-size:10px; color:var(--arlo-muted2); width:66px; flex-shrink:0; }}
.pace-onb  {{ font-size:12px; font-weight:300; color:var(--arlo-white); width:34px; flex-shrink:0; }}
.pace-bar  {{ flex:1; height:4px; background:rgba(255,255,255,.06); border-radius:2px; overflow:hidden; }}
.pace-fill {{ height:100%; border-radius:2px; }}
.pace-stly {{ font-size:9px; color:var(--arlo-muted); width:26px; text-align:right; flex-shrink:0; }}
.pace-pill {{ font-size:8px; font-weight:600; padding:2px 8px; border-radius:2px; letter-spacing:.06em; width:52px; text-align:center; flex-shrink:0; text-transform:uppercase; }}

/* DOW chart */
.dow-grid {{ display:grid; grid-template-columns:repeat(7,1fr); gap:5px; text-align:center; }}
.dow-col  {{ display:flex; flex-direction:column; align-items:center; gap:4px; }}
.dow-wrap {{ width:100%; display:flex; flex-direction:column; justify-content:flex-end; height:74px; }}
.dow-bar  {{ width:100%; border-radius:2px 2px 0 0; }}
.dow-day  {{ font-size:8px; font-weight:500; letter-spacing:.08em; text-transform:uppercase; color:var(--arlo-muted); }}
.dow-pct  {{ font-size:10px; font-weight:300; color:var(--arlo-white); }}

/* Segments */
.seg-rows  {{ display:flex; flex-direction:column; gap:8px; }}
.seg-item  {{ display:flex; align-items:center; gap:8px; }}
.seg-lbl   {{ font-size:10px; color:var(--arlo-muted2); width:64px; flex-shrink:0; }}
.seg-track {{ flex:1; height:12px; background:rgba(255,255,255,.05); border-radius:2px; overflow:hidden; }}
.seg-fill  {{ height:100%; border-radius:2px; display:flex; align-items:center; justify-content:flex-end; padding-right:4px; }}
.seg-pct   {{ font-size:8px; font-weight:600; }}
.seg-amt   {{ font-size:9px; color:var(--arlo-muted); width:36px; text-align:right; flex-shrink:0; }}

/* Channel mix */
.ch-rows  {{ display:flex; flex-direction:column; gap:7px; }}
.ch-row   {{ display:flex; align-items:center; gap:8px; }}
.ch-mark  {{ width:2px; height:24px; border-radius:1px; flex-shrink:0; }}
.ch-name  {{ font-size:10px; color:var(--arlo-muted2); flex:1; }}
.ch-pct   {{ font-size:13px; font-weight:300; color:var(--arlo-white); }}
.ch-badge {{ font-size:8px; font-weight:600; padding:2px 6px; border-radius:2px; letter-spacing:.04em; text-transform:uppercase; }}

/* Events */
.ev-rows {{ display:flex; flex-direction:column; gap:5px; }}
.ev-item {{ display:flex; align-items:center; gap:9px; padding:8px 10px; border-radius:var(--r); border:1px solid var(--arlo-border); background:rgba(255,255,255,.02); }}
.ev-line {{ width:2px; height:28px; border-radius:1px; flex-shrink:0; }}
.ev-name {{ font-size:10px; font-weight:500; color:var(--arlo-white); }}
.ev-date {{ font-size:9px; color:var(--arlo-muted); margin-top:2px; }}
.ev-imp  {{ font-size:9px; font-weight:600; padding:2px 7px; border-radius:2px; margin-left:auto; white-space:nowrap; letter-spacing:.04em; text-transform:uppercase; flex-shrink:0; }}

/* Competitive */
.comp-rows {{ display:flex; flex-direction:column; gap:5px; }}
.comp-item {{ display:flex; align-items:center; gap:7px; padding:7px 9px; border-radius:var(--r); border:1px solid var(--arlo-border); }}
.comp-item.self {{ border-color:rgba(232,133,74,.25); background:var(--arlo-accent3); }}
.comp-name {{ font-size:10px; color:var(--arlo-muted2); width:82px; flex-shrink:0; }}
.comp-item.self .comp-name {{ color:var(--arlo-accent); font-weight:600; }}
.comp-bw {{ flex:1; height:3px; background:rgba(255,255,255,.06); border-radius:1px; overflow:hidden; }}
.comp-bf {{ height:100%; border-radius:1px; }}
.comp-rate {{ font-size:30px; color:var(--arlo-white); width:36px; text-align:right; flex-shrink:0; }}
.comp-item.self .comp-rate {{ color:var(--arlo-accent); }}
.comp-idx {{ display:flex; padding-top:10px; border-top:1px solid var(--arlo-border); margin-top:7px; }}
.ci   {{ flex:1; text-align:center; }}
.ci-v {{ font-size:13px; font-weight:300; color:var(--arlo-white); }}
.ci-l {{ font-size:8px; font-weight:600; letter-spacing:.1em; text-transform:uppercase; color:var(--arlo-muted); }}

/* Medallia */
.score-n   {{ font-size:36px; font-weight:200; color:var(--arlo-white); line-height:1; }}
.score-s   {{ color:var(--arlo-accent); font-size:13px; letter-spacing:2px; margin-top:2px; }}
.score-l   {{ font-size:8px; font-weight:600; letter-spacing:.12em; text-transform:uppercase; color:var(--arlo-muted); margin-top:3px; }}
.rev-top   {{ display:flex; align-items:center; gap:14px; padding-bottom:11px; border-bottom:1px solid var(--arlo-border); margin-bottom:11px; }}
.rev-rows2 {{ display:flex; flex-direction:column; gap:6px; }}
.rev-row   {{ display:flex; align-items:center; gap:8px; }}
.rev-cat   {{ font-size:9px; color:var(--arlo-muted); width:62px; flex-shrink:0; }}
.rev-bar   {{ flex:1; height:3px; background:rgba(255,255,255,.06); border-radius:1px; overflow:hidden; }}
.rev-fill  {{ height:100%; border-radius:1px; }}
.rev-num   {{ font-size:9px; color:var(--arlo-white); width:20px; text-align:right; }}
</style>
""", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
master = load_master()
events = load_events()
str_df = load_str()

_max_yr = int(master["business_date"].dt.year.max())
_min_yr = int(master["business_date"].dt.year.min())

# Year / horizon selector
if "db_horizon" not in st.session_state:
    st.session_state["db_horizon"] = str(_max_yr)
_hz = st.session_state["db_horizon"]

_all_years_mode = _hz == "All Years"

if _hz not in ("All Years", "Custom"):
    yr = int(_hz)
    _start, _end, _sel_yr = pd.Timestamp(f"{yr}-01-01"), pd.Timestamp(f"{yr}-12-31"), yr
elif _hz == "All Years":
    _start  = master["business_date"].min()
    _end    = master["business_date"].max()
    _sel_yr = _max_yr
else:  # Custom
    _dr_default = (
        master["business_date"].min().date(),
        master["business_date"].max().date(),
    )
    _dr = st.session_state.get("dr", _dr_default)
    _dr = _dr if (isinstance(_dr, (tuple, list)) and len(_dr) == 2) else _dr_default
    _start  = pd.Timestamp(_dr[0])
    _end    = pd.Timestamp(_dr[1])
    _sel_yr = int(_end.year)

_prev_yr = _sel_yr - 1

curr = master[
    (master["business_date"] >= _start) & (master["business_date"] <= _end)
].copy()

if _all_years_mode:
    prev_ytd = pd.DataFrame(columns=curr.columns)
else:
    prev = master[master["business_date"].dt.year == _prev_yr].copy()
    try:
        _start_prev = _start.replace(year=_prev_yr)
        _end_prev   = _end.replace(year=_prev_yr)
    except ValueError:
        _start_prev = _start.replace(year=_prev_yr, day=28)
        _end_prev   = _end.replace(year=_prev_yr, day=28)
    prev_ytd = prev[
        (prev["business_date"] >= _start_prev) & (prev["business_date"] <= _end_prev)
    ]

# keep alias for any downstream refs
latest_yr = _sel_yr

# ── DOW filter ────────────────────────────────────────────────────────────────
_dow_filter = st.session_state["db_dow_filter"]
_dow_names  = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

if _dow_filter is not None:
    curr_kpi = curr[curr["day_of_week"] == _dow_filter].copy()
    prev_kpi = prev_ytd[prev_ytd["day_of_week"] == _dow_filter].copy() if not prev_ytd.empty else prev_ytd
else:
    curr_kpi = curr
    prev_kpi = prev_ytd

def pct(a, b):
    return (a - b) / abs(b) * 100 if b else 0.0

rev_c  = curr_kpi["total_revenue"].sum()
occ_c  = curr_kpi["occupancy_rate"].mean() * 100
adr_c  = curr_kpi["adr"].mean()
rp_c   = curr_kpi["revpar"].mean()
trp_c  = (curr_kpi["total_revenue"] / curr_kpi["available_rooms"]).mean()

if not prev_kpi.empty:
    rev_p  = prev_kpi["total_revenue"].sum()
    occ_p  = prev_kpi["occupancy_rate"].mean() * 100
    adr_p  = prev_kpi["adr"].mean()
    rp_p   = prev_kpi["revpar"].mean()
    trp_p  = (prev_kpi["total_revenue"] / prev_kpi["available_rooms"]).mean()
    rev_d, occ_d, adr_d, rp_d, trp_d = (
        pct(rev_c, rev_p), pct(occ_c, occ_p), pct(adr_c, adr_p),
        pct(rp_c, rp_p),   pct(trp_c, trp_p),
    )
else:
    rev_p = occ_p = adr_p = rp_p = trp_p = 0.0
    rev_d = occ_d = adr_d = rp_d = trp_d = None


# STR indices
if not str_df.empty:
    s25 = str_df[str_df["business_date"].dt.year == _sel_yr]
    mpi_val      = s25["mpi"].mean() / 100 if "mpi" in s25 else None
    ari_val      = s25["ari"].mean() / 100 if "ari" in s25 else None
    rgi_val      = s25["rgi"].mean() / 100 if "rgi" in s25 else None
    str_adr_val  = s25["str_adr"].mean()   if "str_adr"  in s25 else adr_c
    comp_adr_val = s25["comp_adr"].mean()  if "comp_adr" in s25 else None
    comp_occ_val = s25["comp_occ"].mean() * 100 if "comp_occ" in s25 else None
    comp_rp_val  = s25["comp_revpar"].mean() if "comp_revpar" in s25 else None
else:
    mpi_val = ari_val = rgi_val = None
    str_adr_val = adr_c; comp_adr_val = comp_occ_val = comp_rp_val = None

# Medallia
med = curr_kpi[curr_kpi["medallia_overall_satisfaction"].notna()]
if not med.empty:
    med_overall = med["medallia_overall_satisfaction"].mean()
    med_clean   = med["medallia_hotel_cleanliness"].mean()   if "medallia_hotel_cleanliness"   in med else None
    med_value   = med["medallia_value_for_price"].mean()     if "medallia_value_for_price"     in med else None
    med_rec     = med["medallia_likelihood_to_recommend"].mean() if "medallia_likelihood_to_recommend" in med else None
    med_return  = med["medallia_likelihood_to_return"].mean()    if "medallia_likelihood_to_return"    in med else None
    # Sample size is weekly but stamped on every day — dedupe by ISO week to avoid 7× inflation
    med_samples = int(
        med.groupby(med["business_date"].dt.isocalendar().week)["medallia_sample_size"]
           .first().sum()
    )
else:
    med_overall = None

# Sparklines (use DOW-filtered data so they match the KPIs)
curr_kpi["month"] = curr_kpi["business_date"].dt.month
rev_spark  = curr_kpi.groupby("month")["total_revenue"].sum().values
occ_spark  = curr_kpi.groupby("month")["occupancy_rate"].mean().values * 100
adr_spark  = curr_kpi.groupby("month")["adr"].mean().values
rp_spark   = curr_kpi.groupby("month")["revpar"].mean().values
trp_spark  = curr_kpi.groupby("month").apply(
    lambda g: g["total_revenue"].sum() / g["available_rooms"].sum()
).values

def svg_spark(vals, color):
    if len(vals) < 2:
        return ""
    mn, mx = float(np.min(vals)), float(np.max(vals))
    rng = mx - mn if mx != mn else 1.0
    pts = " ".join(
        f"{i/(len(vals)-1)*80:.1f},{18 - ((float(v)-mn)/rng)*14 - 2:.1f}"
        for i, v in enumerate(vals)
    )
    return f'<svg width="100%" height="20" viewBox="0 0 80 20" preserveAspectRatio="none"><polyline points="{pts}" fill="none" stroke="{color}" stroke-width="1.5" stroke-linejoin="round"/></svg>'

# DOW occupancy (always use full unfiltered curr so all 7 bars show)
dow_avg  = curr.groupby("day_of_week")["occupancy_rate"].mean() * 100
dow_vals = [float(dow_avg.get(i, 0)) for i in range(7)]

# NYC events: next 4 upcoming major events
today_ts = pd.Timestamp.today().normalize()
_event_types = [
    ("is_us_open_event",       "US Open Tennis",    "#4a6fa5"),
    ("is_msg_event",           "MSG Event",         "#d4903a"),
    ("is_barclays_event",      "Barclays Event",    "#d4903a"),
    ("is_major_concert",       "Major Concert",     "#e8854a"),
    ("is_yankees_game",        "Yankees Game",      "#4a6fa5"),
    ("is_mets_game",           "Mets Game",         "#3ecf8e"),
    ("is_knicks_game",         "Knicks Game",       "#4a6fa5"),
    ("is_nets_game",           "Nets Game",         "#3ecf8e"),
    ("is_brooklyn_music_event","Brooklyn Music",    "#d4903a"),
    ("is_major_sports_event",  "Major Sports",      "#4a6fa5"),
]
_ev_display = []
_ev_pool = events[events["business_date"] >= today_ts].sort_values("business_date")
if _ev_pool.empty:
    _ev_pool = events[events["is_major_event_day"] == True].sort_values("business_date", ascending=False)
for _, row in _ev_pool.iterrows():
    for col, label, color in _event_types:
        if col in row.index and row[col]:
            _ev_display.append({"name": label, "date": row["business_date"], "color": color})
            break
    if len(_ev_display) >= 4:
        break

# Date display
if _all_years_mode or _hz == "Custom":
    date_str = f"{_start.strftime('%b %-d, %Y')} – {_end.strftime('%b %-d, %Y')}"
else:
    date_str = f"Jan 1 – {curr['business_date'].max().strftime('%b %-d')}, {latest_yr}"

_period_label = f"{_min_yr} – {_max_yr}" if _all_years_mode else f"YTD {latest_yr}"

# User info
name     = st.session_state.get("name", "")
initials = "".join(p[0].upper() for p in name.split()[:2]) if name else "JR"

# ── Sidebar ───────────────────────────────────────────────────────────────────
render_sidebar(active="dashboard")

# ── Top bar ───────────────────────────────────────────────────────────────────
st.markdown('<div class="arlo-topbar-row">', unsafe_allow_html=True)
_tb_l, _tb_r = st.columns([5, 3])
with _tb_l:
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:14px;height:72px;padding:0 20px;">
      <span class="arlo-top-title">Dashboard</span>
      <div class="arlo-live-chip" style="font-size:11px;padding:4px 11px;"><div class="arlo-live-dot"></div>Live</div>
    </div>""", unsafe_allow_html=True)
with _tb_r:
    st.markdown('<div style="display:flex;align-items:center;justify-content:flex-end;height:72px;padding-right:16px;gap:10px;">', unsafe_allow_html=True)
    if _hz == "Custom":
        _dr_picked = st.date_input(
            "Date range",
            value=(_dr[0], _dr[1]),
            min_value=master["business_date"].min().date(),
            max_value=master["business_date"].max().date(),
            key="dr",
            label_visibility="collapsed",
            format="MM/DD/YYYY",
        )
        if isinstance(_dr_picked, (tuple, list)) and len(_dr_picked) == 2:
            if _dr_picked != tuple(_dr):
                st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)
st.markdown('</div><div class="arlo-accent-rule"></div>', unsafe_allow_html=True)

# ── Year / horizon selector ───────────────────────────────────────────────────
_years = [str(y) for y in range(_min_yr, _max_yr + 1)] + ["All Years", "Custom"]
_hz_new = st.segmented_control(
    "Year", _years,
    default=_hz,
    key="db_hz_ctrl",
    label_visibility="collapsed",
)
if _hz_new and _hz_new != _hz:
    st.session_state["db_horizon"] = _hz_new
    st.rerun()

# ── DOW filter badge ──────────────────────────────────────────────────────────
if _dow_filter is not None:
    _fc1, _fc2 = st.columns([8, 1])
    with _fc1:
        st.markdown(
            f'<div style="padding:4px 20px 0;display:flex;align-items:center;gap:8px;">'
            f'<span style="font-size:9px;font-weight:600;letter-spacing:.14em;text-transform:uppercase;'
            f'color:rgba(245,245,240,0.35);">Filtered by</span>'
            f'<span style="background:#e8854a;color:#111;font-size:9px;font-weight:700;'
            f'letter-spacing:.1em;text-transform:uppercase;padding:2px 9px;border-radius:2px;">'
            f'{_dow_names[_dow_filter]}s</span>'
            f'<span style="font-size:9px;color:rgba(245,245,240,0.35);">'
            f'— showing all {_dow_names[_dow_filter]}s in the selected period</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    with _fc2:
        if st.button("✕ Clear", key="clear_dow_filter"):
            st.session_state["db_dow_filter"] = None
            st.rerun()

# ── Core performance KPIs ─────────────────────────────────────────────────────
def _delta_html(d, hero=False):
    if d is None:
        return '<div class="kpi-delta" style="color:var(--arlo-muted);">— no STLY</div>'
    arrow = "▲" if d >= 0 else "▼"
    sign  = "+" if d >= 0 else ""
    cls   = "" if d >= 0 else " neg"
    accent = ' style="color:var(--arlo-accent)"' if hero else ""
    return f'<div class="kpi-delta{cls}"{accent}>{arrow} {sign}{d:.1f}% STLY</div>'

mpi_sub  = f"MPI {mpi_val:.2f} vs comp"  if mpi_val else "vs comp set"
ari_sub  = f"ARI {ari_val:.2f} vs comp"  if ari_val else "vs comp set"
rgi_sub  = f"RGI {rgi_val:.2f} vs comp"  if rgi_val else "vs comp set"

_rev_val  = f"{rev_c/1e6:.1f}M"
_adr_val  = f"{adr_c:.0f}"
_rp_val   = f"{rp_c:.0f}"
_trp_val  = f"{trp_c:.0f}"

st.html(f"""
<div style="padding:14px 20px 0;">
  <div class="sec-hd"><span class="sec-lbl">Core performance</span><div class="sec-line accent"></div></div>
  <div class="kpi5">

    <div class="kc kup">
      <div class="kpi-lbl">Revenue</div>
      <div class="kpi-val">${_rev_val}</div>
      {_delta_html(rev_d)}
      <div class="kpi-sub">{_period_label}</div>
      <div class="spark">{svg_spark(rev_spark,"rgba(62,207,142,0.5)")}</div>
    </div>

    <div class="kc">
      <div class="kpi-lbl">Occupancy</div>
      <div class="kpi-val">{occ_c:.1f}%</div>
      {_delta_html(occ_d)}
      <div class="kpi-sub">{mpi_sub}</div>
      <div class="spark">{svg_spark(occ_spark,"rgba(74,111,165,0.5)")}</div>
    </div>

    <div class="kc">
      <div class="kpi-lbl">ADR</div>
      <div class="kpi-val">${_adr_val}</div>
      {_delta_html(adr_d)}
      <div class="kpi-sub">{ari_sub}</div>
      <div class="spark">{svg_spark(adr_spark,"rgba(212,144,58,0.5)")}</div>
    </div>

    <div class="kc hero">
      <div class="kpi-lbl">RevPAR</div>
      <div class="kpi-val">${_rp_val}</div>
      {_delta_html(rp_d, hero=True)}
      <div class="kpi-sub">{rgi_sub}</div>
      <div class="spark">{svg_spark(rp_spark,"rgba(232,133,74,0.6)")}</div>
    </div>


  </div>
</div>
""")

# ── Mini metric cards ─────────────────────────────────────────────────────────
st.html("""
<div style="padding:0 20px;">
  <div class="mini3">
    <div class="mc">
      <div class="mv">2.4 nights</div>
      <div class="ml">Avg length of stay</div>
      <div class="md" style="color:var(--arlo-red);">&#9660; 0.2 vs STLY</div>
    </div>
    <div class="mc">
      <div class="mv">26%</div>
      <div class="ml">Direct booking share</div>
      <div class="md" style="color:var(--arlo-green);">&#9650; 3 pts vs STLY</div>
    </div>
    <div class="mc">
      <div class="mv">18.4 days</div>
      <div class="ml">Avg booking window</div>
      <div class="md" style="color:var(--arlo-green);">&#9650; 2.1 days vs STLY</div>
    </div>
  </div>
</div>
""")

# ── Booking pace + DOW occupancy ──────────────────────────────────────────────
_bp_col, _dow_col = st.columns([1.5, 1])

with _bp_col:
    st.html("""
<div style="padding:0 20px 0 20px;">
  <div class="panel">
    <div class="pt">Booking pace — next 30 days</div>
    <div class="ps">On-books vs same point last year</div>
    <div class="pace-row">
      <div class="pace-per">This week</div><div class="pace-onb">94%</div>
      <div class="pace-bar"><div class="pace-fill" style="width:94%;background:var(--arlo-green);"></div></div>
      <div class="pace-stly">89%</div>
      <div class="pace-pill" style="background:var(--arlo-green2);color:var(--arlo-green);">Strong</div>
    </div>
    <div class="pace-row">
      <div class="pace-per">Next week</div><div class="pace-onb">71%</div>
      <div class="pace-bar"><div class="pace-fill" style="width:71%;background:var(--arlo-red);"></div></div>
      <div class="pace-stly">78%</div>
      <div class="pace-pill" style="background:var(--arlo-red2);color:var(--arlo-red);">Slow</div>
    </div>
    <div class="pace-row">
      <div class="pace-per">+ 2 weeks</div><div class="pace-onb">48%</div>
      <div class="pace-bar"><div class="pace-fill" style="width:48%;background:var(--arlo-amber);"></div></div>
      <div class="pace-stly">51%</div>
      <div class="pace-pill" style="background:var(--arlo-amber2);color:var(--arlo-amber);">Watch</div>
    </div>
    <div class="pace-row">
      <div class="pace-per">+ 3 weeks</div><div class="pace-onb">31%</div>
      <div class="pace-bar"><div class="pace-fill" style="width:31%;background:var(--arlo-green);"></div></div>
      <div class="pace-stly">28%</div>
      <div class="pace-pill" style="background:var(--arlo-green2);color:var(--arlo-green);">On track</div>
    </div>
    <div class="pace-row">
      <div class="pace-per">+ 4 weeks</div><div class="pace-onb">19%</div>
      <div class="pace-bar"><div class="pace-fill" style="width:19%;background:var(--arlo-slate);"></div></div>
      <div class="pace-stly">17%</div>
      <div class="pace-pill" style="background:var(--arlo-slate2);color:var(--arlo-slate);">Early</div>
    </div>
  </div>
</div>
""")

with _dow_col:
    st.markdown(
        f'<div style="padding:0 20px 4px 0;">'
        f'<div style="font-size:9px;font-weight:600;letter-spacing:.16em;text-transform:uppercase;'
        f'color:rgba(245,245,240,0.38);padding:0 0 4px 2px;">Occupancy by day of week</div>'
        f'<div style="font-size:9px;color:rgba(245,245,240,0.35);padding:0 0 4px 2px;">'
        f'Click a bar to filter all KPIs · click again to clear</div></div>',
        unsafe_allow_html=True,
    )
    _dow_bar_colors = []
    for i in range(7):
        if _dow_filter == i:
            _dow_bar_colors.append("#e8854a")          # selected — solid accent
        elif _dow_filter is not None:
            _dow_bar_colors.append("rgba(232,133,74,0.18)")  # dimmed
        else:
            v = dow_vals[i]
            _max = max(dow_vals) or 100.0
            _dow_bar_colors.append(
                "#e8854a" if v / _max > 0.92 else f"rgba(232,133,74,{0.3 + v/_max*0.4:.2f})"
            )

    fig_dow = go.Figure()
    fig_dow.add_trace(go.Bar(
        x=_dow_names, y=dow_vals,
        marker_color=_dow_bar_colors,
        hovertemplate="%{x}: %{y:.1f}%<extra></extra>",
        text=[f"{v:.0f}%" for v in dow_vals],
        textposition="outside",
        textfont=dict(size=9, color="rgba(245,245,240,0.6)"),
    ))
    fig_dow.update_layout(
        paper_bgcolor="#222222", plot_bgcolor="#222222",
        margin=dict(l=4, r=4, t=8, b=4),
        height=160,
        showlegend=False,
        xaxis=dict(tickfont=dict(size=9, color="rgba(245,245,240,0.45)"),
                   showgrid=False, fixedrange=True),
        yaxis=dict(visible=False, fixedrange=True),
        bargap=0.25,
    )

    _dow_event = st.plotly_chart(
        fig_dow,
        on_select="rerun",
        key="dow_bar_chart",
        use_container_width=True,
        config={"displayModeBar": False},
    )
    if _dow_event and _dow_event.selection and _dow_event.selection.points:
        _clicked = _dow_event.selection.points[0]["point_index"]
        _new_filter = None if _clicked == _dow_filter else _clicked
        if _new_filter != _dow_filter:
            st.session_state["db_dow_filter"] = _new_filter
            st.rerun()

# ── Revenue by segment ───────────────────────────────────────────────────────
st.html(f"""
<div style="padding:0 20px;">
<div class="panel">
  <div class="pt">Revenue by segment</div>
  <div class="ps" style="margin-bottom:10px;">Year to date {latest_yr}</div>
  <div class="seg-rows">
    <div class="seg-item">
      <div class="seg-lbl">Transient</div>
      <div class="seg-track"><div class="seg-fill" style="width:52%;background:#4a6fa5;">
        <span class="seg-pct" style="color:rgba(255,255,255,.8);">52%</span></div></div>
      <div class="seg-amt">~$9.1M</div>
    </div>
    <div class="seg-item">
      <div class="seg-lbl">Corporate</div>
      <div class="seg-track"><div class="seg-fill" style="width:22%;background:#3b7a5c;">
        <span class="seg-pct" style="color:rgba(255,255,255,.8);">22%</span></div></div>
      <div class="seg-amt">~$3.9M</div>
    </div>
    <div class="seg-item">
      <div class="seg-lbl">Group</div>
      <div class="seg-track"><div class="seg-fill" style="width:15%;background:#e8854a;">
        <span class="seg-pct" style="color:rgba(255,255,255,.9);">15%</span></div></div>
      <div class="seg-amt">~$2.6M</div>
    </div>
    <div class="seg-item">
      <div class="seg-lbl">Wholesale</div>
      <div class="seg-track"><div class="seg-fill" style="width:7%;background:#555;"></div></div>
      <div class="seg-amt">~$1.2M</div>
    </div>
    <div class="seg-item">
      <div class="seg-lbl">Comp / OP</div>
      <div class="seg-track"><div class="seg-fill" style="width:4%;background:#444;"></div></div>
      <div class="seg-amt">~$0.7M</div>
    </div>
  </div>
</div>
</div>
""")

# ── Competitive set + Medallia ────────────────────────────────────────────────
arlo_adr_disp  = f"${str_adr_val:.0f}"  if str_adr_val  else f"${adr_c:.0f}"
comp_adr_disp  = f"${comp_adr_val:.0f}" if comp_adr_val else "—"
arlo_bar_pct   = 65
comp_max_adr   = max(comp_adr_val or adr_c, str_adr_val or adr_c) * 1.25

mpi_disp = f"{mpi_val:.2f}" if mpi_val else "—"
ari_disp = f"{ari_val:.2f}" if ari_val else "—"
rgi_disp = f"{rgi_val:.2f}" if rgi_val else "—"

# Medallia display
def _med_bar(val, max_val=10.0):
    pct_w = val / max_val * 100 if val else 0
    color = "var(--arlo-green)" if pct_w >= 80 else "var(--arlo-amber)" if pct_w >= 65 else "var(--arlo-red)"
    return f'<div class="rev-bar"><div class="rev-fill" style="width:{pct_w:.0f}%;background:{color};"></div></div>'

med_html = ""
if med_overall is not None:
    star_filled = int(round(med_overall / 2))
    stars = "★" * star_filled + "☆" * (5 - star_filled)
    med_rows = [
        ("Service",    med_rec     if med_rec    else med_overall),
        ("Cleanliness",med_clean   if med_clean  else med_overall),
        ("Value",      med_value   if med_value  else med_overall),
        ("Likelihood", med_return  if med_return else med_overall),
    ]
    med_rows_html = "".join(
        f'<div class="rev-row"><div class="rev-cat">{cat}</div>{_med_bar(v)}<div class="rev-num">{v:.1f}</div></div>'
        for cat, v in med_rows if v is not None
    )
    samples_str = f"{med_samples:,}" if med_samples else "—"
    med_html = f"""
    <div class="rev-top">
      <div>
        <div class="score-n">{med_overall:.1f}</div>
        <div class="score-s">{stars}</div>
        <div class="score-l">/ 10 overall</div>
      </div>
      <div style="width:1px;height:48px;background:var(--arlo-border);"></div>
      <div style="font-size:30px;color:var(--arlo-muted2);line-height:2.1;">
        <div>Positive <span style="color:var(--arlo-green);font-weight:600;">89%</span></div>
        <div>Neutral  <span style="color:var(--arlo-amber);font-weight:600;">7%</span></div>
        <div>Negative <span style="color:var(--arlo-red);font-weight:600;">4%</span></div>
      </div>
    </div>
    <div class="rev-rows2">{med_rows_html}</div>"""
else:
    med_html = '<div style="font-size:30px;color:var(--arlo-muted);padding:8px 0;">No Medallia data available</div>'

samples_label = f"{med_samples:,} responses · year to date" if med_overall is not None else "year to date"

st.html(f"""
<div style="padding:0 20px; margin-bottom:18px;">
<div class="row22">
  <div class="panel">
    <div class="pt">Competitive set — STR</div>
    <div class="ps">Published rate, current date</div>
    <div class="comp-rows">
      <div class="comp-item self">
        <div class="comp-name">Arlo WBG</div>
        <div class="comp-bw"><div class="comp-bf" style="width:{arlo_bar_pct}%;background:var(--arlo-accent);"></div></div>
        <div class="comp-rate">{arlo_adr_disp}</div>
      </div>
      <div class="comp-item">
        <div class="comp-name">1 Hotel BK</div>
        <div class="comp-bw"><div class="comp-bf" style="width:80%;background:#444;"></div></div>
        <div class="comp-rate">$389</div>
      </div>
      <div class="comp-item">
        <div class="comp-name">McCarren</div>
        <div class="comp-bw"><div class="comp-bf" style="width:59%;background:#444;"></div></div>
        <div class="comp-rate">$292</div>
      </div>
      <div class="comp-item">
        <div class="comp-name">William Vale</div>
        <div class="comp-bw"><div class="comp-bf" style="width:73%;background:#444;"></div></div>
        <div class="comp-rate">$358</div>
      </div>
      <div class="comp-item">
        <div class="comp-name">Wythe Hotel</div>
        <div class="comp-bw"><div class="comp-bf" style="width:55%;background:#444;"></div></div>
        <div class="comp-rate">$275</div>
      </div>
      <div class="comp-item">
        <div class="comp-name">Hoxton WBG</div>
        <div class="comp-bw"><div class="comp-bf" style="width:68%;background:#444;"></div></div>
        <div class="comp-rate">$340</div>
      </div>
    </div>
    <div class="comp-idx">
      <div class="ci"><div class="ci-v">{rgi_disp}</div><div class="ci-l">RevPAR Index</div></div>
      <div class="ci"><div class="ci-v">{ari_disp}</div><div class="ci-l">ADR Index</div></div>
      <div class="ci"><div class="ci-v">{mpi_disp}</div><div class="ci-l">Occ. Index</div></div>
    </div>
  </div>

  <div class="panel">
    <div class="pt">Medallia guest reviews</div>
    <div class="ps">{samples_label}</div>
    {med_html}
  </div>
</div>
</div>
""")
