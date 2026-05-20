import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import date, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.sidebar import render_sidebar
from components.forecast_engine import generate_future_predictions

BASE = Path(__file__).resolve().parents[2]
TODAY = date(2026, 5, 19)   # reference date (last data point is Dec 31, 2025)

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
/* sidebar rendered by render_sidebar() */
.stApp { background:var(--dark2) !important; }
.block-container { padding:0 !important; max-width:1300px !important; margin:0 auto !important; }
[data-testid="stAppViewContainer"] { background:var(--dark2) !important; }

.fc-topbar {
  background:var(--dark); border-bottom:1px solid var(--border);
  height:52px; display:flex; align-items:center; padding:0 20px; gap:10px;
}
.fc-title { font-size:14px; font-weight:600; color:var(--white); }
.fc-sub   { font-size:11px; color:var(--muted); }
.fc-rule  { height:2px; background:linear-gradient(90deg,var(--accent) 0%,transparent 60%); }
.fc-home-btn {
  margin-left:auto; font-size:11px; color:rgba(245,245,240,.45);
  text-decoration:none; padding:5px 11px;
  border:1px solid rgba(255,255,255,.1); border-radius:4px;
  transition:color .15s,border-color .15s;
}
.fc-home-btn:hover { color:rgba(245,245,240,.9); border-color:rgba(255,255,255,.25); }

.fc-kpi-grid {
  display:grid; grid-template-columns:repeat(4,1fr); gap:12px;
  padding:18px 20px 6px;
}
.fc-kpi {
  background:var(--dark); border:1px solid var(--border);
  border-radius:6px; padding:14px 16px;
}
.fc-kpi-lbl { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:6px; }
.fc-kpi-val { font-size:22px; font-weight:600; color:var(--white); line-height:1; margin-bottom:4px; }
.fc-kpi-row { display:flex; align-items:center; gap:8px; margin-top:6px; padding-top:6px; border-top:1px solid var(--border); }
.fc-kpi-py  { font-size:10px; color:var(--muted2); }
.fc-kpi-py span { font-weight:500; }
.up   { font-size:10px; font-weight:600; color:var(--green); }
.down { font-size:10px; font-weight:600; color:var(--red); }

.fc-note {
  font-size:10px; color:var(--muted); padding:4px 20px 12px;
  display:flex; align-items:center; gap:6px;
}
.fc-section { padding:16px 20px 0; }
.fc-section-ttl { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }
</style>
""", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Generating 2026 forecast…")
def get_forecast():
    return generate_future_predictions(2026)

@st.cache_data
def load_actuals():
    return pd.read_csv(
        BASE / "outputs" / "model_predictions_with_events.csv",
        parse_dates=["business_date"],
    )

@st.cache_data
def load_rt():
    return pd.read_csv(BASE / "data" / "processed" / "daily_stats_room_type.csv",
                       parse_dates=["business_date"])

@st.cache_data
def load_src():
    return pd.read_csv(BASE / "data" / "processed" / "daily_stats_source.csv",
                       parse_dates=["business_date"])

render_sidebar(active="forecast")
forecast  = get_forecast()   # 2026 ML predictions
actuals25 = load_actuals()   # 2025 true actuals
rt_df     = load_rt()
src_df    = load_src()

# ── Topbar ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="fc-topbar">
  <span class="fc-title">Forecast</span>
  <span class="fc-sub">ML model · 2026 forward predictions</span>
  <a href="/" target="_self" class="fc-home-btn">⌂ Home</a>
</div>
<div class="fc-rule"></div>
""", unsafe_allow_html=True)

# ── Horizon selector ──────────────────────────────────────────────────────────
HORIZONS = ["Next 7d", "Next 30d", "Q1", "Q2", "Q3", "Q4", "Full Year", "Custom"]

if "fc_hz" not in st.session_state:
    st.session_state["fc_hz"] = "Full Year"

hz = st.segmented_control(
    "Horizon", HORIZONS,
    default=st.session_state["fc_hz"],
    key="fc_hz_ctrl",
    label_visibility="collapsed",
)
if hz:
    st.session_state["fc_hz"] = hz

horizon = st.session_state["fc_hz"]

def preset_dates(h):
    if h == "Next 7d":   return TODAY,                      TODAY + timedelta(days=6)
    if h == "Next 30d":  return TODAY,                      TODAY + timedelta(days=29)
    if h == "Q1":        return date(2026, 1, 1),           date(2026, 3, 31)
    if h == "Q2":        return date(2026, 4, 1),           date(2026, 6, 30)
    if h == "Q3":        return date(2026, 7, 1),           date(2026, 9, 30)
    if h == "Q4":        return date(2026, 10, 1),          date(2026, 12, 31)
    if h == "Full Year": return date(2026, 1, 1),           date(2026, 12, 31)
    return None, None

if horizon == "Custom":
    _c1, _c2, _ = st.columns([2, 2, 5])
    with _c1:
        start = st.date_input("From", value=date(2026, 7, 1),
                              min_value=date(2024, 1, 1), max_value=date(2026, 12, 31),
                              key="fc_start", format="MM/DD/YYYY",
                              label_visibility="collapsed")
    with _c2:
        end = st.date_input("To", value=date(2026, 9, 30),
                            min_value=date(2024, 1, 1), max_value=date(2026, 12, 31),
                            key="fc_end", format="MM/DD/YYYY",
                            label_visibility="collapsed")
    if isinstance(start, date) and isinstance(end, date) and start > end:
        start, end = end, start
else:
    start, end = preset_dates(horizon)

s_ts, e_ts = pd.Timestamp(start), pd.Timestamp(end)

# ── Slice forecast and prior-year actuals ─────────────────────────────────────
# For 2026 dates: use ML forecast. For 2024-2025 dates: use historical actuals.
all_actuals = load_actuals()

if start.year >= 2026:
    fc = forecast[(forecast["business_date"] >= s_ts) & (forecast["business_date"] <= e_ts)].copy()
    fc_rev_col, fc_occ_col, fc_adr_col, fc_rp_col = "pred_revenue", "pred_occupancy", "pred_adr", "pred_revpar"
    fc_bar_label = "2026 Forecast"
else:
    fc = all_actuals[(all_actuals["business_date"] >= s_ts) & (all_actuals["business_date"] <= e_ts)].copy()
    fc["pred_revenue"]   = fc["actual_revenue"]
    fc["pred_occupancy"] = fc["actual_occupancy"]
    fc["pred_adr"]       = fc["actual_adr"]
    fc["pred_revpar"]    = fc["actual_revenue"] / 147
    fc_bar_label = f"{start.year} Actual"

py_year = start.year - 1
try:
    py_start = pd.Timestamp(start.replace(year=py_year))
    py_end   = pd.Timestamp(end.replace(year=py_year))
except ValueError:
    py_start = pd.Timestamp(start.replace(year=py_year, day=28))
    py_end   = pd.Timestamp(end.replace(year=py_year, day=28))

py = all_actuals[(all_actuals["business_date"] >= py_start) & (all_actuals["business_date"] <= py_end)].copy()
py_bar_label = f"{py_year} Actual"

# ── Auto-granularity ──────────────────────────────────────────────────────────
n_days   = (end - start).days + 1
freq     = "D" if n_days <= 14 else ("W" if n_days <= 60 else "ME")
date_fmt = "%b %d" if n_days <= 60 else "%b"

def agg_fc(d, col, fn):
    d = d.copy()
    d["period"] = d["business_date"].dt.to_period("D" if freq == "D" else ("W" if freq == "W" else "M"))
    return d.groupby("period")[col].agg(fn).reset_index()

def period_labels(periods):
    out = []
    for p in periods:
        try:
            out.append(p.start_time.strftime(date_fmt))
        except Exception:
            out.append(str(p))
    return out

# ── KPI cards ─────────────────────────────────────────────────────────────────
if fc.empty:
    rev_fc = occ_fc = adr_fc = rp_fc = 0.0
    rev_py = occ_py = adr_py = rp_py = 0.0
else:
    rev_fc = fc["pred_revenue"].sum()
    occ_fc = fc["pred_occupancy"].mean() * 100
    adr_fc = fc["pred_adr"].mean()
    rp_fc  = fc["pred_revpar"].mean()
    rev_py = py["actual_revenue"].sum()    if not py.empty else 0.0
    occ_py = py["actual_occupancy"].mean() * 100 if not py.empty else 0.0
    adr_py = py["actual_adr"].mean()       if not py.empty else 0.0
    rp_py  = (py["actual_revenue"].sum() / 147 / len(py)) if not py.empty else 0.0

def delta(v, base):
    d   = v - base
    pct = (d / abs(base) * 100) if base else 0
    cls = "up" if d >= 0 else "down"
    return cls, f"{'▲' if d>=0 else '▼'} {abs(pct):.1f}%"

rc, rd = delta(rev_fc, rev_py)
oc, od = delta(occ_fc, occ_py)
ac, ad = delta(adr_fc, adr_py)
pc, pd_ = delta(rp_fc, rp_py)

kpi_yr = start.year if start.year >= 2026 else start.year
kpi_tag = "Forecast" if start.year >= 2026 else "Actual"

st.markdown(f"""
<div class="fc-kpi-grid">
  <div class="fc-kpi">
    <div class="fc-kpi-lbl">Revenue {kpi_tag} {kpi_yr}</div>
    <div class="fc-kpi-val">${rev_fc/1e3:,.0f}k</div>
    <div class="fc-kpi-row">
      <span class="{rc}">{rd}</span>
      <span class="fc-kpi-py">vs <span>${rev_py/1e3:,.0f}k</span> in {py_year}</span>
    </div>
  </div>
  <div class="fc-kpi">
    <div class="fc-kpi-lbl">Occupancy {kpi_tag} {kpi_yr}</div>
    <div class="fc-kpi-val">{occ_fc:.1f}%</div>
    <div class="fc-kpi-row">
      <span class="{oc}">{od}</span>
      <span class="fc-kpi-py">vs <span>{occ_py:.1f}%</span> in {py_year}</span>
    </div>
  </div>
  <div class="fc-kpi">
    <div class="fc-kpi-lbl">ADR {kpi_tag} {kpi_yr}</div>
    <div class="fc-kpi-val">${adr_fc:,.0f}</div>
    <div class="fc-kpi-row">
      <span class="{ac}">{ad}</span>
      <span class="fc-kpi-py">vs <span>${adr_py:,.0f}</span> in {py_year}</span>
    </div>
  </div>
  <div class="fc-kpi">
    <div class="fc-kpi-lbl">RevPAR {kpi_tag} {kpi_yr}</div>
    <div class="fc-kpi-val">${rp_fc:,.0f}</div>
    <div class="fc-kpi-row">
      <span class="{pc}">{pd_}</span>
      <span class="fc-kpi-py">vs <span>${rp_py:,.0f}</span> in {py_year}</span>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="fc-note">
  <i class="ti ti-info-circle"></i>
  2026 forecast generated by LightGBM using 2024–2025 historical patterns and YoY growth trend.
  Bars show 2026 ML prediction vs same period 2025 actual.
</div>
""", unsafe_allow_html=True)

# ── Chart helpers ─────────────────────────────────────────────────────────────
BG      = "#1a1a1a"
GRID    = "rgba(255,255,255,0.05)"
FONT    = "rgba(245,245,240,0.5)"
ACCENT  = "#e8854a"
ACCENT_F = "rgba(232,133,74,0.22)"
GREEN   = "#3ecf8e"
GREEN_F = "rgba(62,207,142,0.20)"

def chart_layout(h=280):
    return dict(
        paper_bgcolor=BG, plot_bgcolor=BG,
        font=dict(family="Inter", color=FONT, size=10),
        margin=dict(l=4, r=4, t=28, b=4),
        height=h, bargap=0.18, bargroupgap=0.08,
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0,
                    orientation="h", x=0, y=1.14,
                    font=dict(size=10, color="rgba(245,245,240,0.6)")),
        xaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)",
                   tickfont=dict(size=9), tickangle=-30),
        yaxis=dict(gridcolor=GRID, linecolor="rgba(255,255,255,0.08)",
                   tickfont=dict(size=9), zeroline=False),
        hovermode="x unified",
    )

def build_bars(fc_df, py_df, val_col_fc, val_col_py, agg_fn,
               color, color_faded, prefix="", suffix="",
               fc_label="2026 Forecast", py_label="2025 Actual"):
    freq_key = "D" if freq == "D" else ("W" if freq == "W" else "M")

    # Aggregate forecast
    fc_a = agg_fc(fc_df, val_col_fc, agg_fn)
    xlabels = period_labels(fc_a["period"])

    # Aggregate prior year — sort by period then align positionally
    # (periods differ by year so equality matching always fails)
    py_a = pd.Series(dtype=float)
    if not py_df.empty:
        py2 = py_df.copy()
        py2["period"] = py2["business_date"].dt.to_period(freq_key)
        py_agg = py2.groupby("period")[val_col_py].agg(agg_fn).sort_index()
        py_a = py_agg.reset_index()[val_col_py]

    n = len(xlabels)
    py_vals = list(py_a.values) + [0.0] * n
    py_vals = py_vals[:n]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=xlabels, y=fc_a[val_col_fc],
        name=fc_label, marker_color=color,
        hovertemplate=f"{prefix}%{{y:,.1f}}{suffix}<extra>{fc_label}</extra>",
    ))
    fig.add_trace(go.Bar(
        x=xlabels, y=py_vals,
        name=py_label, marker_color=color_faded,
        marker_line=dict(color=color, width=1),
        hovertemplate=f"{prefix}%{{y:,.1f}}{suffix}<extra>{py_label}</extra>",
    ))
    return fig

if fc.empty:
    st.info("No forecast data available for the selected period.")
else:
    # Bar / Line toggle
    _tc, _ = st.columns([3, 6])
    with _tc:
        use_line = st.radio("Chart style", ["Bar", "Line"], horizontal=True,
                            key="fc_chart_style", label_visibility="collapsed")

    # Revenue + Occupancy side by side
    st.markdown('<div class="fc-section"><div class="fc-section-ttl">Revenue &amp; Occupancy Forecast</div></div>',
                unsafe_allow_html=True)
    _rc, _oc = st.columns(2)

    with _rc:
        if use_line == "Bar":
            fig_rev = build_bars(fc, py, "pred_revenue", "actual_revenue", "sum",
                                 ACCENT, ACCENT_F, prefix="$",
                                 fc_label=fc_bar_label, py_label=py_bar_label)
        else:
            fc_r = agg_fc(fc, "pred_revenue", "sum")
            xl_r  = period_labels(fc_r["period"])
            fig_rev = go.Figure()
            fig_rev.add_trace(go.Scatter(x=xl_r, y=fc_r["pred_revenue"],
                                         name=fc_bar_label, line=dict(color=ACCENT, width=2),
                                         mode="lines+markers"))
            if not py.empty:
                py_r = agg_fc(py.rename(columns={"actual_revenue": "pred_revenue"}),
                              "pred_revenue", "sum")
                fig_rev.add_trace(go.Scatter(x=period_labels(py_r["period"]), y=py_r["pred_revenue"],
                                             name=py_bar_label, line=dict(color=ACCENT_F, width=1.5, dash="dot"),
                                             mode="lines+markers"))
        fig_rev.update_layout(**chart_layout())
        fig_rev.update_yaxes(tickprefix="$", tickformat=",.0f")
        st.plotly_chart(fig_rev, use_container_width=True, config={"displayModeBar": False})

    with _oc:
        fc_occ = fc.copy()
        fc_occ["pred_occ_pct"] = fc_occ["pred_occupancy"] * 100
        py_occ = py.copy()
        py_occ["actual_occ_pct"] = py_occ["actual_occupancy"] * 100

        if use_line == "Bar":
            fig_occ = build_bars(fc_occ, py_occ, "pred_occ_pct", "actual_occ_pct", "mean",
                                 GREEN, GREEN_F, suffix="%",
                                 fc_label=fc_bar_label, py_label=py_bar_label)
        else:
            o_r = agg_fc(fc_occ, "pred_occ_pct", "mean")
            xl_o = period_labels(o_r["period"])
            fig_occ = go.Figure()
            fig_occ.add_trace(go.Scatter(x=xl_o, y=o_r["pred_occ_pct"],
                                          name=fc_bar_label, line=dict(color=GREEN, width=2),
                                          mode="lines+markers"))
            if not py_occ.empty:
                o_py = agg_fc(py_occ.rename(columns={"actual_occ_pct": "pred_occ_pct"}),
                              "pred_occ_pct", "mean")
                fig_occ.add_trace(go.Scatter(x=period_labels(o_py["period"]), y=o_py["pred_occ_pct"],
                                             name=py_bar_label, line=dict(color=GREEN_F, width=1.5, dash="dot"),
                                             mode="lines+markers"))
        fig_occ.update_layout(**chart_layout())
        fig_occ.update_yaxes(ticksuffix="%", range=[0, 105])
        st.plotly_chart(fig_occ, use_container_width=True, config={"displayModeBar": False})

    # ADR + RevPAR forecast side by side
    st.markdown('<div class="fc-section"><div class="fc-section-ttl">ADR &amp; RevPAR Forecast</div></div>',
                unsafe_allow_html=True)
    _ac, _rpc = st.columns(2)

    with _ac:
        adr_r = agg_fc(fc, "pred_adr", "mean")
        xl_a  = period_labels(adr_r["period"])
        fig_adr = go.Figure()
        fig_adr.add_trace(go.Scatter(x=xl_a, y=adr_r["pred_adr"],
                                     name=fc_bar_label, line=dict(color=ACCENT, width=2),
                                     mode="lines+markers",
                                     hovertemplate="$%{y:,.0f}<extra>" + fc_bar_label + "</extra>"))
        if not py.empty:
            adr_py = agg_fc(py.rename(columns={"actual_adr": "pred_adr"}), "pred_adr", "mean")
            fig_adr.add_trace(go.Scatter(x=period_labels(adr_py["period"]), y=adr_py["pred_adr"],
                                         name=py_bar_label, line=dict(color=ACCENT_F, width=1.5, dash="dot"),
                                         mode="lines+markers",
                                         hovertemplate="$%{y:,.0f}<extra>" + py_bar_label + "</extra>"))
        adr_lay = chart_layout()
        adr_lay["title"] = dict(text="ADR Forecast", font=dict(size=11, color="rgba(245,245,240,0.7)"), x=0)
        fig_adr.update_layout(**adr_lay)
        fig_adr.update_yaxes(tickprefix="$", tickformat=",.0f")
        st.plotly_chart(fig_adr, use_container_width=True, config={"displayModeBar": False})

    with _rpc:
        rp_r = agg_fc(fc, "pred_revpar", "mean")
        xl_rp = period_labels(rp_r["period"])
        SLATE = "#4a6fa5"
        SLATE_F = "rgba(74,111,165,0.22)"
        fig_rp = go.Figure()
        fig_rp.add_trace(go.Scatter(x=xl_rp, y=rp_r["pred_revpar"],
                                    name=fc_bar_label, line=dict(color=SLATE, width=2),
                                    mode="lines+markers",
                                    hovertemplate="$%{y:,.0f}<extra>" + fc_bar_label + "</extra>"))
        rp_lay = chart_layout()
        rp_lay["title"] = dict(text="RevPAR Forecast", font=dict(size=11, color="rgba(245,245,240,0.7)"), x=0)
        fig_rp.update_layout(**rp_lay)
        fig_rp.update_yaxes(tickprefix="$", tickformat=",.0f")
        st.plotly_chart(fig_rp, use_container_width=True, config={"displayModeBar": False})

st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
