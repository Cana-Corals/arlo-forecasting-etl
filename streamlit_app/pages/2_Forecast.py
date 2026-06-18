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
.fc-sub   { font-size:33px; color:var(--muted); }
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
.up   { font-size:30px; font-weight:600; color:var(--green); }
.down { font-size:30px; font-weight:600; color:var(--red); }

.fc-note {
  font-size:30px; color:var(--muted); padding:4px 20px 12px;
  display:flex; align-items:center; gap:6px;
}
.fc-section { padding:16px 20px 0; }
.fc-section-ttl { font-size:27px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }
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
        start = st.date_input("From", value=date(2026, 1, 1),
                              min_value=date(2024, 1, 1), max_value=date(2026, 12, 31),
                              key="fc_start", format="MM/DD/YYYY",
                              label_visibility="collapsed")
    with _c2:
        end = st.date_input("To", value=date(2026, 12, 31),
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
        margin=dict(l=4, r=4, t=48, b=4),
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

SLATE   = "#4a6fa5"
SLATE_F = "rgba(74,111,165,0.22)"

if fc.empty:
    st.info("No forecast data available for the selected period.")
else:
    # ── Revenue + Occupancy — each chart has its own type toggle ──────────────
    _rc, _oc = st.columns(2)

    with _rc:
        _rl, _rt = st.columns([3, 1.5])
        with _rl:
            st.markdown('<div class="fc-section"><div class="fc-section-ttl">Revenue Forecast</div></div>',
                        unsafe_allow_html=True)
        with _rt:
            rev_t = st.segmented_control("", ["📊", "📈"], default="📊",
                                          key="fc_rev_t", label_visibility="collapsed")
        is_bar_rev = (rev_t == "📊" or rev_t is None)

        if is_bar_rev:
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
        _ol, _ot = st.columns([3, 1.5])
        with _ol:
            st.markdown('<div class="fc-section"><div class="fc-section-ttl">Occupancy Forecast</div></div>',
                        unsafe_allow_html=True)
        with _ot:
            occ_t = st.segmented_control("", ["📊", "📈"], default="📊",
                                          key="fc_occ_t", label_visibility="collapsed")
        is_bar_occ = (occ_t == "📊" or occ_t is None)

        fc_occ = fc.copy()
        fc_occ["pred_occ_pct"] = fc_occ["pred_occupancy"] * 100
        py_occ = py.copy()
        if not py_occ.empty:
            py_occ["actual_occ_pct"] = py_occ["actual_occupancy"] * 100

        if is_bar_occ:
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

    # ── ADR + RevPAR — each chart has its own type toggle ────────────────────
    _ac, _rpc = st.columns(2)

    with _ac:
        _al, _at = st.columns([3, 1.5])
        with _al:
            st.markdown('<div class="fc-section"><div class="fc-section-ttl">ADR Forecast</div></div>',
                        unsafe_allow_html=True)
        with _at:
            adr_t = st.segmented_control("", ["📊", "📈"], default="📈",
                                          key="fc_adr_t", label_visibility="collapsed")
        is_bar_adr = (adr_t == "📊")

        if is_bar_adr:
            fig_adr = build_bars(fc, py, "pred_adr", "actual_adr", "mean",
                                 ACCENT, ACCENT_F, prefix="$",
                                 fc_label=fc_bar_label, py_label=py_bar_label)
        else:
            adr_r = agg_fc(fc, "pred_adr", "mean")
            xl_a  = period_labels(adr_r["period"])
            fig_adr = go.Figure()
            fig_adr.add_trace(go.Scatter(x=xl_a, y=adr_r["pred_adr"],
                                         name=fc_bar_label, line=dict(color=ACCENT, width=2),
                                         mode="lines+markers",
                                         hovertemplate="$%{y:,.0f}<extra>" + fc_bar_label + "</extra>"))
            if not py.empty:
                adr_py_agg = agg_fc(py.rename(columns={"actual_adr": "pred_adr"}), "pred_adr", "mean")
                fig_adr.add_trace(go.Scatter(x=period_labels(adr_py_agg["period"]), y=adr_py_agg["pred_adr"],
                                             name=py_bar_label, line=dict(color=ACCENT_F, width=1.5, dash="dot"),
                                             mode="lines+markers",
                                             hovertemplate="$%{y:,.0f}<extra>" + py_bar_label + "</extra>"))
        fig_adr.update_layout(**chart_layout())
        fig_adr.update_yaxes(tickprefix="$", tickformat=",.0f")
        st.plotly_chart(fig_adr, use_container_width=True, config={"displayModeBar": False})

    with _rpc:
        _rl2, _rt2 = st.columns([3, 1.5])
        with _rl2:
            st.markdown('<div class="fc-section"><div class="fc-section-ttl">RevPAR Forecast</div></div>',
                        unsafe_allow_html=True)
        with _rt2:
            rp_t = st.segmented_control("", ["📊", "📈"], default="📈",
                                         key="fc_rp_t", label_visibility="collapsed")
        is_bar_rp = (rp_t == "📊")

        if is_bar_rp:
            py_rp_df = py.copy()
            if not py_rp_df.empty:
                py_rp_df["actual_revpar"] = py_rp_df["actual_revenue"] / 147
            fig_rp = build_bars(fc, py_rp_df, "pred_revpar", "actual_revpar", "mean",
                                SLATE, SLATE_F, prefix="$",
                                fc_label=fc_bar_label, py_label=py_bar_label)
        else:
            rp_r = agg_fc(fc, "pred_revpar", "mean")
            xl_rp = period_labels(rp_r["period"])
            fig_rp = go.Figure()
            fig_rp.add_trace(go.Scatter(x=xl_rp, y=rp_r["pred_revpar"],
                                        name=fc_bar_label, line=dict(color=SLATE, width=2),
                                        mode="lines+markers",
                                        hovertemplate="$%{y:,.0f}<extra>" + fc_bar_label + "</extra>"))
            if not py.empty:
                py_for_rp = py.copy()
                py_for_rp["pred_revpar"] = py_for_rp["actual_revenue"] / 147
                rp_py_agg = agg_fc(py_for_rp, "pred_revpar", "mean")
                fig_rp.add_trace(go.Scatter(x=period_labels(rp_py_agg["period"]), y=rp_py_agg["pred_revpar"],
                                            name=py_bar_label, line=dict(color=SLATE_F, width=1.5, dash="dot"),
                                            mode="lines+markers",
                                            hovertemplate="$%{y:,.0f}<extra>" + py_bar_label + "</extra>"))
        fig_rp.update_layout(**chart_layout())
        fig_rp.update_yaxes(tickprefix="$", tickformat=",.0f")
        st.plotly_chart(fig_rp, use_container_width=True, config={"displayModeBar": False})

    # ── Channel Revenue Forecast ──────────────────────────────────────────────
    st.divider()
    st.markdown('<div class="fc-section"><div class="fc-section-ttl">Booking Channel Revenue Forecast</div></div>',
                unsafe_allow_html=True)
    st.markdown(
        '<div style="padding:0 20px 10px;font-size:30px;color:rgba(245,245,240,0.45);">'
        'Projected by applying the ML growth factor to the 2025 channel mix. '
        'Use to anticipate OTA, direct, and corporate contributions for the selected period.</div>',
        unsafe_allow_html=True,
    )

    _CHANNEL_MAP = {
        "EXTRA": "OTA", "EBL": "OTA", "GTA": "OTA", "GLOB": "OTA",
        "WEBSITE": "Direct / Web", "WEBBOOK": "Direct / Web",
        "MOBILE": "Direct / Web", "FD": "Direct / Web",
        "GDSS": "GDS / Corporate", "EXEC": "GDS / Corporate", "SLS": "GDS / Corporate",
        "CALL": "Voice / Other", "PHONE": "Voice / Other", "WHLS": "Voice / Other",
        "INP": "Voice / Other", "EVENTS": "Voice / Other",
    }
    _CH_COLORS = {
        "OTA":             ACCENT,
        "Direct / Web":    GREEN,
        "GDS / Corporate": SLATE,
        "Voice / Other":   "#9b6fd4",
    }

    _src = src_df.copy()
    _src["source_code"] = _src["source_code"].fillna("UNKNOWN")
    _src["channel"] = _src["source_code"].map(_CHANNEL_MAP).fillna("Voice / Other")

    # For 2026 horizons: map to 2025 equivalent and apply ML growth factor
    if start.year >= 2026:
        try:
            _cb_s = pd.Timestamp(start.replace(year=2025))
            _cb_e = pd.Timestamp(end.replace(year=2025))
        except ValueError:
            _cb_s = pd.Timestamp(start.replace(year=2025, day=28))
            _cb_e = pd.Timestamp(end.replace(year=2025, day=28))
        _gf = rev_fc / rev_py if rev_py > 0 else 1.0
        _ch_fc_lbl = "2026 Forecast"
    else:
        _cb_s, _cb_e = s_ts, e_ts
        _gf = 1.0
        _ch_fc_lbl = f"{start.year} Actual"

    # Prior year window for comparison
    try:
        _cb_py_s = pd.Timestamp(_cb_s.date().replace(year=_cb_s.year - 1))
        _cb_py_e = pd.Timestamp(_cb_e.date().replace(year=_cb_e.year - 1))
    except ValueError:
        _cb_py_s = pd.Timestamp(_cb_s.date().replace(year=_cb_s.year - 1, day=28))
        _cb_py_e = pd.Timestamp(_cb_e.date().replace(year=_cb_e.year - 1, day=28))

    _ch_base = _src[(_src["business_date"] >= _cb_s) & (_src["business_date"] <= _cb_e)].copy()
    _ch_pydf = _src[(_src["business_date"] >= _cb_py_s) & (_src["business_date"] <= _cb_py_e)].copy()

    _fk = "D" if freq == "D" else ("W" if freq == "W" else "M")
    _ch_base["period"] = _ch_base["business_date"].dt.to_period(_fk)
    _ch_base_agg = _ch_base.groupby(["period", "channel"])["total_revenue"].sum().reset_index()
    _ch_base_agg["proj_rev"] = _ch_base_agg["total_revenue"] * _gf

    _ch_py_totals = (_ch_pydf.groupby("channel")["total_revenue"].sum()
                     if not _ch_pydf.empty else pd.Series(dtype=float))
    _ch_fc_totals = _ch_base_agg.groupby("channel")["proj_rev"].sum().sort_values(ascending=False)
    _total_proj   = float(_ch_fc_totals.sum()) or 1.0

    _ps_sorted = sorted(_ch_base_agg["period"].unique())
    _pl        = period_labels(_ps_sorted)

    fig_ch = go.Figure()
    for _ch_name, _ch_color in _CH_COLORS.items():
        _d  = _ch_base_agg[_ch_base_agg["channel"] == _ch_name]
        _yv = []
        for _p in _ps_sorted:
            _r = _d[_d["period"] == _p]
            _yv.append(float(_r["proj_rev"].values[0]) if len(_r) else 0.0)
        fig_ch.add_trace(go.Bar(
            x=_pl, y=_yv, name=_ch_name, marker_color=_ch_color,
            hovertemplate=f"{_ch_name}: $%{{y:,.0f}}<extra></extra>",
        ))

    _ch_lay = chart_layout(h=340)
    _ch_lay["barmode"] = "stack"
    _ch_lay["yaxis"]["tickprefix"] = "$"
    _ch_lay["yaxis"]["tickformat"] = ",.0f"
    fig_ch.update_layout(**_ch_lay)

    _cha_col, _chb_col = st.columns([2, 1])
    with _cha_col:
        st.markdown(
            f'<div style="font-size:33px;color:rgba(245,245,240,0.7);padding:4px 0 6px;">'
            f'Revenue by Booking Channel — {_ch_fc_lbl}</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(fig_ch, use_container_width=True, config={"displayModeBar": False})

    with _chb_col:
        _cards_ch = ""
        for _ch_name, _ch_color in _CH_COLORS.items():
            _proj  = float(_ch_fc_totals.get(_ch_name, 0))
            _py_v  = float(_ch_py_totals.get(_ch_name, 0)) if not _ch_py_totals.empty else 0.0
            _share = _proj / _total_proj * 100
            _diff_html = ""
            if _py_v > 0:
                _diff = (_proj - _py_v) / _py_v * 100
                _dc   = "#3ecf8e" if _diff >= 0 else "#e05252"
                _ds   = "▲" if _diff >= 0 else "▼"
                _diff_html = (
                    f'<div style="font-size:9px;color:{_dc};margin-top:2px;">'
                    f'{_ds} {abs(_diff):.1f}% vs prior year</div>'
                )
            _cards_ch += (
                f'<div style="border-left:3px solid {_ch_color};padding:10px 12px;'
                f'margin-bottom:8px;background:rgba(255,255,255,0.02);border-radius:0 4px 4px 0;">'
                f'<div style="font-size:9px;font-weight:600;letter-spacing:.1em;text-transform:uppercase;'
                f'color:{_ch_color};margin-bottom:4px;">{_ch_name}</div>'
                f'<div style="display:flex;align-items:baseline;gap:10px;">'
                f'<span style="font-size:15px;font-weight:600;color:#f5f5f0;">${_proj:,.0f}</span>'
                f'<span style="font-size:9px;color:rgba(245,245,240,0.4);">{_share:.1f}%</span>'
                f'</div>'
                f'{_diff_html}'
                f'</div>'
            )
        st.markdown(f'<div style="padding:28px 0 0;">{_cards_ch}</div>', unsafe_allow_html=True)

st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
