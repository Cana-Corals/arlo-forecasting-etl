import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.nav import render_nav

BASE = Path(__file__).resolve().parents[2]

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@2.47.0/tabler-icons.min.css">
<style>
:root {
  --dark:#111111; --dark2:#1a1a1a; --dark3:#222222;
  --border:rgba(255,255,255,0.08); --border2:rgba(255,255,255,0.14);
  --white:#f5f5f0; --muted:rgba(245,245,240,0.38); --muted2:rgba(245,245,240,0.6);
  --accent:#e8854a; --green:#3ecf8e; --red:#e05252; --amber:#d4903a; --slate:#4a6fa5;
}
*, *::before, *::after { font-family:'Inter',system-ui,sans-serif !important; -webkit-font-smoothing:antialiased; box-sizing:border-box; }
#MainMenu, footer, header { visibility:hidden; }
[data-testid="collapsedControl"] { display:none; }
section[data-testid="stSidebar"] { display:none; }
.stApp { background:var(--dark2) !important; }
.block-container { padding:0 !important; max-width:1300px !important; margin:0 auto !important; }
[data-testid="stAppViewContainer"] { background:var(--dark2) !important; }

/* topbar */
.fc-topbar {
  background:var(--dark); border-bottom:1px solid var(--border);
  height:52px; display:flex; align-items:center; padding:0 20px; gap:12px;
}
.fc-title { font-size:14px; font-weight:600; color:var(--white); }
.fc-subtitle { font-size:11px; color:var(--muted); }
.fc-accent-rule { height:2px; background:linear-gradient(90deg,var(--accent) 0%,transparent 60%); }

/* metric cards */
.fc-metric-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; padding:20px 20px 0; }
.fc-card {
  background:var(--dark); border:1px solid var(--border);
  border-radius:6px; padding:16px 18px;
}
.fc-card-label { font-size:9px; font-weight:600; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); margin-bottom:10px; }
.fc-card-row { display:flex; align-items:baseline; gap:8px; margin-bottom:6px; }
.fc-card-model { font-size:22px; font-weight:600; color:var(--white); }
.fc-card-tag {
  font-size:9px; font-weight:600; letter-spacing:.1em; text-transform:uppercase;
  padding:2px 7px; border-radius:3px;
}
.tag-ml { background:rgba(232,133,74,.15); color:var(--accent); border:1px solid rgba(232,133,74,.25); }
.tag-yoy { background:rgba(74,111,165,.15); color:var(--slate); border:1px solid rgba(74,111,165,.25); }
.fc-card-compare { display:flex; gap:16px; margin-top:8px; padding-top:8px; border-top:1px solid var(--border); }
.fc-card-stat { font-size:10px; color:var(--muted2); }
.fc-card-stat span { font-weight:600; color:var(--white); }
.fc-win { color:var(--green) !important; }

/* section wrapper */
.fc-section { padding:20px 20px 0; }
.fc-section-title { font-size:11px; font-weight:600; letter-spacing:.14em; text-transform:uppercase; color:var(--muted); margin-bottom:14px; }

/* heatmap label */
.fc-hm-label { font-size:10px; color:var(--muted); text-align:center; margin-top:-8px; margin-bottom:12px; }
</style>
""", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_predictions():
    p = BASE / "outputs" / "model_predictions_with_events.csv"
    df = pd.read_csv(p, parse_dates=["business_date"])
    return df

@st.cache_data
def load_baseline():
    p = BASE / "outputs" / "model_predictions_baseline.csv"
    df = pd.read_csv(p, parse_dates=["business_date"])
    return df

@st.cache_data
def load_fi():
    p = BASE / "outputs" / "feature_importance_with_events.csv"
    return pd.read_csv(p)

ml  = load_predictions()
yoy = load_baseline()
fi  = load_fi()

ml_test  = ml[ml["split"] == "test"]
yoy_test = yoy[yoy["split"] == "test"]

# ── Metrics ───────────────────────────────────────────────────────────────────
def metrics(df):
    out = {}
    for kpi in ["revenue", "occupancy", "adr"]:
        a = df[f"actual_{kpi}"]
        p = df[f"predicted_{kpi}"]
        mae  = float(np.mean(np.abs(a - p)))
        mape = float(np.mean(np.abs((a - p) / a)) * 100)
        ss_res = float(np.sum((a - p) ** 2))
        ss_tot = float(np.sum((a - a.mean()) ** 2))
        r2   = 1 - ss_res / ss_tot if ss_tot else 0.0
        out[kpi] = {"mae": mae, "mape": mape, "r2": r2}
    return out

ml_m  = metrics(ml_test)
yoy_m = metrics(yoy_test)

# ── Plotly theme ──────────────────────────────────────────────────────────────
PLOT_BG  = "#1a1a1a"
PAPER_BG = "#1a1a1a"
GRID_COL = "rgba(255,255,255,0.06)"
FONT_COL = "rgba(245,245,240,0.55)"
ACCENT   = "#e8854a"
SLATE    = "#4a6fa5"
GREEN    = "#3ecf8e"

def base_layout(title=""):
    return dict(
        paper_bgcolor=PAPER_BG, plot_bgcolor=PLOT_BG,
        font=dict(family="Inter", color=FONT_COL, size=11),
        title=dict(text=title, font=dict(size=12, color="rgba(245,245,240,0.8)"), x=0.0, xanchor="left", pad=dict(l=4)),
        margin=dict(l=8, r=8, t=36, b=8),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(255,255,255,0.1)", borderwidth=1,
                    font=dict(size=10, color=FONT_COL), orientation="h", x=0, y=1.12),
        xaxis=dict(gridcolor=GRID_COL, linecolor="rgba(255,255,255,0.1)", tickfont=dict(size=10)),
        yaxis=dict(gridcolor=GRID_COL, linecolor="rgba(255,255,255,0.1)", tickfont=dict(size=10)),
        hovermode="x unified",
    )

# ── Topbar ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="fc-topbar">
  <span class="fc-title">Forecast</span>
  <span class="fc-subtitle">LightGBM · Nov – Dec 2025 holdout</span>
</div>
<div class="fc-accent-rule"></div>
""", unsafe_allow_html=True)

# ── Model accuracy cards ──────────────────────────────────────────────────────
def win_class(ml_val, yoy_val, lower_is_better=True):
    better = ml_val < yoy_val if lower_is_better else ml_val > yoy_val
    return "fc-win" if better else ""

st.markdown('<div class="fc-metric-grid">', unsafe_allow_html=True)

_cards = [
    ("Revenue",   "revenue",   "R²",  "r2",   False, lambda v: f"{v:.3f}",  lambda v: f"${v:,.0f}"),
    ("Occupancy", "occupancy", "R²",  "r2",   False, lambda v: f"{v:.3f}",  lambda v: f"{v*100:.1f}pp"),
    ("ADR",       "adr",       "R²",  "r2",   False, lambda v: f"{v:.3f}",  lambda v: f"${v:.2f}"),
]
for label, kpi, _, metric_key, lower, fmt_r2, fmt_mae in _cards:
    ml_r2   = ml_m[kpi]["r2"]
    yoy_r2  = yoy_m[kpi]["r2"]
    ml_mape = ml_m[kpi]["mape"]
    yoy_mape= yoy_m[kpi]["mape"]
    ml_mae  = ml_m[kpi]["mae"]
    yoy_mae = yoy_m[kpi]["mae"]
    mape_cls = win_class(ml_mape, yoy_mape, lower_is_better=True)
    mae_cls  = win_class(ml_mae,  yoy_mae,  lower_is_better=True)
    r2_cls   = win_class(ml_r2,   yoy_r2,   lower_is_better=False)
    st.markdown(f"""
    <div class="fc-card">
      <div class="fc-card-label">{label}</div>
      <div class="fc-card-row">
        <span class="fc-card-model {r2_cls}">R² {ml_r2:.3f}</span>
        <span class="fc-card-tag tag-ml">ML</span>
      </div>
      <div class="fc-card-compare">
        <div class="fc-card-stat">MAPE <span class="{mape_cls}">{ml_mape:.1f}%</span> vs <span>{yoy_mape:.1f}%</span> YoY</div>
        <div class="fc-card-stat">MAE <span class="{mae_cls}">{fmt_mae(ml_mae)}</span> vs <span>{fmt_mae(yoy_mae)}</span> YoY</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── Forecast chart ────────────────────────────────────────────────────────────
st.markdown('<div class="fc-section">', unsafe_allow_html=True)
st.markdown('<div class="fc-section-title">Forecast vs Actual — Test Period (Nov – Dec 2025)</div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

kpi_tab_labels = ["Revenue", "Occupancy", "ADR"]
kpi_keys       = ["revenue", "occupancy", "adr"]
kpi_fmts       = [
    lambda v: f"${v:,.0f}",
    lambda v: f"{v*100:.1f}%",
    lambda v: f"${v:.2f}",
]
kpi_ytitles = ["Revenue ($)", "Occupancy Rate", "ADR ($)"]

tab1, tab2, tab3 = st.tabs(kpi_tab_labels)

test_start = ml_test["business_date"].min()
test_end   = ml_test["business_date"].max()

for tab, kpi, fmt, ytitle in zip([tab1, tab2, tab3], kpi_keys, kpi_fmts, kpi_ytitles):
    with tab:
        fig = go.Figure()

        # Shaded test region
        fig.add_vrect(
            x0=test_start, x1=test_end,
            fillcolor="rgba(232,133,74,0.06)",
            layer="below", line_width=0,
            annotation_text="Test window",
            annotation_position="top left",
            annotation_font=dict(size=9, color=ACCENT),
        )

        # Full actual line
        fig.add_trace(go.Scatter(
            x=ml["business_date"], y=ml[f"actual_{kpi}"],
            name="Actual", line=dict(color="rgba(245,245,240,0.7)", width=1.5),
            hovertemplate="%{y:.3g}<extra>Actual</extra>",
        ))

        # ML forecast — test only
        fig.add_trace(go.Scatter(
            x=ml_test["business_date"], y=ml_test[f"predicted_{kpi}"],
            name="ML Forecast", line=dict(color=ACCENT, width=2),
            hovertemplate="%{y:.3g}<extra>ML Forecast</extra>",
        ))

        # YoY baseline — test only
        fig.add_trace(go.Scatter(
            x=yoy_test["business_date"], y=yoy_test[f"predicted_{kpi}"],
            name="YoY Baseline", line=dict(color=SLATE, width=1.5, dash="dot"),
            hovertemplate="%{y:.3g}<extra>YoY Baseline</extra>",
        ))

        layout = base_layout()
        layout["yaxis"]["title"] = ytitle
        layout["height"] = 300
        fig.update_layout(**layout)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

# ── Feature importance ────────────────────────────────────────────────────────
st.markdown('<div class="fc-section">', unsafe_allow_html=True)
st.markdown('<div class="fc-section-title">Feature Importance — Top 15 Drivers</div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

_fi_sel = st.segmented_control(
    "Model", ["Revenue", "Occupancy", "ADR"],
    default="Revenue", key="fi_sel", label_visibility="collapsed"
)
_fi_col = {"Revenue": "revenue", "Occupancy": "occupancy", "ADR": "adr"}[_fi_sel]

top15 = fi.nlargest(15, _fi_col)[["feature", _fi_col]].copy()
top15["pct"] = top15[_fi_col] / top15[_fi_col].sum() * 100
top15 = top15.sort_values("pct")

# Clean up feature names for display
_name_map = {
    "avg_booked_rate": "Avg Booked Rate", "total_rooms_on_books": "Rooms on Books",
    "adr_lag_7d": "ADR Lag 7d", "adr_lag_14d": "ADR Lag 14d",
    "adr_lag_28d": "ADR Lag 28d", "adr_lag_364d": "ADR Lag 364d (YoY)",
    "rev_lag_7d": "Revenue Lag 7d", "rev_lag_14d": "Revenue Lag 14d",
    "rev_lag_28d": "Revenue Lag 28d", "rev_lag_364d": "Revenue Lag 364d (YoY)",
    "occ_lag_7d": "Occupancy Lag 7d", "occ_lag_364d": "Occupancy Lag 364d (YoY)",
    "rev_roll_7d": "Revenue Rolling 7d", "rev_roll_28d": "Revenue Rolling 28d",
    "adr_roll_7d": "ADR Rolling 7d", "adr_roll_28d": "ADR Rolling 28d",
    "occ_roll_7d": "Occ Rolling 7d", "occ_roll_28d": "Occ Rolling 28d",
    "pickup_7d": "Pickup 7d", "pickup_14d": "Pickup 14d",
    "pickup_21d": "Pickup 21d", "pickup_30d": "Pickup 30d", "pickup_60d": "Pickup 60d",
    "ooo_rooms": "OOO Rooms", "available_rooms": "Available Rooms",
    "day_of_week": "Day of Week", "week_of_year": "Week of Year",
    "is_weekend": "Weekend", "is_holiday": "Federal Holiday",
    "days_to_next_event": "Days to Next Event", "days_since_last_holiday": "Days Since Holiday",
    "comp_rate_gap": "Competitor Rate Gap",
    "temperature_max": "Max Temperature", "precipitation": "Precipitation",
    "snowfall": "Snowfall", "wind_speed_max": "Max Wind Speed",
}
top15["label"] = top15["feature"].map(lambda x: _name_map.get(x, x.replace("_", " ").title()))

fig_fi = go.Figure(go.Bar(
    x=top15["pct"], y=top15["label"],
    orientation="h",
    marker=dict(
        color=top15["pct"],
        colorscale=[[0, "rgba(232,133,74,0.3)"], [1, ACCENT]],
        showscale=False,
    ),
    text=top15["pct"].map(lambda v: f"{v:.1f}%"),
    textposition="outside",
    textfont=dict(size=10, color=FONT_COL),
    hovertemplate="%{y}: %{x:.1f}%<extra></extra>",
))

fi_layout = base_layout()
fi_layout.update({"height": 420, "margin": dict(l=8, r=60, t=10, b=8)})
fi_layout["xaxis"].update({"title": "Relative Importance (%)", "showgrid": True})
fig_fi.update_layout(**fi_layout)
st.plotly_chart(fig_fi, use_container_width=True, config={"displayModeBar": False})

# ── Seasonal heatmap ──────────────────────────────────────────────────────────
st.markdown('<div class="fc-section">', unsafe_allow_html=True)
st.markdown('<div class="fc-section-title">Seasonal Demand Heatmap</div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

_hm_sel = st.segmented_control(
    "Metric", ["Occupancy", "Revenue", "ADR"],
    default="Occupancy", key="hm_sel", label_visibility="collapsed"
)

ml["week"] = ml["business_date"].dt.isocalendar().week.astype(int)
ml["dow"]  = ml["business_date"].dt.dayofweek
ml["year"] = ml["business_date"].dt.year

_hm_col = {"Occupancy": "actual_occupancy", "Revenue": "actual_revenue", "ADR": "actual_adr"}[_hm_sel]
_hm_fmt = {"Occupancy": ".0%", "Revenue": "$,.0f", "ADR": "$,.0f"}[_hm_sel]

pivot = ml.pivot_table(index="dow", columns="week", values=_hm_col, aggfunc="mean")
dow_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

colorscale = [
    [0.0, "#111111"], [0.3, "rgba(74,111,165,0.6)"],
    [0.6, "rgba(232,133,74,0.7)"], [1.0, "#e8854a"]
]

fig_hm = go.Figure(go.Heatmap(
    z=pivot.values,
    x=[f"W{w}" for w in pivot.columns],
    y=[dow_labels[i] for i in pivot.index],
    colorscale=colorscale,
    showscale=True,
    colorbar=dict(thickness=10, tickfont=dict(size=9, color=FONT_COL), outlinewidth=0),
    hovertemplate="Week %{x} · %{y}<br>%{z:.3g}<extra></extra>",
    xgap=1, ygap=1,
))

hm_layout = base_layout()
hm_layout.update({"height": 220, "margin": dict(l=8, r=8, t=10, b=30)})
hm_layout["xaxis"].update({"tickangle": -45, "tickfont": dict(size=8), "showgrid": False, "nticks": 26})
hm_layout["yaxis"].update({"showgrid": False})
fig_hm.update_layout(**hm_layout)
st.plotly_chart(fig_hm, use_container_width=True, config={"displayModeBar": False})

st.markdown('<div class="fc-hm-label">2024 – 2025 daily actuals averaged by week × day of week</div>', unsafe_allow_html=True)

# ── Bottom nav ────────────────────────────────────────────────────────────────
st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
render_nav()
