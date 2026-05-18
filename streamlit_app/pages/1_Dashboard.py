import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.data import load_master, load_predictions

# ── Page config — must be first ──────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard · Arlo Forecasting",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Auth guard ───────────────────────────────────────────────────────────────
if not st.session_state.get("authentication_status"):
    st.switch_page("Home.py")

# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    #MainMenu, footer, header { visibility: hidden; }
    [data-testid="collapsedControl"] { display: none; }
    section[data-testid="stSidebar"] { display: none; }
    .block-container { padding-top: 2rem; padding-bottom: 5rem; }

    .page-title {
        font-size: 1.6rem;
        font-weight: 700;
        color: #1A1A1A;
        margin-bottom: 0.1rem;
    }
    .page-rule {
        border: none;
        border-top: 2.5px solid #1A1A1A;
        margin: 0 0 1.5rem 0;
        width: 50px;
    }
    .kpi-box {
        background: #F7F5F2;
        padding: 1.2rem 1.4rem;
        border-radius: 8px;
    }
    .kpi-label {
        font-size: 0.72rem;
        color: #9E9E9E;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 0.3rem;
    }
    .kpi-value {
        font-size: 1.7rem;
        font-weight: 700;
        color: #1A1A1A;
        margin-bottom: 0.2rem;
    }
    .kpi-up   { font-size: 0.82rem; color: #2E7D32; }
    .kpi-down { font-size: 0.82rem; color: #C62828; }
    .kpi-vs   { font-size: 0.72rem; color: #BDBDBD; margin-top: 0.1rem; }

    .nav-bar {
        position: fixed;
        bottom: 0; left: 0; right: 0;
        background: #FFFFFF;
        border-top: 1px solid #E0E0E0;
        padding: 0.75rem 2rem;
        display: flex;
        justify-content: center;
        gap: 2.5rem;
        z-index: 999;
    }
    .nav-item {
        font-size: 0.85rem;
        color: #757575;
        text-decoration: none;
        letter-spacing: 0.05em;
    }
    .nav-item:hover { color: #C8522A; }
    .nav-active {
        font-size: 0.85rem;
        color: #C8522A;
        font-weight: 600;
        letter-spacing: 0.05em;
    }
</style>
""", unsafe_allow_html=True)

# ── Header + Period toggle ────────────────────────────────────────────────────
col_title, _, col_toggle = st.columns([4, 2, 2])
with col_title:
    st.markdown('<div class="page-title">Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<hr class="page-rule">', unsafe_allow_html=True)
with col_toggle:
    period = st.radio(
        "", ["W", "M", "Q", "Y"],
        horizontal=True,
        index=3,
        label_visibility="collapsed",
        key="dash_period",
    )

# ── Load data ─────────────────────────────────────────────────────────────────
master = load_master()
pred   = load_predictions()
latest = master["business_date"].max()

# ── Period slices ─────────────────────────────────────────────────────────────
if period == "W":
    curr       = master[master["business_date"] > latest - pd.Timedelta(days=7)]
    prev       = master[
        (master["business_date"] > latest - pd.Timedelta(days=7) - pd.DateOffset(years=1)) &
        (master["business_date"] <= latest - pd.DateOffset(years=1))
    ]
    chart_data = pred[pred["business_date"] > latest - pd.Timedelta(days=56)]

elif period == "M":
    curr       = master[(master["business_date"].dt.month == latest.month) &
                        (master["business_date"].dt.year  == latest.year)]
    prev       = master[(master["business_date"].dt.month == latest.month) &
                        (master["business_date"].dt.year  == latest.year - 1)]
    chart_data = pred[pred["business_date"] > latest - pd.DateOffset(months=12)]

elif period == "Q":
    curr       = master[(master["business_date"].dt.quarter == latest.quarter) &
                        (master["business_date"].dt.year    == latest.year)]
    prev       = master[(master["business_date"].dt.quarter == latest.quarter) &
                        (master["business_date"].dt.year    == latest.year - 1)]
    chart_data = pred[pred["business_date"].dt.year == latest.year]

else:  # Y
    curr       = master[master["business_date"].dt.year == latest.year]
    prev       = master[master["business_date"].dt.year == latest.year - 1]
    chart_data = pred.copy()

# ── KPI helpers ───────────────────────────────────────────────────────────────
def pct(a, b):
    return (a - b) / abs(b) * 100 if b else 0.0

def kpi_card(label, value, delta, fmt="$"):
    if fmt == "$":
        val_str = f"${value:,.0f}" if value >= 1000 else f"${value:,.2f}"
    else:
        val_str = f"{value:.1f}%"
    sign  = "+" if delta >= 0 else ""
    arrow = "▲" if delta >= 0 else "▼"
    cls   = "kpi-up" if delta >= 0 else "kpi-down"
    st.markdown(f"""
    <div class="kpi-box">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{val_str}</div>
        <div class="{cls}">{arrow} {sign}{delta:.1f}%</div>
        <div class="kpi-vs">vs same period last year</div>
    </div>
    """, unsafe_allow_html=True)

# ── KPI values ────────────────────────────────────────────────────────────────
rev_c  = curr["total_revenue"].sum()
rev_p  = prev["total_revenue"].sum()
occ_c  = curr["occupancy_rate"].mean() * 100
occ_p  = prev["occupancy_rate"].mean() * 100
adr_c  = curr["adr"].mean()
adr_p  = prev["adr"].mean()
rp_c   = curr["revpar"].mean()
rp_p   = prev["revpar"].mean()

# ── KPI Cards ─────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1: kpi_card("Revenue",   rev_c, pct(rev_c, rev_p), "$")
with c2: kpi_card("Occupancy", occ_c, pct(occ_c, occ_p), "%")
with c3: kpi_card("ADR",       adr_c, pct(adr_c, adr_p), "$")
with c4: kpi_card("RevPAR",    rp_c,  pct(rp_c,  rp_p),  "$")

st.markdown("<br>", unsafe_allow_html=True)

# ── Forecast vs Actual chart ──────────────────────────────────────────────────
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=chart_data["business_date"],
    y=chart_data["actual_revenue"],
    name="Actual",
    line=dict(color="#1A1A1A", width=2),
    hovertemplate="<b>Actual</b>: $%{y:,.0f}<extra></extra>",
))
fig.add_trace(go.Scatter(
    x=chart_data["business_date"],
    y=chart_data["predicted_revenue"],
    name="Forecast",
    line=dict(color="#C8522A", width=2, dash="dash"),
    hovertemplate="<b>Forecast</b>: $%{y:,.0f}<extra></extra>",
))

fig.update_layout(
    plot_bgcolor="#FFFFFF",
    paper_bgcolor="#FFFFFF",
    margin=dict(t=10, b=10, l=0, r=0),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    xaxis=dict(showgrid=False, zeroline=False),
    yaxis=dict(
        showgrid=True, gridcolor="#F0F0F0",
        zeroline=False, tickprefix="$", tickformat=",.0f",
    ),
    hovermode="x unified",
    height=340,
)

st.markdown("**Forecast vs Actual — Revenue**")
st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

# ── Bottom nav ────────────────────────────────────────────────────────────────
st.markdown("""
<div class="nav-bar">
    <a class="nav-item" href="/">Home</a>
    <span class="nav-active">Dashboard</span>
    <a class="nav-item" href="/Forecast">Forecast</a>
    <a class="nav-item" href="/Performance">Performance</a>
    <a class="nav-item" href="/Demand">Demand</a>
    <a class="nav-item" href="/Competitive">Competitive</a>
    <a class="nav-item" href="/Model_Insights">Model Insights</a>
</div>
""", unsafe_allow_html=True)
