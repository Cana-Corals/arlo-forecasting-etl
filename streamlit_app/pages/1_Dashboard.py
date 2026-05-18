import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.data import load_master, load_predictions
from components.nav import render_nav

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    #MainMenu, footer, header { visibility: hidden; }
    [data-testid="collapsedControl"] { display: none; }
    section[data-testid="stSidebar"] { display: none; }
    .block-container { padding-top: 1.5rem; padding-bottom: 5rem; }

    .arlo-header {
        text-align: center;
        font-size: 1.0rem;
        font-weight: 700;
        color: #1A1A1A;
        letter-spacing: 0.18em;
        margin-bottom: 1.2rem;
    }
    .page-title {
        font-size: 1.6rem;
        font-weight: 700;
        color: #1A1A1A;
        margin-bottom: 0.1rem;
    }
    .page-rule {
        border: none;
        border-top: 2.5px solid #1A1A1A;
        margin: 0 0 1rem 0;
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
</style>
""", unsafe_allow_html=True)

# ── Arlo Williamsburg header ──────────────────────────────────────────────────
st.markdown('<div class="arlo-header">ARLO WILLIAMSBURG</div>', unsafe_allow_html=True)

# ── Top bar: title + user + logout ────────────────────────────────────────────
col_title, _, col_user = st.columns([5, 4, 1])
with col_title:
    st.markdown('<div class="page-title">Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<hr class="page-rule">', unsafe_allow_html=True)
with col_user:
    name = st.session_state.get("name", "")
    st.markdown(f"<div style='text-align:right;font-size:0.8rem;color:#9E9E9E;padding-top:0.3rem;'>{name}</div>", unsafe_allow_html=True)
    if st.button("Logout", key="dash_logout", use_container_width=True):
        for key in ["authentication_status", "name", "username"]:
            st.session_state.pop(key, None)
        st.switch_page("Home.py")

# ── Load data ─────────────────────────────────────────────────────────────────
master   = load_master()
pred     = load_predictions()
latest   = master["business_date"].max()
earliest = master["business_date"].min()

# ── Read filter state (set by widgets at bottom on previous rerun) ─────────────
if "dash_slider" not in st.session_state:
    st.session_state["dash_slider"] = (
        max(latest - pd.DateOffset(years=1), earliest).date(),
        latest.date(),
    )

date_range = st.session_state["dash_slider"]
start_ts   = pd.Timestamp(date_range[0])
end_ts     = pd.Timestamp(date_range[1])

curr       = master[(master["business_date"] >= start_ts) & (master["business_date"] <= end_ts)]
prev       = master[(master["business_date"] >= start_ts - pd.DateOffset(years=1)) &
                    (master["business_date"] <= end_ts   - pd.DateOffset(years=1))]
chart_data = pred[(pred["business_date"] >= start_ts) & (pred["business_date"] <= end_ts)]

# ── Nav bar ───────────────────────────────────────────────────────────────────
render_nav()

# ── KPI helpers ───────────────────────────────────────────────────────────────
def pct(a, b):
    return (a - b) / abs(b) * 100 if b else 0.0

def kpi_card(label, value, delta, fmt="$"):
    val_str = f"${value:,.0f}" if fmt == "$" else f"{value:.1f}%"
    sign    = "+" if delta >= 0 else ""
    arrow   = "▲" if delta >= 0 else "▼"
    cls     = "kpi-up" if delta >= 0 else "kpi-down"
    st.markdown(f"""
    <div class="kpi-box">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{val_str}</div>
        <div class="{cls}">{arrow} {sign}{delta:.1f}%</div>
        <div class="kpi-vs">vs same period last year</div>
    </div>
    """, unsafe_allow_html=True)

# ── KPI cards ─────────────────────────────────────────────────────────────────
rev_c = curr["total_revenue"].sum()
rev_p = prev["total_revenue"].sum()
occ_c = curr["occupancy_rate"].mean() * 100
occ_p = prev["occupancy_rate"].mean() * 100
adr_c = curr["adr"].mean()
adr_p = prev["adr"].mean()
rp_c  = curr["revpar"].mean()
rp_p  = prev["revpar"].mean()

st.markdown("<br>", unsafe_allow_html=True)
c1, c2, c3, c4 = st.columns(4)
with c1: kpi_card("Revenue",   rev_c, pct(rev_c, rev_p), "$")
with c2: kpi_card("Occupancy", occ_c, pct(occ_c, occ_p), "%")
with c3: kpi_card("ADR",       adr_c, pct(adr_c, adr_p), "$")
with c4: kpi_card("RevPAR",    rp_c,  pct(rp_c,  rp_p),  "$")

st.markdown("<br>", unsafe_allow_html=True)

# ── Chart controls ────────────────────────────────────────────────────────────
col_label, col_actual, col_forecast = st.columns([5, 1, 1])
with col_label:
    st.markdown("**Forecast vs Actual — Revenue**")
with col_actual:
    show_actual   = st.checkbox("Actual",   value=True, key="show_actual")
with col_forecast:
    show_forecast = st.checkbox("Forecast", value=True, key="show_forecast")

# ── Chart ─────────────────────────────────────────────────────────────────────
if not show_actual and not show_forecast:
    st.info("Select at least one line to display.")
else:
    fig = go.Figure()

    if show_actual:
        fig.add_trace(go.Scatter(
            x=chart_data["business_date"],
            y=chart_data["actual_revenue"],
            name="Actual",
            line=dict(color="#1A1A1A", width=2),
            hovertemplate="<b>Actual</b>: $%{y:,.0f}<extra></extra>",
        ))

    if show_forecast:
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
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

# ── Date filter (below chart) ─────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)

_presets = {
    "W": pd.Timedelta(days=7),
    "M": pd.DateOffset(months=1),
    "Q": pd.DateOffset(months=3),
    "Y": pd.DateOffset(years=1),
}

avail_years = sorted(master["business_date"].dt.year.unique().tolist())

# Row 1 — duration presets (left) + year presets (right)
n_years  = len(avail_years)
row1_cols = st.columns([0.5, 0.5, 0.5, 0.5, 2] + [0.8] * n_years)

for col, label in zip(row1_cols[:4], _presets):
    with col:
        if st.button(label, key=f"btn_{label}", use_container_width=True):
            new_start = max(latest - _presets[label], earliest).date()
            st.session_state["dash_slider"] = (new_start, latest.date())
            st.rerun()

for col, yr in zip(row1_cols[5:], avail_years):
    with col:
        if st.button(str(yr), key=f"btn_yr_{yr}", use_container_width=True):
            import datetime as dt
            st.session_state["dash_slider"] = (
                max(dt.date(yr, 1, 1), earliest.date()),
                min(dt.date(yr, 12, 31), latest.date()),
            )
            st.rerun()

# Row 2 — shift-left arrow | slider | shift-right arrow
col_prev, col_slide, col_next = st.columns([0.4, 9, 0.4])

with col_prev:
    if st.button("◀", key="btn_shift_left", use_container_width=True):
        s, e = st.session_state["dash_slider"]
        span = pd.Timestamp(e) - pd.Timestamp(s)
        new_s = max(pd.Timestamp(s) - span, pd.Timestamp(earliest)).date()
        new_e = (pd.Timestamp(new_s) + span).date()
        st.session_state["dash_slider"] = (new_s, new_e)
        st.rerun()

with col_next:
    if st.button("▶", key="btn_shift_right", use_container_width=True):
        s, e = st.session_state["dash_slider"]
        span = pd.Timestamp(e) - pd.Timestamp(s)
        new_e = min(pd.Timestamp(e) + span, pd.Timestamp(latest)).date()
        new_s = (pd.Timestamp(new_e) - span).date()
        st.session_state["dash_slider"] = (new_s, new_e)
        st.rerun()

with col_slide:
    st.slider(
        "",
        min_value=earliest.date(),
        max_value=latest.date(),
        label_visibility="collapsed",
        key="dash_slider",
    )
