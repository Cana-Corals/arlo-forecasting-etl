import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.sidebar import render_sidebar
from components.auth import require_auth

require_auth()

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

.mi-topbar { background:var(--dark); border-bottom:1px solid var(--border); height:52px; display:flex; align-items:center; padding:0 20px; gap:10px; }
.mi-title  { font-size:14px; font-weight:600; color:var(--white); }
.mi-sub    { font-size:33px; color:var(--muted); }
.mi-rule   { height:2px; background:linear-gradient(90deg,var(--purple) 0%,transparent 60%); }
.mi-home-btn { margin-left:auto; font-size:11px; color:rgba(245,245,240,.45); text-decoration:none; padding:5px 11px; border:1px solid rgba(255,255,255,.1); border-radius:4px; }

.mi-section-hdr {
  font-size:30px; font-weight:600; letter-spacing:.18em; text-transform:uppercase;
  color:rgba(245,245,240,.35); padding:20px 20px 8px;
}

/* ── Model score cards ── */
.mi-cards { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; padding:0 20px 4px; }
.mi-card  { background:var(--dark); border:1px solid var(--border); border-radius:8px; padding:16px 18px; }
.mi-card-name { font-size:9px; font-weight:700; letter-spacing:.18em; text-transform:uppercase; color:var(--muted); margin-bottom:10px; }
.mi-card-r2   { font-size:28px; font-weight:700; color:var(--white); line-height:1; margin-bottom:2px; }
.mi-card-label{ font-size:9px; color:var(--muted); margin-bottom:12px; }
.mi-card-row  { display:flex; gap:14px; border-top:1px solid var(--border); padding-top:10px; margin-top:4px; }
.mi-card-stat { flex:1; }
.mi-card-stat-lbl { font-size:8px; letter-spacing:.1em; text-transform:uppercase; color:var(--muted); }
.mi-card-stat-val { font-size:13px; font-weight:600; color:var(--white); margin-top:2px; }

/* ── Feature importance bar ── */
.fi-row  { display:flex; align-items:center; gap:10px; padding:5px 0; border-bottom:1px solid rgba(255,255,255,.04); }
.fi-name { font-size:30px; color:rgba(245,245,240,.75); min-width:220px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.fi-bar-wrap { flex:1; height:6px; background:rgba(255,255,255,.06); border-radius:3px; }
.fi-bar  { height:6px; border-radius:3px; }
.fi-val  { font-size:9px; color:var(--muted); min-width:42px; text-align:right; }

/* ── Tabs override ── */
[data-baseweb="tab-list"] { background:var(--dark) !important; border-bottom:1px solid var(--border) !important; padding:0 20px !important; gap:4px !important; }
[data-baseweb="tab"] { background:transparent !important; color:rgba(245,245,240,.45) !important; font-size:12px !important; font-weight:500 !important; border:none !important; border-radius:0 !important; padding:10px 14px !important; }
[aria-selected="true"][data-baseweb="tab"] { color:var(--white) !important; border-bottom:2px solid var(--purple) !important; }
[data-baseweb="tab-panel"] { background:transparent !important; padding:0 !important; }
</style>
""", unsafe_allow_html=True)

# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_predictions():
    return pd.read_csv(BASE / "outputs" / "model_predictions_with_events.csv",
                       parse_dates=["business_date"])

@st.cache_data(show_spinner=False)
def load_importance():
    return pd.read_csv(BASE / "outputs" / "feature_importance_with_events.csv")

# ── Metrics ───────────────────────────────────────────────────────────────────
def compute_metrics(df, split="test"):
    sub = df[df["split"] == split]
    results = {}
    for col in ["revenue", "occupancy", "adr", "revpar"]:
        y  = sub[f"actual_{col}"].values
        yh = sub[f"predicted_{col}"].values
        mask = y != 0
        mae  = float(np.mean(np.abs(y - yh)))
        mape = float(np.mean(np.abs((y[mask] - yh[mask]) / y[mask])) * 100)
        ss_res = np.sum((y - yh) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r2   = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0
        rmse = float(np.sqrt(np.mean((y - yh) ** 2)))
        results[col] = dict(mae=mae, mape=mape, r2=r2, rmse=rmse)
    return results

# ── Dark chart theme ──────────────────────────────────────────────────────────
DARK = dict(
    paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
    font=dict(color="rgba(245,245,240,0.7)", family="Inter, system-ui", size=33),
    margin=dict(l=10, r=10, t=36, b=10),
    hoverlabel=dict(bgcolor="#222", bordercolor="rgba(255,255,255,.15)", font_color="#f5f5f0"),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=33)),
)
def _dark(fig):
    fig.update_layout(**DARK)
    fig.update_xaxes(gridcolor="rgba(255,255,255,.06)", linecolor="rgba(255,255,255,.1)", zeroline=False)
    fig.update_yaxes(gridcolor="rgba(255,255,255,.06)", linecolor="rgba(255,255,255,.1)", zeroline=False)
    return fig

ACCENT  = "#e8854a"
GREEN   = "#3ecf8e"
SLATE   = "#4a6fa5"
PURPLE  = "#9b6fd4"
RED     = "#e05252"

MODEL_COLORS = {"revenue": ACCENT, "occupancy": GREEN, "adr": SLATE, "revpar": PURPLE}
MODEL_LABELS = {"revenue": "Revenue", "occupancy": "Occupancy", "adr": "ADR", "revpar": "RevPAR"}
MODEL_UNITS  = {"revenue": "$", "occupancy": "", "adr": "$", "revpar": "$"}
MODEL_FMT    = {
    "revenue":   lambda v: f"${v:,.0f}",
    "occupancy": lambda v: f"{v*100:.1f}%",
    "adr":       lambda v: f"${v:,.0f}",
    "revpar":    lambda v: f"${v:,.0f}",
}

# ── R² color ─────────────────────────────────────────────────────────────────
def r2_color(r2):
    if r2 >= 0.95: return GREEN
    if r2 >= 0.85: return ACCENT
    return RED

# ── Top-bar ───────────────────────────────────────────────────────────────────
render_sidebar(active="model")

st.markdown("""
<div class="mi-topbar">
  <i class="ti ti-brain" style="font-size:18px;color:#9b6fd4;"></i>
  <div>
    <div class="mi-title">Model Insights</div>
    <div class="mi-sub">LightGBM · 4 models · Test set: Nov–Dec 2025 (61 days)</div>
  </div>
</div>
<div class="mi-rule"></div>
""", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
pred = load_predictions()
fi   = load_importance()
metrics = compute_metrics(pred)

# ── Score cards ───────────────────────────────────────────────────────────────
st.markdown('<div class="mi-section-hdr">Test Set Performance — Nov–Dec 2025 (61 days, never seen during training)</div>',
            unsafe_allow_html=True)

cards_html = '<div class="mi-cards">'
for col in ["revenue", "occupancy", "adr", "revpar"]:
    m = metrics[col]
    color = r2_color(m["r2"])
    u = MODEL_UNITS[col]
    mae_fmt  = f"${m['mae']:,.0f}" if u == "$" else f"{m['mae']*100:.2f}pp"
    mape_fmt = f"{m['mape']:.1f}%"
    rmse_fmt = f"${m['rmse']:,.0f}" if u == "$" else f"{m['rmse']*100:.2f}pp"
    cards_html += f"""
<div class="mi-card">
  <div class="mi-card-name">{MODEL_LABELS[col]}</div>
  <div class="mi-card-r2" style="color:{color};">{m['r2']:.4f}</div>
  <div class="mi-card-label">R² score</div>
  <div class="mi-card-row">
    <div class="mi-card-stat">
      <div class="mi-card-stat-lbl">MAPE</div>
      <div class="mi-card-stat-val">{mape_fmt}</div>
    </div>
    <div class="mi-card-stat">
      <div class="mi-card-stat-lbl">MAE</div>
      <div class="mi-card-stat-val">{mae_fmt}</div>
    </div>
    <div class="mi-card-stat">
      <div class="mi-card-stat-lbl">RMSE</div>
      <div class="mi-card-stat-val">{rmse_fmt}</div>
    </div>
  </div>
</div>"""
cards_html += "</div>"
st.markdown(cards_html, unsafe_allow_html=True)

st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_actual, tab_fi, tab_scatter = st.tabs([
    "  Actual vs Predicted  ",
    "  Feature Importance  ",
    "  Prediction Scatter  ",
])

# ══ TAB 1 — Actual vs Predicted ══════════════════════════════════════════════
with tab_actual:
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    split_opt = st.radio(
        "Show", ["Test set only (Nov–Dec 2025)", "Full dataset (2024–2025)"],
        horizontal=True, label_visibility="collapsed",
        key="avp_split"
    )
    show_df = pred if "Full" in split_opt else pred[pred["split"] == "test"]

    col1, col2 = st.columns(2)

    for i, target in enumerate(["revenue", "occupancy", "adr", "revpar"]):
        color = MODEL_COLORS[target]
        col   = col1 if i % 2 == 0 else col2
        df_t  = show_df.sort_values("business_date")

        actual_y = df_t[f"actual_{target}"]
        pred_y   = df_t[f"predicted_{target}"]
        if target == "occupancy":
            actual_y = actual_y * 100
            pred_y   = pred_y   * 100

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_t["business_date"], y=actual_y,
            name="Actual", mode="lines",
            line=dict(color="rgba(245,245,240,0.5)", width=1.5),
        ))
        fig.add_trace(go.Scatter(
            x=df_t["business_date"], y=pred_y,
            name="Predicted", mode="lines",
            line=dict(color=color, width=2, dash="dot"),
        ))
        m = metrics[target]
        title_txt = (
            f"{MODEL_LABELS[target]}  ·  "
            f"R²={m['r2']:.4f}  MAPE={m['mape']:.1f}%"
        )
        _dark(fig)
        fig.update_layout(
            title=dict(text=title_txt, font=dict(size=33, color="rgba(245,245,240,.8)")),
            height=260, margin=dict(l=10, r=10, t=36, b=10),
        )
        if target == "occupancy":
            fig.update_yaxes(ticksuffix="%")
        elif target in ("revenue", "adr", "revpar"):
            fig.update_yaxes(tickprefix="$")

        with col:
            st.plotly_chart(fig, use_container_width=True)

# ══ TAB 2 — Feature Importance ════════════════════════════════════════════════
with tab_fi:
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    fi_col1, fi_col2 = st.columns([1, 3])
    with fi_col1:
        fi_model = st.radio(
            "Model", ["Revenue", "Occupancy", "ADR", "RevPAR"],
            key="fi_model"
        )
        top_n = st.slider("Top N features", 10, 30, 20, key="fi_topn")

    fi_key = fi_model.lower()
    top = fi.nlargest(top_n, fi_key)[["feature", fi_key]].copy()
    top["pct"] = top[fi_key] / top[fi_key].max()

    color = MODEL_COLORS[fi_key]

    with fi_col2:
        # Horizontal bar chart
        fig_fi = go.Figure(go.Bar(
            x=top[fi_key].values[::-1],
            y=top["feature"].values[::-1],
            orientation="h",
            marker_color=color,
            marker_opacity=0.85,
        ))
        _dark(fig_fi)
        fig_fi.update_layout(
            title=dict(
                text=f"Top {top_n} Features — {fi_model} Model (gain-based importance)",
                font=dict(size=33, color="rgba(245,245,240,.8)")
            ),
            height=max(360, top_n * 22),
            margin=dict(l=10, r=10, t=36, b=10),
            xaxis_title="Importance (gain)",
        )
        st.plotly_chart(fig_fi, use_container_width=True)

    # ── Feature category legend ───────────────────────────────────────────
    st.markdown("""
<div style="display:flex;flex-wrap:wrap;gap:8px;padding:0 4px 16px;">
  <span style="font-size:9px;padding:3px 8px;border-radius:10px;background:rgba(232,133,74,.15);color:#e8854a;">Booking Pace</span>
  <span style="font-size:9px;padding:3px 8px;border-radius:10px;background:rgba(62,207,142,.12);color:#3ecf8e;">Lag / Rolling</span>
  <span style="font-size:9px;padding:3px 8px;border-radius:10px;background:rgba(74,111,165,.15);color:#4a6fa5;">Calendar</span>
  <span style="font-size:9px;padding:3px 8px;border-radius:10px;background:rgba(155,111,212,.15);color:#9b6fd4;">STR / Competitive</span>
  <span style="font-size:9px;padding:3px 8px;border-radius:10px;background:rgba(245,245,240,.08);color:rgba(245,245,240,.5);">Weather · Holidays · Events · Medallia</span>
</div>
""", unsafe_allow_html=True)

# ══ TAB 3 — Scatter ══════════════════════════════════════════════════════════
with tab_scatter:
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    sc_col1, sc_col2 = st.columns([1, 3])
    with sc_col1:
        sc_model  = st.radio("Model", ["Revenue", "Occupancy", "ADR", "RevPAR"], key="sc_model")
        sc_split  = st.radio("Data", ["Test only", "All data"], key="sc_split")

    sc_key = sc_model.lower()
    sc_df  = pred if sc_split == "All data" else pred[pred["split"] == "test"]
    sc_df  = sc_df.copy()

    act = sc_df[f"actual_{sc_key}"]
    prd = sc_df[f"predicted_{sc_key}"]
    if sc_key == "occupancy":
        act = act * 100
        prd = prd * 100

    color = MODEL_COLORS[sc_key]
    mn, mx = float(min(act.min(), prd.min())), float(max(act.max(), prd.max()))

    fig_sc = go.Figure()
    fig_sc.add_trace(go.Scatter(
        x=act, y=prd,
        mode="markers",
        marker=dict(color=color, size=5, opacity=0.7),
        name="Days",
        customdata=sc_df["business_date"].dt.strftime("%b %d %Y"),
        hovertemplate="<b>%{customdata}</b><br>Actual: %{x:,.1f}<br>Predicted: %{y:,.1f}<extra></extra>",
    ))
    # Perfect prediction line
    fig_sc.add_trace(go.Scatter(
        x=[mn, mx], y=[mn, mx],
        mode="lines",
        line=dict(color="rgba(255,255,255,0.2)", width=1, dash="dash"),
        name="Perfect fit",
        hoverinfo="skip",
    ))
    m = metrics[sc_key]
    _dark(fig_sc)
    fig_sc.update_layout(
        title=dict(
            text=f"{sc_model} — Actual vs Predicted  ·  R²={m['r2']:.4f}  MAPE={m['mape']:.1f}%",
            font=dict(size=33, color="rgba(245,245,240,.8)")
        ),
        height=460,
        xaxis_title="Actual",
        yaxis_title="Predicted",
        margin=dict(l=10, r=10, t=36, b=40),
    )
    if sc_key in ("revenue", "adr", "revpar"):
        fig_sc.update_xaxes(tickprefix="$")
        fig_sc.update_yaxes(tickprefix="$")
    elif sc_key == "occupancy":
        fig_sc.update_xaxes(ticksuffix="%")
        fig_sc.update_yaxes(ticksuffix="%")

    with sc_col2:
        st.plotly_chart(fig_sc, use_container_width=True)

# ── Model architecture info ───────────────────────────────────────────────────
with st.expander("Model architecture & training details", expanded=False):
    st.markdown("""
<div style="padding:12px 4px;color:rgba(245,245,240,.75);font-size:12px;line-height:1.8;">

**Algorithm:** LightGBM (Light Gradient Boosting Machine) — gradient boosted decision trees

**Training split (temporal, never random):**
- Train-fit: Jan 2024 – Sep 2025 (639 days)
- Validation (early stopping): Oct 2025 (31 days)
- Test: Nov–Dec 2025 (61 days)

**Parameters:** n_estimators=2000 · learning_rate=0.03 · num_leaves=31 · min_child_samples=15 · feature_fraction=0.8 · bagging_fraction=0.8 · early_stopping=50 rounds

**Feature strategy:**

| Model | Base (67) | STR Index | STR Raw |
|---|---|---|---|
| Revenue | ✓ | — | — |
| Occupancy | ✓ | — | — |
| ADR | ✓ | ✓ | ✓ |
| RevPAR | ✓ | ✓ | ✓ |

Revenue and Occupancy exclude STR features — own booking pace outperformed competitor data.
ADR and RevPAR include STR because competitor pricing directly drives Arlo's rate decisions.

</div>
""", unsafe_allow_html=True)
