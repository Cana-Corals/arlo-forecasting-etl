import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from pathlib import Path
from datetime import date, datetime
import sys
import re
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))

# ── Page config — must be first ──────────────────────────────────────────────
st.set_page_config(
    page_title="Arlo Williamsburg Forecasting",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Shared CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    #MainMenu, footer, header { visibility: hidden; }
    [data-testid="collapsedControl"] { display: none; }
    .stApp { background: #111111 !important; }
    [data-testid="stAppViewContainer"] { background: #111111 !important; }
    .block-container { padding-top: 0 !important; padding-bottom: 0 !important; max-width: 1400px !important; }

    .arlo-title { font-size: 1.6rem; font-weight: 600; color: #f5f5f0; margin-bottom: 0.15rem; }
    .arlo-date  { font-size: 0.85rem; color: rgba(245,245,240,0.4); margin-bottom: 1rem; }
    .search-hint { font-size: 0.72rem; color: rgba(245,245,240,0.3); text-align: center; margin-top: 0.3rem; }

    div[data-testid="stTextInput"] input {
        background: #1a1a1a !important;
        border: 1px solid rgba(255,255,255,0.12) !important;
        border-radius: 8px !important;
        color: #f5f5f0 !important;
        font-size: 0.95rem !important;
        padding: 12px 16px !important;
    }
    div[data-testid="stTextInput"] input:focus {
        border-color: rgba(232,133,74,0.5) !important;
        box-shadow: 0 0 0 2px rgba(232,133,74,0.1) !important;
    }
    button[kind="primaryFormSubmit"] { display: none !important; }

    .chat-q {
        font-size: 0.75rem; font-weight: 600; color: rgba(232,133,74,0.85);
        text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 6px;
    }
    .chat-answer {
        background: #1c1c1c; border: 1px solid rgba(255,255,255,0.07);
        border-radius: 8px; padding: 12px 16px; margin-bottom: 14px;
        color: rgba(245,245,240,0.82); font-size: 0.87rem; line-height: 1.65;
    }
    .chat-answer p  { margin: 0 0 6px 0; }
    .chat-answer ul, .chat-answer ol { margin: 4px 0 6px 0; padding-left: 18px; }
    .chat-answer strong { color: #f5f5f0; }
    .chat-empty {
        text-align: center; padding-top: 80px;
        color: rgba(245,245,240,0.18); font-size: 0.82rem; line-height: 2;
    }
</style>
""", unsafe_allow_html=True)

# ── Auth config ───────────────────────────────────────────────────────────────
def load_auth_config() -> dict:
    if "credentials" in st.secrets:
        credentials = {"usernames": {}}
        for username, data in st.secrets["credentials"]["usernames"].items():
            credentials["usernames"][username] = dict(data)
        return {
            "credentials": credentials,
            "cookie": {
                "name":        st.secrets["cookie"]["name"],
                "key":         st.secrets["cookie"]["key"],
                "expiry_days": st.secrets["cookie"]["expiry_days"],
            },
        }
    config_path = BASE / "config" / "users.yaml"
    with open(config_path) as f:
        return yaml.load(f, Loader=SafeLoader)


config        = load_auth_config()
authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
)

# ── Dark chart theme ──────────────────────────────────────────────────────────
DARK_LAYOUT = dict(
    paper_bgcolor="#1a1a1a",
    plot_bgcolor="#1a1a1a",
    font=dict(color="rgba(245,245,240,0.7)", family="Inter, system-ui, sans-serif", size=11),
    xaxis=dict(gridcolor="rgba(255,255,255,0.06)", linecolor="rgba(255,255,255,0.1)", zeroline=False),
    yaxis=dict(gridcolor="rgba(255,255,255,0.06)", linecolor="rgba(255,255,255,0.1)", zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
    margin=dict(l=10, r=10, t=36, b=10),
    hoverlabel=dict(bgcolor="#222222", bordercolor="rgba(255,255,255,0.15)", font_color="#f5f5f0"),
)
ACCENT = "#e8854a"
GREEN  = "#3ecf8e"
RED    = "#e05252"
SLATE  = "#4a6fa5"

# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _load_master() -> pd.DataFrame:
    return pd.read_csv(BASE / "data" / "final" / "hotel_daily_master.csv", parse_dates=["business_date"])

@st.cache_data(ttl=3600, show_spinner=False)
def _load_forecast() -> pd.DataFrame:
    try:
        from components.forecast_engine import generate_future_predictions
        return generate_future_predictions(2026)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def _build_hotel_context() -> str:
    master = _load_master()
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    lines = ["## Arlo Williamsburg Hotel Data\n", "**Property**: 147 rooms, Brooklyn NY\n\n"]
    for year in [2024, 2025]:
        df_y = master[master["business_date"].dt.year == year]
        if df_y.empty:
            continue
        lines.append(f"### {year} Full Year\n")
        lines.append(f"- Revenue: ${df_y['room_revenue'].sum():,.0f}\n")
        lines.append(f"- Avg Occ: {df_y['occupancy_rate'].mean()*100:.1f}%\n")
        lines.append(f"- Avg ADR: ${df_y['adr'].mean():.0f}\n")
        lines.append(f"- Avg RevPAR: ${df_y['revpar'].mean():.0f}\n\n")
        monthly = df_y.groupby(df_y["business_date"].dt.month).agg(
            occ=("occupancy_rate","mean"), adr=("adr","mean"),
            revpar=("revpar","mean"), revenue=("room_revenue","sum"),
        )
        lines.append(f"#### {year} Monthly\n| Month | Occ% | ADR | RevPAR | Revenue |\n|---|---|---|---|---|\n")
        for m, r in monthly.iterrows():
            lines.append(f"| {month_names[m-1]} | {r['occ']*100:.1f}% | ${r['adr']:.0f} | ${r['revpar']:.0f} | ${r['revenue']:,.0f} |\n")
        lines.append("\n")
    df24 = master[master["business_date"].dt.year == 2024]
    df25 = master[master["business_date"].dt.year == 2025]
    if not df24.empty and not df25.empty:
        lines.append("### YoY 2024→2025\n")
        lines.append(f"- Revenue: {(df25['room_revenue'].sum()/df24['room_revenue'].sum()-1)*100:+.1f}%\n")
        lines.append(f"- Occ: {(df25['occupancy_rate'].mean()/df24['occupancy_rate'].mean()-1)*100:+.1f}pp\n")
        lines.append(f"- ADR: {(df25['adr'].mean()/df24['adr'].mean()-1)*100:+.1f}%\n\n")
    try:
        fc = _load_forecast()
        if not fc.empty:
            lines.append("### 2026 ML Forecast\n")
            lines.append(f"- Projected Revenue: ${fc['pred_revenue'].sum():,.0f}\n")
            lines.append(f"- Projected Occ: {fc['pred_occupancy'].mean()*100:.1f}%\n")
            lines.append(f"- Projected ADR: ${fc['pred_adr'].mean():.0f}\n")
    except Exception:
        pass
    return "".join(lines)

# ── Claude call ───────────────────────────────────────────────────────────────
_SYSTEM = """You are a hotel revenue intelligence assistant for Arlo Williamsburg (147 rooms, Brooklyn NY).

Answer questions concisely using the data provided. Use markdown formatting (bold key numbers, bullet lists).

CHARTS: When a visualization would make the answer clearer, include Plotly code inside <chart></chart> tags. The code must assign a Plotly figure to a variable named `fig`. You can include text before and/or after the chart tags.

Available variables inside chart code:
- `master` (DataFrame, 2024–2025 daily): business_date, room_revenue, occupancy_rate (0–1), adr, revpar, room_nights, ooo_rooms, available_rooms, temp_mean_f, had_precipitation, is_federal_holiday, is_major_event_day, is_barclays_event, is_msg_event, is_yankees_game, is_us_open_event, pickup_7d, pickup_14d, pickup_30d, total_rooms_on_books, avg_booked_rate, medallia_overall_satisfaction
- `forecast` (DataFrame, 2026 ML predictions): business_date, pred_revenue, pred_occupancy (0–1), pred_adr, pred_revpar
- `go`, `px`, `pd`, `np`
- `DARK_LAYOUT` — always call `fig.update_layout(**DARK_LAYOUT)` on every chart
- `ACCENT="#e8854a"` (orange), `GREEN="#3ecf8e"`, `RED="#e05252"`, `SLATE="#4a6fa5"`

Chart rules:
- Always apply `fig.update_layout(**DARK_LAYOUT)`
- Use ACCENT as the primary series color
- Keep titles short, use `title=dict(text="...", font_color="rgba(245,245,240,0.9)")`
- Bar charts for monthly comparisons, line charts for time trends

Example:
<chart>
df = master[master["business_date"].dt.year == 2025].copy()
monthly = df.groupby(df["business_date"].dt.month)["room_revenue"].sum().reset_index()
labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
monthly["label"] = monthly["business_date"].apply(lambda m: labels[m-1])
fig = go.Figure(go.Bar(x=monthly["label"], y=monthly["room_revenue"], marker_color=ACCENT))
fig.update_layout(**DARK_LAYOUT, title=dict(text="2025 Monthly Revenue", font_color="rgba(245,245,240,0.9)"))
</chart>
"""

def _call_claude(question: str, context: str, history: list) -> str:
    import anthropic
    api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "⚠️ ANTHROPIC_API_KEY not set in Streamlit secrets."
    client = anthropic.Anthropic(api_key=api_key)
    messages = []
    for item in history[-4:]:
        messages.append({"role": "user",      "content": item["question"]})
        messages.append({"role": "assistant", "content": item["answer_raw"]})
    messages.append({"role": "user", "content": f"{question}\n\n---\n{context}"})
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2048,
        system=_SYSTEM,
        messages=messages,
    )
    return resp.content[0].text

# ── Parse response into segments (no rendering) ───────────────────────────────
def _parse_segments(response_text: str) -> list:
    """Execute chart code and return segments as {type, content} dicts."""
    master   = _load_master()
    forecast = _load_forecast()
    exec_ns  = {
        "go": go, "px": px, "pd": pd, "np": np,
        "master":   master.copy(),
        "forecast": forecast.copy() if not forecast.empty else pd.DataFrame(),
        "DARK_LAYOUT": DARK_LAYOUT,
        "ACCENT": ACCENT, "GREEN": GREEN, "RED": RED, "SLATE": SLATE,
    }
    parts    = re.split(r'(<chart>.*?</chart>)', response_text, flags=re.DOTALL)
    segments = []
    for part in parts:
        if part.startswith("<chart>") and part.endswith("</chart>"):
            code = part[7:-8].strip()
            try:
                ns = {**exec_ns}
                exec(code, ns)
                fig = ns.get("fig")
                if isinstance(fig, go.Figure):
                    segments.append({"type": "chart", "content": fig.to_dict()})
                else:
                    segments.append({"type": "text", "content": "_Chart error: no `fig` produced._"})
            except Exception as e:
                segments.append({"type": "text", "content": f"_Chart error: {e}_"})
        else:
            text = part.strip()
            if text:
                segments.append({"type": "text", "content": text})
    return segments

# ── Render stored segments ────────────────────────────────────────────────────
def _render_item(item: dict):
    st.markdown(f'<div class="chat-q">{item["question"]}</div>', unsafe_allow_html=True)
    for seg in item.get("segments", []):
        if seg["type"] == "text":
            st.markdown(f'<div class="chat-answer">{seg["content"]}</div>', unsafe_allow_html=True)
        elif seg["type"] == "chart":
            try:
                st.plotly_chart(go.Figure(seg["content"]), use_container_width=True)
            except Exception:
                pass

# ── Login page ────────────────────────────────────────────────────────────────
def login_page():
    st.markdown("""
    <style>
        section[data-testid="stSidebar"] { display: none; }
        .block-container { padding-top: 2rem; }
        .arlo-title { text-align:center; font-size:2.4rem; font-weight:700; color:#f5f5f0; margin-bottom:0.2rem; }
        .confidential { text-align:center; font-size:0.75rem; color:rgba(245,245,240,0.3); letter-spacing:0.12em; margin-top:2rem; }
    </style>
    """, unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown('<div class="arlo-title">Arlo Williamsburg</div>', unsafe_allow_html=True)
        st.markdown(
            '<div style="text-align:center;font-size:1rem;color:#1A1A1A;letter-spacing:0.08em;margin-bottom:0.4rem;">FORECASTING</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div style="text-align:center;font-size:0.85rem;color:#9E9E9E;margin-bottom:2rem;">Internal Intelligence Platform</div>',
            unsafe_allow_html=True,
        )
        authenticator.login(
            location="main",
            fields={"Form name": "", "Username": "Username", "Password": "Password", "Login": "Login"},
        )
        if st.session_state.get("authentication_status") is False:
            st.error("Incorrect username or password.")
        st.markdown('<div class="confidential">CONFIDENTIAL — INTERNAL USE ONLY</div>', unsafe_allow_html=True)


# ── Home page ─────────────────────────────────────────────────────────────────
def home_page():
    from components.nav import render_nav

    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    # ── Process a pending question before any rendering ───────────────────────
    pending_q = st.session_state.pop("pending_q", None)
    if pending_q:
        with st.spinner("Thinking..."):
            context = _build_hotel_context()
            raw     = _call_claude(pending_q, context, st.session_state["chat_history"])
        segments = _parse_segments(raw)
        st.session_state["chat_history"].append({
            "question":   pending_q,
            "answer_raw": raw,
            "segments":   segments,
        })

    name     = st.session_state.get("name", "")
    today    = date.today()
    date_str = f"{today.strftime('%A')}, {today.strftime('%B')} {today.day} · {today.year}"
    hour     = datetime.now().hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 18 else "Good evening"

    col_space, col_logout = st.columns([11, 1])
    with col_logout:
        authenticator.logout("Logout", location="main")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f'<div class="arlo-title">{greeting}, {name.split()[0]}.</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="arlo-date">{date_str}</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # Fixed-height scrollable chat box
        chat_box = st.container(height=420, border=False)
        with chat_box:
            if not st.session_state["chat_history"]:
                st.markdown(
                    '<div class="chat-empty">'
                    'Ask anything about the hotel<br>'
                    '"Show me Q3 occupancy"<br>'
                    '"Chart monthly ADR 2024 vs 2025"<br>'
                    '"How is the 2026 forecast looking?"'
                    '</div>',
                    unsafe_allow_html=True,
                )
            for item in st.session_state["chat_history"]:
                _render_item(item)

        # Input always visible below the box
        with st.form("ai_form", clear_on_submit=True, border=False):
            question = st.text_input(
                label="",
                placeholder='Ask anything — "Chart monthly ADR" or "How is Q3 looking?"',
                label_visibility="collapsed",
                key="home_question",
            )
            st.form_submit_button("Ask", use_container_width=True, type="primary")

        st.markdown(
            '<div class="search-hint">Press Enter to ask · Powered by Claude AI · Charts supported</div>',
            unsafe_allow_html=True,
        )

        if question and question.strip():
            st.session_state["pending_q"] = question.strip()
            st.rerun()

    render_nav()


# ── Navigation router ─────────────────────────────────────────────────────────
is_auth = st.session_state.get("authentication_status")

if is_auth:
    pg = st.navigation(
        [
            st.Page(home_page,                   title="Home",          url_path="",    default=True),
            st.Page("pages/1_Dashboard.py",      title="Dashboard"),
            st.Page("pages/2_Forecast.py",       title="Forecast"),
            st.Page("pages/3_Performance.py",    title="Performance"),
            st.Page("pages/4_Demand.py",         title="Demand"),
            st.Page("pages/5_Competitive.py",    title="Competitive"),
            st.Page("pages/6_Model_Insights.py", title="Model Insights"),
        ],
        position="hidden",
    )
else:
    pg = st.navigation(
        [st.Page(login_page, title="Login", url_path="", default=True)],
        position="hidden",
    )

pg.run()
