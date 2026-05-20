import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from pathlib import Path
from datetime import date, datetime
import sys
import pandas as pd

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
    .arlo-date  { font-size: 0.85rem; color: rgba(245,245,240,0.4); margin-bottom: 1.5rem; }
    .search-hint { font-size: 0.72rem; color: rgba(245,245,240,0.3); text-align: center; margin-top: 0.3rem; }

    /* Style the text input to look like a search bar */
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

    /* Hide the Ask button — form submits on Enter */
    button[kind="primaryFormSubmit"] { display: none !important; }

    /* Chat history cards */
    .chat-q {
        font-size: 0.78rem; font-weight: 600; color: rgba(232,133,74,0.9);
        margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.05em;
    }
    .chat-card {
        background: #1a1a1a; border: 1px solid rgba(255,255,255,0.07);
        border-radius: 8px; padding: 14px 18px; margin-bottom: 12px;
    }
    .chat-card p { color: rgba(245,245,240,0.8); font-size: 0.88rem; margin: 0; }
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
    config_path = Path(__file__).resolve().parents[1] / "config" / "users.yaml"
    with open(config_path) as f:
        return yaml.load(f, Loader=SafeLoader)


config        = load_auth_config()
authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
)

# ── Hotel data context builder ────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _build_hotel_context() -> str:
    master = pd.read_csv(
        BASE / "data" / "final" / "hotel_daily_master.csv",
        parse_dates=["business_date"],
    )

    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    lines = [
        "## Arlo Williamsburg — Hotel Revenue Intelligence\n",
        "**Property**: Arlo Williamsburg, Brooklyn NY | 147 rooms | Williamsburg neighborhood\n\n",
    ]

    for year in [2024, 2025]:
        df_y = master[master["business_date"].dt.year == year]
        if df_y.empty:
            continue
        total_rev  = df_y["room_revenue"].sum()
        avg_occ    = df_y["occupancy_rate"].mean() * 100
        avg_adr    = df_y["adr"].mean()
        avg_revpar = df_y["revpar"].mean()
        lines.append(f"### {year} Full Year\n")
        lines.append(f"- Total Room Revenue: ${total_rev:,.0f}\n")
        lines.append(f"- Avg Occupancy: {avg_occ:.1f}%\n")
        lines.append(f"- Avg ADR: ${avg_adr:.0f}\n")
        lines.append(f"- Avg RevPAR: ${avg_revpar:.0f}\n\n")

        monthly = df_y.groupby(df_y["business_date"].dt.month).agg(
            occ=("occupancy_rate", "mean"),
            adr=("adr", "mean"),
            revpar=("revpar", "mean"),
            revenue=("room_revenue", "sum"),
            room_nights=("room_nights", "sum"),
        )
        lines.append(f"#### {year} Monthly Breakdown\n")
        lines.append("| Month | Occ% | ADR | RevPAR | Revenue | Rm Nights |\n")
        lines.append("|-------|------|-----|--------|---------|----------|\n")
        for m, row in monthly.iterrows():
            lines.append(
                f"| {month_names[m-1]} | {row['occ']*100:.1f}% | ${row['adr']:.0f} |"
                f" ${row['revpar']:.0f} | ${row['revenue']:,.0f} | {int(row['room_nights'])} |\n"
            )
        lines.append("\n")

    # YoY comparison
    df24 = master[master["business_date"].dt.year == 2024]
    df25 = master[master["business_date"].dt.year == 2025]
    if not df24.empty and not df25.empty:
        rev_chg  = (df25["room_revenue"].sum() / df24["room_revenue"].sum() - 1) * 100
        occ_chg  = (df25["occupancy_rate"].mean() / df24["occupancy_rate"].mean() - 1) * 100
        adr_chg  = (df25["adr"].mean() / df24["adr"].mean() - 1) * 100
        lines.append("### 2024→2025 Year-over-Year Change\n")
        lines.append(f"- Revenue: {rev_chg:+.1f}%\n")
        lines.append(f"- Occupancy: {occ_chg:+.1f}pp\n")
        lines.append(f"- ADR: {adr_chg:+.1f}%\n\n")

    # 2026 ML forecast
    try:
        from components.forecast_engine import generate_future_predictions
        fc = generate_future_predictions(2026)
        fc_total  = fc["pred_revenue"].sum()
        fc_occ    = fc["pred_occupancy"].mean() * 100
        fc_adr    = fc["pred_adr"].mean()
        fc_revpar = fc["pred_revpar"].mean()
        lines.append("### 2026 ML Forecast (full year)\n")
        lines.append(f"- Projected Revenue: ${fc_total:,.0f}\n")
        lines.append(f"- Projected Avg Occupancy: {fc_occ:.1f}%\n")
        lines.append(f"- Projected Avg ADR: ${fc_adr:.0f}\n")
        lines.append(f"- Projected Avg RevPAR: ${fc_revpar:.0f}\n\n")

        fc_m = fc.groupby(fc["business_date"].dt.month).agg(
            occ=("pred_occupancy", "mean"),
            adr=("pred_adr", "mean"),
            revpar=("pred_revpar", "mean"),
            revenue=("pred_revenue", "sum"),
        )
        lines.append("#### 2026 Monthly Forecast\n")
        lines.append("| Month | Pred Occ% | Pred ADR | Pred RevPAR | Pred Revenue |\n")
        lines.append("|-------|-----------|----------|-------------|---------------|\n")
        for m, row in fc_m.iterrows():
            lines.append(
                f"| {month_names[m-1]} | {row['occ']*100:.1f}% | ${row['adr']:.0f} |"
                f" ${row['revpar']:.0f} | ${row['revenue']:,.0f} |\n"
            )
    except Exception:
        pass

    return "".join(lines)


def _stream_claude(question: str, context: str, history: list):
    """Yields text chunks from Claude API."""
    import anthropic
    api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        yield "⚠️ ANTHROPIC_API_KEY not set in Streamlit secrets."
        return

    client = anthropic.Anthropic(api_key=api_key)

    system = (
        "You are a hotel revenue intelligence assistant for Arlo Williamsburg, a 147-room boutique hotel "
        "in Brooklyn, New York. You have access to the hotel's actual performance data for 2024–2025 and "
        "ML-generated revenue forecasts for 2026.\n\n"
        "Answer questions concisely and professionally. Focus on actionable revenue management insights. "
        "Use specific numbers from the data when relevant. Use markdown formatting (bold key numbers, "
        "bullet lists, short tables) to make answers easy to scan. "
        "If you need data that isn't in the context, say so and offer the closest available insight."
    )

    messages = []
    for item in history[-4:]:
        messages.append({"role": "user",      "content": item["question"]})
        messages.append({"role": "assistant", "content": item["answer"]})
    messages.append({
        "role": "user",
        "content": f"{question}\n\n---\n{context}",
    })

    with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=system,
        messages=messages,
    ) as stream:
        for chunk in stream.text_stream:
            yield chunk


# ── Login page ────────────────────────────────────────────────────────────────
def login_page():
    st.markdown("""
    <style>
        section[data-testid="stSidebar"] { display: none; }
        .block-container { padding-top: 2rem; padding-bottom: 2rem; }
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

    name     = st.session_state.get("name", "")
    today    = date.today()
    date_str = f"{today.strftime('%A')}, {today.strftime('%B')} {today.day} · {today.year}"
    hour     = datetime.now().hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 18 else "Good evening"

    col_space, col_logout = st.columns([11, 1])
    with col_logout:
        authenticator.logout("Logout", location="main")

    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown(f'<div class="arlo-title">{greeting}, {name.split()[0]}.</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="arlo-date">{date_str}</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # Previous conversation history
        for item in st.session_state["chat_history"]:
            st.markdown(
                f'<div class="chat-card"><div class="chat-q">{item["question"]}</div></div>',
                unsafe_allow_html=True,
            )
            st.markdown(item["answer"])
            st.markdown("---")

        # Input form — submits on Enter
        with st.form("ai_form", clear_on_submit=True, border=False):
            question = st.text_input(
                label="",
                placeholder='Ask anything — "How is Q3 looking?"',
                label_visibility="collapsed",
                key="home_question",
            )
            st.form_submit_button("Ask", use_container_width=True, type="primary")

        st.markdown('<div class="search-hint">Press Enter to ask · Powered by Claude AI</div>', unsafe_allow_html=True)

        # Process submitted question
        if question and question.strip():
            q = question.strip()
            context = _build_hotel_context()
            response_slot = st.empty()
            full_answer = ""
            try:
                for chunk in _stream_claude(q, context, st.session_state["chat_history"]):
                    full_answer += chunk
                    response_slot.markdown(full_answer + " ▌")
                response_slot.markdown(full_answer)
                st.session_state["chat_history"].append({"question": q, "answer": full_answer})
            except Exception as exc:
                st.error(f"Error calling Claude: {exc}")

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
