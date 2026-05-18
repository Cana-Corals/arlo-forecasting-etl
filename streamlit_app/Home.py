import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from pathlib import Path
from datetime import date, datetime

# ── Page config — must be first ──────────────────────────────────────────────
st.set_page_config(
    page_title="Arlo Williamsburg Forecasting",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Shared CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    #MainMenu, footer, header { visibility: hidden; }
    [data-testid="collapsedControl"] { display: none; }
    section[data-testid="stSidebar"] { display: none; }
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }

    .arlo-title {
        text-align: center;
        font-size: 2.4rem;
        font-weight: 700;
        color: #1A1A1A;
        margin-bottom: 0.2rem;
    }
    .arlo-date {
        text-align: center;
        font-size: 0.95rem;
        color: #9E9E9E;
        margin-bottom: 2.5rem;
    }
    .confidential {
        text-align: center;
        font-size: 0.75rem;
        color: #BDBDBD;
        letter-spacing: 0.12em;
        margin-top: 2rem;
    }
    .search-hint {
        text-align: center;
        font-size: 0.82rem;
        color: #BDBDBD;
        margin-top: 0.4rem;
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

# ── Login page ────────────────────────────────────────────────────────────────
def login_page():
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
        question = st.text_input(
            label="",
            placeholder='Ask anything — "How is Q3 looking?"',
            label_visibility="collapsed",
            key="home_question",
        )
        st.markdown('<div class="search-hint">Press Enter to ask · Powered by Claude AI</div>', unsafe_allow_html=True)
        if question:
            st.session_state["ai_question"] = question
            st.info("AI Assistant coming soon.")

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
        [st.Page(login_page, title="Login", url_path="")],
        position="hidden",
    )

pg.run()
