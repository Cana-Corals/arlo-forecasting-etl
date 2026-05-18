import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from pathlib import Path

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Arlo Williamsburg Forecasting",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    /* Hide default Streamlit elements */
    #MainMenu, footer, header { visibility: hidden; }
    [data-testid="collapsedControl"] { display: none; }

    /* Hide sidebar nav labels */
    section[data-testid="stSidebar"] { display: none; }

    /* Full page centering for login/home */
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }

    /* Title styling */
    .arlo-title {
        text-align: center;
        font-size: 2.4rem;
        font-weight: 700;
        color: #1A1A1A;
        margin-bottom: 0.2rem;
    }
    .arlo-subtitle {
        text-align: center;
        font-size: 1rem;
        color: #1A1A1A;
        letter-spacing: 0.08em;
        margin-bottom: 2rem;
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

    /* Nav bar bottom */
    .nav-bar {
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
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
        cursor: pointer;
    }
    .nav-item:hover { color: #C8522A; }

    /* Login form */
    .login-container {
        max-width: 380px;
        margin: 0 auto;
    }

    /* Home search bar */
    .search-hint {
        text-align: center;
        font-size: 0.82rem;
        color: #BDBDBD;
        margin-top: 0.4rem;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Load user config
# Streamlit Cloud → reads from st.secrets (dashboard)
# Local dev       → reads from config/users.yaml
# ---------------------------------------------------------------------------

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


config = load_auth_config()

authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
)

# ---------------------------------------------------------------------------
# Login page
# ---------------------------------------------------------------------------

def show_login():
    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown('<div class="arlo-title">Arlo Williamsburg</div>', unsafe_allow_html=True)
        st.markdown('<div class="arlo-subtitle">FORECASTING</div>', unsafe_allow_html=True)
        st.markdown('<div class="arlo-subtitle" style="font-size:0.85rem; color:#9E9E9E;">Internal Intelligence Platform</div>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        authenticator.login(location="main", fields={
            "Form name": "",
            "Username": "Username",
            "Password": "Password",
            "Login": "Login",
        })

        if st.session_state.get("authentication_status") is False:
            st.error("Incorrect username or password.")

        st.markdown('<div class="confidential">CONFIDENTIAL — INTERNAL USE ONLY</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Home page (after login)
# ---------------------------------------------------------------------------

def show_home():
    from datetime import date, datetime

    name = st.session_state.get("name", "")
    today = date.today()
    date_str = f"{today.strftime('%A')}, {today.strftime('%B')} {today.day} · {today.year}"

    hour = datetime.now().hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 18 else "Good evening"

    st.markdown("<br><br><br>", unsafe_allow_html=True)
    st.markdown(f'<div class="arlo-title">{greeting}, {name.split()[0]}.</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="arlo-date">{date_str}</div>', unsafe_allow_html=True)

    # AI search bar
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
            st.info("AI Assistant coming soon — all other pages must be built first.")

    # Bottom nav
    from components.nav import render_nav
    render_nav()

    # Logout in top right
    with st.container():
        col1, col2 = st.columns([9, 1])
        with col2:
            authenticator.logout("Logout", location="main")


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

if st.session_state.get("authentication_status"):
    show_home()
else:
    show_login()
