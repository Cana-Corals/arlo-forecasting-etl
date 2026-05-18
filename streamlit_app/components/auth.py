import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from pathlib import Path


def _load_config() -> dict:
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
    config_path = Path(__file__).resolve().parents[2] / "config" / "users.yaml"
    with open(config_path) as f:
        return yaml.load(f, Loader=SafeLoader)


def require_auth() -> stauth.Authenticate:
    """
    Call at the top of every page (after set_page_config).
    Restores auth from the login cookie if the session was reset by navigation.
    Redirects to the login screen if the cookie is missing or expired.
    """
    config = _load_config()
    authenticator = stauth.Authenticate(
        config["credentials"],
        config["cookie"]["name"],
        config["cookie"]["key"],
        config["cookie"]["expiry_days"],
    )
    # Check cookie silently — no UI rendered
    authenticator.login(location="unrendered")
    if not st.session_state.get("authentication_status"):
        st.switch_page("Home.py")
    return authenticator
