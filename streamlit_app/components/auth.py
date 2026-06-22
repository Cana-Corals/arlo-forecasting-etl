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


def require_auth() -> None:
    pass
