import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.auth import require_auth
from components.nav import render_nav

st.set_page_config(page_title="Forecast · Arlo Forecasting", page_icon="🏨", layout="wide", initial_sidebar_state="collapsed")
require_auth()

st.markdown("<style>#MainMenu,footer,header{visibility:hidden}[data-testid='collapsedControl']{display:none}section[data-testid='stSidebar']{display:none}</style>", unsafe_allow_html=True)
st.markdown("## Forecast Calendar")
st.markdown("---")
st.info("Coming soon — this page is under construction.")
render_nav()
