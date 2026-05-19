import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.sidebar import render_sidebar

st.markdown("<style>#MainMenu,footer,header{visibility:hidden}[data-testid='collapsedControl']{display:none}</style>",
            unsafe_allow_html=True)
render_sidebar(active="demand")

st.markdown("## Demand Drivers")
st.info("Coming soon — this page is under construction.")
