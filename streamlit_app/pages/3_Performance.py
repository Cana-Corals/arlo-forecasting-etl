import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.nav import render_nav

st.markdown("<style>#MainMenu,footer,header{visibility:hidden}[data-testid='collapsedControl']{display:none}section[data-testid='stSidebar']{display:none}</style>", unsafe_allow_html=True)
st.markdown("## Performance Analysis")
st.markdown("---")
st.info("Coming soon — this page is under construction.")
render_nav()
