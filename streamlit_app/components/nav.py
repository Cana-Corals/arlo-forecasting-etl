import streamlit as st
from pathlib import Path

PAGES = [
    ("Home",           "Home.py"),
    ("Dashboard",      "pages/1_Dashboard.py"),
    ("Forecast",       "pages/2_Forecast.py"),
    ("Performance",    "pages/3_Performance.py"),
    ("Demand",         "pages/4_Demand.py"),
    ("Competitive",    "pages/5_Competitive.py"),
    ("Model Insights", "pages/6_Model_Insights.py"),
]


def render_nav() -> None:
    """Bottom nav using buttons + st.switch_page — session stays alive, no path issues."""
    app_root = Path(__file__).resolve().parents[1]

    st.markdown("""
    <style>
    .block-container { padding-bottom: 1rem; }
    div[data-testid="stHorizontalBlock"] button {
        background: none !important;
        border: none !important;
        color: #757575 !important;
        font-size: 0.84rem !important;
        letter-spacing: 0.05em;
        padding: 0.3rem 0.6rem !important;
        width: 100%;
    }
    div[data-testid="stHorizontalBlock"] button:hover {
        color: #C8522A !important;
        background: none !important;
    }
    </style>
    """, unsafe_allow_html=True)

    st.divider()

    existing = [(label, path) for label, path in PAGES if (app_root / path).exists()]
    cols = st.columns(len(existing))
    for col, (label, path) in zip(cols, existing):
        with col:
            if st.button(label, key=f"nav_{label}", use_container_width=True):
                st.switch_page(path)
