import streamlit as st


PAGES = [
    ("Home",          "Home.py"),
    ("Dashboard",     "pages/1_Dashboard.py"),
    ("Forecast",      "pages/2_Forecast.py"),
    ("Performance",   "pages/3_Performance.py"),
    ("Demand",        "pages/4_Demand.py"),
    ("Competitive",   "pages/5_Competitive.py"),
    ("Model Insights","pages/6_Model_Insights.py"),
]


def render_nav() -> None:
    """Bottom nav bar using st.page_link — keeps Streamlit session alive across pages."""
    from pathlib import Path

    app_root = Path(__file__).resolve().parents[1]

    st.markdown("""
    <style>
    .block-container { padding-bottom: 4rem; }
    [data-testid="stPageLink"] a {
        font-size: 0.84rem !important;
        color: #757575 !important;
        text-decoration: none !important;
        letter-spacing: 0.05em;
    }
    [data-testid="stPageLink"] a:hover { color: #C8522A !important; }
    </style>
    """, unsafe_allow_html=True)

    st.divider()

    existing = [(label, path) for label, path in PAGES if (app_root / path).exists()]
    cols = st.columns(len(existing))
    for col, (label, path) in zip(cols, existing):
        with col:
            st.page_link(path, label=label)
