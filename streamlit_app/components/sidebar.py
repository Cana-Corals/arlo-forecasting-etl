import streamlit as st
from pathlib import Path

_PAGES = [
    ("Home",          "Home.py",             "ti-sparkles",       "ai"),
    ("Dashboard",     "pages/1_Dashboard.py","ti-layout-dashboard","dashboard"),
    ("Forecast",      "pages/2_Forecast.py", "ti-trending-up",    "forecast"),
    ("Performance",   "pages/3_Performance.py","ti-chart-bar",    "performance"),
    ("Demand",        "pages/4_Demand.py",   "ti-calendar-event", "demand"),
    ("Competitive",   "pages/5_Competitive.py","ti-building-store","competitive"),
    ("Model Insights","pages/6_Model_Insights.py","ti-brain",     "model"),
]

_CSS = """
<style>
section[data-testid="stSidebar"] {
  background:#111111 !important;
  min-width:200px !important; width:200px !important;
  transform:none !important; left:0 !important;
  visibility:visible !important; display:block !important;
}
section[data-testid="stSidebar"] > div:first-child {
  padding:0 !important; background:#111111 !important;
}
section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
  gap:0 !important; padding:0 0 16px 0 !important;
}
section[data-testid="stSidebar"] .element-container,
section[data-testid="stSidebar"] .stElementContainer { margin:0 !important; padding:0 !important; }
section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p { margin:0 !important; }

section[data-testid="stSidebar"] .stButton > button {
  background:transparent !important; border:none !important;
  box-shadow:none !important; outline:none !important;
  color:rgba(245,245,240,.6) !important;
  font-size:12px !important; font-weight:400 !important;
  text-align:left !important; justify-content:flex-start !important;
  padding:9px 10px 9px 16px !important; margin:3px 6px !important;
  width:calc(100% - 12px) !important; border-radius:4px !important;
  transition:background .12s !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
  background:rgba(255,255,255,.05) !important;
  color:rgba(245,245,240,1) !important;
}
[data-testid="collapsedControl"] { display:none !important; }
#MainMenu, footer, header { visibility:hidden; }
</style>
"""


def render_sidebar(active: str = "") -> None:
    app_root = Path(__file__).resolve().parents[1]

    st.markdown(_CSS, unsafe_allow_html=True)

    with st.sidebar:
        # Header
        st.markdown("""
        <div style="display:flex;align-items:center;gap:9px;padding:14px 14px 12px;
                    border-bottom:1px solid rgba(255,255,255,.08);">
          <div style="width:28px;height:28px;border-radius:6px;
                      background:rgba(232,133,74,.15);border:1px solid rgba(232,133,74,.3);
                      display:flex;align-items:center;justify-content:center;
                      font-size:11px;font-weight:700;color:#e8854a;flex-shrink:0;">AW</div>
          <div>
            <div style="font-size:11px;font-weight:600;color:rgba(245,245,240,.9);">Arlo Williamsburg</div>
            <div style="font-size:9px;color:rgba(245,245,240,.35);letter-spacing:.06em;">REVENUE INTELLIGENCE</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Overview section
        st.markdown('<div style="padding:10px 16px 6px;font-size:9px;font-weight:600;'
                    'letter-spacing:.18em;text-transform:uppercase;'
                    'color:rgba(255,255,255,.22);">Overview</div>', unsafe_allow_html=True)

        dashboard_path = app_root / "pages/1_Dashboard.py"
        dashboard_label = "● Dashboard" if active == "dashboard" else "Dashboard"
        if st.button(dashboard_label, key="sb_dashboard", use_container_width=True,
                     disabled=not dashboard_path.exists()):
            st.switch_page("pages/1_Dashboard.py")

        # Analytics section
        st.markdown('<div style="padding:10px 16px 6px;font-size:9px;font-weight:600;'
                    'letter-spacing:.18em;text-transform:uppercase;'
                    'color:rgba(255,255,255,.22);">Analytics</div>', unsafe_allow_html=True)

        for label, path, _, key in [
            ("Forecast",    "pages/2_Forecast.py",    "", "forecast"),
            ("Performance", "pages/3_Performance.py", "", "performance"),
            ("Demand",      "pages/4_Demand.py",      "", "demand"),
        ]:
            full_path = app_root / path
            btn_label = f"● {label}" if key == active else label
            if st.button(btn_label, key=f"sb_{key}", use_container_width=True,
                         disabled=not full_path.exists()):
                st.switch_page(path)

        # Intelligence section
        st.markdown('<div style="padding:10px 16px 6px;font-size:9px;font-weight:600;'
                    'letter-spacing:.18em;text-transform:uppercase;'
                    'color:rgba(255,255,255,.22);">Intelligence</div>', unsafe_allow_html=True)

        for label, path, _, key in [
            ("Competitive",   "pages/5_Competitive.py",   "", "competitive"),
            ("Model Insights","pages/6_Model_Insights.py","", "model"),
        ]:
            full_path = app_root / path
            btn_label = f"● {label}" if key == active else label
            if st.button(btn_label, key=f"sb_{key}", use_container_width=True,
                         disabled=not full_path.exists()):
                st.switch_page(path)

        # Dev panel — julio only
        if st.session_state.get("username") == "julio":
            st.markdown('<div style="margin:8px 6px 0;border-top:1px solid rgba(255,255,255,.06);"></div>',
                        unsafe_allow_html=True)
            st.markdown('<div style="padding:10px 16px 4px;font-size:9px;font-weight:600;'
                        'letter-spacing:.18em;text-transform:uppercase;'
                        'color:rgba(255,255,255,.22);">Dev</div>', unsafe_allow_html=True)
            if st.button("Clear Cache", key="sb_clear_cache", use_container_width=True):
                st.cache_data.clear()
                st.success("Cache cleared")

        # Footer — logout
        st.markdown('<div style="margin:12px 6px 0;border-top:1px solid rgba(255,255,255,.06);"></div>',
                    unsafe_allow_html=True)
        if st.button("Logout", key="sb_logout", use_container_width=True):
            for key in ["authentication_status", "name", "username", "logout", "_auth"]:
                st.session_state.pop(key, None)
            st.rerun()
