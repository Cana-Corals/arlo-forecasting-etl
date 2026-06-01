import streamlit as st
from pathlib import Path


def _go_home() -> None:
    """Navigate to home using the Page object stored by Home.py on every run."""
    pg = st.session_state.get("_pg_home")
    if pg is not None:
        st.switch_page(pg)


def render_sidebar(active: str = "") -> None:
    app_root = Path(__file__).resolve().parents[1]

    if "sb_collapsed" not in st.session_state:
        st.session_state["sb_collapsed"] = False

    collapsed = st.session_state["sb_collapsed"]
    sb_w = "52px" if collapsed else "200px"

    name = st.session_state.get("name", "")
    initials = "".join(p[0].upper() for p in name.split()[:2]) if name else "JR"

    st.markdown(f"""
<style>
section[data-testid="stSidebar"] {{
  background:#111111 !important;
  min-width:{sb_w} !important; width:{sb_w} !important;
  transform:none !important; left:0 !important;
  visibility:visible !important; display:block !important;
  transition:width .22s ease; overflow:hidden;
}}
section[data-testid="stSidebar"] > div:first-child {{
  padding:0 !important; background:#111111 !important;
  width:{sb_w} !important; min-width:{sb_w} !important;
  overflow:hidden;
}}
section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {{
  gap:0 !important; padding:0 0 16px 0 !important;
}}
section[data-testid="stSidebar"] .element-container,
section[data-testid="stSidebar"] .stElementContainer {{ margin:0 !important; padding:0 !important; }}
section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {{ margin:0 !important; }}
section[data-testid="stSidebar"] .stButton > button {{
  background:transparent !important; border:none !important;
  box-shadow:none !important; outline:none !important;
  color:rgba(245,245,240,.6) !important;
  font-size:12px !important; font-weight:400 !important;
  text-align:left !important; justify-content:flex-start !important;
  padding:9px 10px 9px 16px !important; margin:3px 6px !important;
  width:calc(100% - 12px) !important; border-radius:4px !important;
  transition:background .12s !important;
}}
section[data-testid="stSidebar"] .stButton > button:hover {{
  background:rgba(255,255,255,.05) !important;
  color:rgba(245,245,240,1) !important;
}}
section[data-testid="stSidebar"] [data-testid="stHorizontalBlock"] {{
  border-bottom:1px solid rgba(255,255,255,.08);
  padding:0 !important; margin:0 !important; gap:0 !important;
}}
section[data-testid="stSidebar"] [data-testid="stHorizontalBlock"] [data-testid="column"] {{
  padding:0 !important; min-width:0 !important;
}}
section[data-testid="stSidebar"] [data-testid="stHorizontalBlock"] .stButton > button {{
  width:100% !important; height:54px !important;
  padding:0 !important; margin:0 !important;
  text-align:center !important; justify-content:center !important;
  font-size:17px !important; letter-spacing:0 !important;
  color:rgba(245,245,240,.38) !important;
  background:transparent !important; border:none !important; border-radius:0 !important;
}}
section[data-testid="stSidebar"] [data-testid="stHorizontalBlock"] .stButton > button:hover {{
  color:rgba(245,245,240,.7) !important; background:rgba(255,255,255,.04) !important;
}}
[data-testid="collapsedControl"] {{ display:none !important; }}
#MainMenu, footer, header {{ visibility:hidden; }}
</style>
""", unsafe_allow_html=True)

    with st.sidebar:
        # ── Header ────────────────────────────────────────────────────────────
        if collapsed:
            st.markdown("""
<div style="display:flex;align-items:center;justify-content:center;
            padding:18px 0 20px;border-bottom:1px solid rgba(255,255,255,.08);">
  <div style="width:32px;height:32px;border-radius:6px;background:#e8854a;
              display:flex;align-items:center;justify-content:center;
              font-size:11px;font-weight:700;color:#fff;letter-spacing:.04em;">AW</div>
</div>
""", unsafe_allow_html=True)
            st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)
            if st.button("☰", key="sb_tog", use_container_width=True):
                st.session_state["sb_collapsed"] = False
                st.rerun()
        else:
            _col_brand, _col_ham = st.columns([4, 1])
            with _col_brand:
                st.markdown("""
<div style="display:flex;align-items:center;gap:9px;padding:14px 14px 12px;">
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
            with _col_ham:
                if st.button("☰", key="sb_tog"):
                    st.session_state["sb_collapsed"] = True
                    st.rerun()

        # ── Nav ───────────────────────────────────────────────────────────────
        if collapsed:
            st.markdown("""<style>
section[data-testid="stSidebar"] .stButton:not(:first-of-type) > button {
    text-align:center !important; justify-content:center !important;
    padding:0 !important; font-size:17px !important;
    height:40px !important; line-height:1 !important; letter-spacing:0 !important;
}
section[data-testid="stSidebar"] .stButton:not(:first-of-type) > button:hover {
    color:#e8854a !important;
    background:rgba(232,133,74,.1) !important;
    border:none !important;
}
</style>""", unsafe_allow_html=True)

            _nav_items = [
                ("✦", "Home",           None),
                (None, None,            None),
                ("◉", "Dashboard",      "pages/1_Dashboard.py"),
                ("↗", "Forecast",       "pages/2_Forecast.py"),
                ("≡", "Performance",    "pages/3_Performance.py"),
                ("◇", "Demand",         "pages/4_Demand.py"),
                ("⊡", "Rooms",          "pages/7_Rooms.py"),
                (None, None,            None),
                ("⊞", "Competitive",    "pages/5_Competitive.py"),
                ("◎", "Model Insights", "pages/6_Model_Insights.py"),
            ]
            for icon, label, path in _nav_items:
                if icon is None:
                    st.markdown('<div style="height:1px;background:rgba(255,255,255,.06);'
                                'margin:4px 10px;"></div>', unsafe_allow_html=True)
                else:
                    if st.button(icon, key=f"sb_nav_{label.replace(' ','_')}",
                                 use_container_width=True, help=label):
                        if path is None:
                            _go_home()
                        else:
                            st.switch_page(path)

            st.markdown(f"""
<div style="margin-top:24px;padding:12px 0;border-top:1px solid rgba(255,255,255,.08);
            display:flex;justify-content:center;">
  <div style="width:28px;height:28px;border-radius:50%;
              background:rgba(232,133,74,.15);border:1px solid rgba(232,133,74,.3);
              display:flex;align-items:center;justify-content:center;
              font-size:11px;font-weight:600;color:#e8854a;">{initials}</div>
</div>
""", unsafe_allow_html=True)

        else:
            # ── Expanded nav ──────────────────────────────────────────────────
            if st.button("← Home", key="sb_home", use_container_width=True):
                _go_home()

            st.markdown('<div style="padding:10px 16px 6px;font-size:9px;font-weight:600;'
                        'letter-spacing:.18em;text-transform:uppercase;'
                        'color:rgba(255,255,255,.22);">Overview</div>', unsafe_allow_html=True)

            dashboard_path = app_root / "pages/1_Dashboard.py"
            dashboard_label = "● Dashboard" if active == "dashboard" else "Dashboard"
            if st.button(dashboard_label, key="sb_dashboard", use_container_width=True,
                         disabled=not dashboard_path.exists()):
                st.switch_page("pages/1_Dashboard.py")

            st.markdown('<div style="padding:10px 16px 6px;font-size:9px;font-weight:600;'
                        'letter-spacing:.18em;text-transform:uppercase;'
                        'color:rgba(255,255,255,.22);">Analytics</div>', unsafe_allow_html=True)

            for label, path, key in [
                ("Forecast",    "pages/2_Forecast.py",    "forecast"),
                ("Performance", "pages/3_Performance.py", "performance"),
                ("Demand",      "pages/4_Demand.py",      "demand"),
                ("Rooms",       "pages/7_Rooms.py",       "rooms"),
            ]:
                full_path = app_root / path
                btn_label = f"● {label}" if key == active else label
                if st.button(btn_label, key=f"sb_{key}", use_container_width=True,
                             disabled=not full_path.exists()):
                    st.switch_page(path)

            st.markdown('<div style="padding:10px 16px 6px;font-size:9px;font-weight:600;'
                        'letter-spacing:.18em;text-transform:uppercase;'
                        'color:rgba(255,255,255,.22);">Intelligence</div>', unsafe_allow_html=True)

            for label, path, key in [
                ("Competitive",   "pages/5_Competitive.py",   "competitive"),
                ("Model Insights","pages/6_Model_Insights.py","model"),
            ]:
                full_path = app_root / path
                btn_label = f"● {label}" if key == active else label
                if st.button(btn_label, key=f"sb_{key}", use_container_width=True,
                             disabled=not full_path.exists()):
                    st.switch_page(path)

            # Footer — logout
            st.markdown('<div style="margin:12px 6px 0;border-top:1px solid rgba(255,255,255,.06);"></div>',
                        unsafe_allow_html=True)
            if st.button("Logout", key="sb_logout", use_container_width=True):
                for k in ["authentication_status", "name", "username", "logout", "_auth"]:
                    st.session_state.pop(k, None)
                st.rerun()
