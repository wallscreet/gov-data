import streamlit as st


# --- Pages Setup ---
landing_page = st.Page(
    page="pages/landing_page.py",
    title="Landing Page",
    icon=":material/home:",
    default=True
)
housing = st.Page(
    page="pages/housing.py",
    title="Housing",
    icon=":material/home:",
    default=False
)
income = st.Page(
    page="pages/income.py",
    title="Income",
    icon=":material/home:",
    default=False
)
cost_living = st.Page(
    page="pages/cost_living.py",
    title="Cost of Living",
    icon=":material/home:",
    default=False
)


pg = st.navigation(
    {
        "Directory": [
            landing_page, 
            housing, 
            income,
            cost_living,
        ],
    }
)

pg.run()
