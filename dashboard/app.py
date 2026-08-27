import streamlit as st
from db import test_connection, list_tables, get_row_count

st.set_page_config(
    page_title="Database Dashboard",
    layout="wide",
)

st.title("Gasoil prices Dashboard")
st.caption("Explore, export, visualize and analyze the tables containing gasoil prices.")

ok, msg = test_connection()

if not ok:
    st.error(f" Could not connect to the database.\n\n**Details:** {msg}")
    st.info(
        "Check that:\n"
        "- The `postgres` service is running and healthy (`docker compose ps`)\n"
        "- Your `.env` values match the ones in your main docker-compose file\n"
        "- `POSTGRES_HOST` is the **service name** (e.g. `postgres`), not `localhost`, "
        "since this app runs inside its own container"
    )
    st.stop()

st.success(" Connected to PostgreSQL")

tables = list_tables()

if not tables:
    st.warning("Connected, but no tables found in the `public` schema yet. Has the ETL load step run?")
    st.stop()

st.subheader(f"Tables found: {len(tables)}")

cols = st.columns(3)
for i, t in enumerate(tables):
    with cols[i % 3]:
        with st.container(border=True):
            st.markdown(f"**{t}**")
            try:
                st.metric("Rows", f"{get_row_count(t):,}")
            except Exception as e:
                st.caption(f"Could not count rows: {e}")

st.divider()
st.markdown(
    " Use the sidebar: **Table Explorer** to browse/export as CSV or JSON, "
    "**Visualizations** to chart columns, **Analysis** for stats and correlations."
)
