import streamlit as st
from db import list_tables, load_table

st.set_page_config(page_title="Table Explorer",  layout="wide")
st.title("Table Explorer")

tables = list_tables()
if not tables:
    st.warning("No tables found.")
    st.stop()

table = st.selectbox("Choose a table", tables)

limit_choice = st.radio(
    "Rows to load",
    ["First 1,000", "First 10,000", "All rows"],
    horizontal=True,
    help="Loading 'All rows' on a very large table can be slow — start small.",
)
limit_map = {"First 1,000": 1000, "First 10,000": 10000, "All rows": None}
limit = limit_map[limit_choice]

df = load_table(table, limit=limit)

st.caption(f"Showing {len(df):,} rows × {df.shape[1]} columns")

selected_cols = st.multiselect("Columns to display", options=list(df.columns), default=list(df.columns))
st.dataframe(df[selected_cols], use_container_width=True, height=500)

st.divider()
st.subheader("Export")

c1, c2 = st.columns(2)
with c1:
    st.download_button(
        "⬇ Download as CSV",
        data=df[selected_cols].to_csv(index=False).encode("utf-8"),
        file_name=f"{table}.csv",
        mime="text/csv",
        use_container_width=True,
    )
with c2:
    st.download_button(
        "⬇Download as JSON",
        data=df[selected_cols].to_json(orient="records", indent=2).encode("utf-8"),
        file_name=f"{table}.json",
        mime="application/json",
        use_container_width=True,
    )

with st.expander("Run a custom SQL query instead"):
    st.caption("Advanced: query across joins, filters, aggregates, etc.")
    custom_sql = st.text_area("SQL", value=f'SELECT * FROM "{table}" LIMIT 100')
    if st.button("Run query"):
        from db import run_query
        try:
            result = run_query(custom_sql)
            st.dataframe(result, use_container_width=True)
        except Exception as e:
            st.error(f"Query failed: {e}")
