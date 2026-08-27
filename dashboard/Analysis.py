import plotly.express as px
import streamlit as st
from db import list_tables, load_table

st.set_page_config(page_title="Analysis", page_icon="🔍", layout="wide")
st.title("🔍 Analysis")

tables = list_tables()
if not tables:
    st.warning("No tables found.")
    st.stop()

table = st.selectbox("Table", tables)
df = load_table(table, limit=50000)

if df.empty:
    st.warning("This table has no rows.")
    st.stop()

tab1, tab2, tab3 = st.tabs(["Summary stats", "Missing values", "Correlation"])

with tab1:
    st.subheader("Descriptive statistics")
    st.dataframe(df.describe(include="all").transpose(), use_container_width=True)

    st.subheader("Column types")
    dtype_df = df.dtypes.reset_index()
    dtype_df.columns = ["column", "dtype"]
    st.dataframe(dtype_df, use_container_width=True)

with tab2:
    st.subheader("Missing values per column")
    missing = df.isna().sum().reset_index()
    missing.columns = ["column", "missing_count"]
    missing["missing_pct"] = (missing["missing_count"] / len(df) * 100).round(2)
    missing = missing.sort_values("missing_count", ascending=False)

    if missing["missing_count"].sum() == 0:
        st.success("No missing values in this table 🎉 — good sign for scraped data.")
    else:
        fig = px.bar(missing, x="column", y="missing_pct", title="% missing by column")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(missing, use_container_width=True)

    st.subheader("Duplicate rows")
    dup_count = df.duplicated().sum()
    st.metric("Exact duplicate rows", f"{dup_count:,}")
    if dup_count > 0:
        st.caption("Worth checking your ETL 'clean' step — duplicates often mean re-scraped pages.")

with tab3:
    numeric_df = df.select_dtypes(include="number")
    if numeric_df.shape[1] < 2:
        st.info("Need at least two numeric columns to compute correlations.")
    else:
        st.subheader("Correlation heatmap")
        corr = numeric_df.corr(numeric_only=True)
        fig = px.imshow(corr, text_auto=".2f", aspect="auto", color_continuous_scale="RdBu_r", zmin=-1, zmax=1)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Strong correlations (near ±1) are useful — and risky — features for a later prediction model.")
