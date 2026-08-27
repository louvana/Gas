import pandas as pd
import plotly.express as px
import streamlit as st
from db import list_tables, load_table

st.set_page_config(page_title="Visualizations",layout="wide")
st.title("Visualizations")

tables = list_tables()
if not tables:
    st.warning("No tables found.")
    st.stop()

table = st.selectbox("Table", tables)
df = load_table(table, limit=20000)  

if df.empty:
    st.warning("This table has no rows.")
    st.stop()

numeric_cols = df.select_dtypes(include="number").columns.tolist()
datetime_cols = df.select_dtypes(include="datetime").columns.tolist()

for c in df.columns:
    if c not in datetime_cols and c not in numeric_cols:
        try:
            pd.to_datetime(df[c].dropna().head(20))
            datetime_cols.append(c)
        except Exception:
            pass
categorical_cols = [c for c in df.columns if c not in numeric_cols and c not in datetime_cols]

chart_type = st.selectbox(
    "Chart type",
    ["Histogram", "Bar (count by category)", "Line (time series)", "Scatter (two numeric columns)", "Box plot"],
)

if chart_type == "Histogram":
    col = st.selectbox("Numeric column", numeric_cols)
    if col:
        fig = px.histogram(df, x=col, title=f"Distribution of {col}")
        st.plotly_chart(fig, use_container_width=True)

elif chart_type == "Bar (count by category)":
    col = st.selectbox("Categorical column", categorical_cols)
    if col:
        counts = df[col].value_counts().head(30).reset_index()
        counts.columns = [col, "count"]
        fig = px.bar(counts, x=col, y="count", title=f"Top values in {col}")
        st.plotly_chart(fig, use_container_width=True)

elif chart_type == "Line (time series)":
    if not datetime_cols:
        st.info("No date-like column detected in this table.")
    else:
        date_col = st.selectbox("Date column", datetime_cols)
        value_col = st.selectbox("Value column", numeric_cols) if numeric_cols else None
        if value_col:
            tmp = df.copy()
            tmp[date_col] = pd.to_datetime(tmp[date_col])
            tmp = tmp.sort_values(date_col)
            fig = px.line(tmp, x=date_col, y=value_col, title=f"{value_col} over time")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No numeric column available to plot on the y-axis.")

elif chart_type == "Scatter (two numeric columns)":
    if len(numeric_cols) < 2:
        st.info("Need at least two numeric columns for a scatter plot.")
    else:
        x_col = st.selectbox("X axis", numeric_cols, index=0)
        y_col = st.selectbox("Y axis", numeric_cols, index=1)
        color_col = st.selectbox("Color by (optional)", ["None"] + categorical_cols)
        fig = px.scatter(
            df, x=x_col, y=y_col,
            color=None if color_col == "None" else color_col,
            title=f"{y_col} vs {x_col}",
        )
        st.plotly_chart(fig, use_container_width=True)

elif chart_type == "Box plot":
    num_col = st.selectbox("Numeric column", numeric_cols)
    cat_col = st.selectbox("Group by (optional)", ["None"] + categorical_cols)
    fig = px.box(df, x=None if cat_col == "None" else cat_col, y=num_col)
    st.plotly_chart(fig, use_container_width=True)
