import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(layout="wide")

st.title("📊 DSD8 ETL Dashboard (Task-Based)")

# Upload
uploaded_file = st.sidebar.file_uploader("Upload CSV", type=["csv"])

if uploaded_file is None:
    st.warning("Upload dataset to start")
    st.stop()

df = pd.read_csv(uploaded_file)

# Sidebar options (map your 35 questions here)
task = st.sidebar.selectbox("📌 Select ETL Task", [
    "View Raw Data",
    "Remove Duplicates",
    "Handle Missing Values",
    "Convert Date Column",
    "Filter Data (Cases > 1000)",
    "Sort Data",
    "Group By State (Max Cases)",
    "Top 5 States",
    "Total Cases",
    "Cases Over Time"
])

st.subheader(f"🔍 {task}")

# -------- TASK IMPLEMENTATIONS -------- #

if task == "View Raw Data":
    st.write("👉 Displays original dataset")
    st.dataframe(df)

elif task == "Remove Duplicates":
    st.write("👉 Removes duplicate rows")
    before = len(df)
    df_clean = df.drop_duplicates()
    after = len(df_clean)
    st.write(f"Rows before: {before}, after: {after}")
    st.dataframe(df_clean.head())

elif task == "Handle Missing Values":
    st.write("👉 Fills missing values with 0")
    df_filled = df.fillna(0)
    st.dataframe(df_filled.head())

elif task == "Convert Date Column":
    st.write("👉 Converts 'date' to datetime format")
    df['date'] = pd.to_datetime(df['date'])
    st.write(df.dtypes)

elif task == "Filter Data (Cases > 1000)":
    st.write("👉 Filters rows where cases > 1000")
    filtered = df[df['cases'] > 1000]
    st.dataframe(filtered.head())

elif task == "Sort Data":
    st.write("👉 Sort by cases (descending)")
    sorted_df = df.sort_values(by='cases', ascending=False)
    st.dataframe(sorted_df.head())

elif task == "Group By State (Max Cases)":
    st.write("👉 Maximum cases per state")
    grouped = df.groupby('state')['cases'].max()
    st.dataframe(grouped)

elif task == "Top 5 States":
    st.write("👉 Top 5 states by cases")
    top = df.groupby('state')['cases'].max().sort_values(ascending=False).head(5)
    st.dataframe(top)

elif task == "Total Cases":
    total = df['cases'].sum()
    st.metric("Total Cases", total)

elif task == "Cases Over Time":
    st.write("👉 Trend of cases over time")
    df['date'] = pd.to_datetime(df['date'])
    df_sorted = df.sort_values('date')

    fig, ax = plt.subplots()
    df_sorted.plot(x='date', y='cases', ax=ax)
    st.pyplot(fig)
