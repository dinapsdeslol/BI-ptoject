import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DB_PATH = PROJECT_ROOT / "data" / "final" / "northwind_dw.sqlite"



def get_connection():
    return sqlite3.connect(str(DB_PATH))


@st.cache_data
def load_fact_joined():
    query = """
    SELECT
        d.date AS order_date,
        e.employee_key,
        c.customer_key,
        TRIM(COALESCE(e.FirstName, '') || ' ' || COALESCE(e.LastName, '')) AS employee_name,
        COALESCE(c.CompanyName, '(sans nom)') AS customer_name,
        COALESCE(e.RegionDescription, '(sans région)') AS region,
        f.nb_commandes_livrees,
        f.nb_commandes_non_livrees
    FROM fact_orders f
    LEFT JOIN dim_employee e ON f.employee_key = e.employee_key
    LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
    LEFT JOIN dim_date d      ON f.order_date_key = d.date_key
    """
    with get_connection() as cnx:
        df = pd.read_sql(query, cnx)

    # IMPORTANT : order_date doit être datetime
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df.dropna(subset=["order_date"]).copy()

    df["employee_name"] = df["employee_name"].fillna("").str.strip()
    df.loc[df["employee_name"] == "", "employee_name"] = "(employé inconnu)"

    df["region"] = df["region"].fillna("(sans région)").astype(str)

    # s'assure que les kpis sont numériques
    df["nb_commandes_livrees"] = pd.to_numeric(df["nb_commandes_livrees"], errors="coerce").fillna(0).astype(int)
    df["nb_commandes_non_livrees"] = pd.to_numeric(df["nb_commandes_non_livrees"], errors="coerce").fillna(0).astype(int)

    return df


def compute_summary(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby(
            ["order_date", "employee_key", "employee_name", "customer_key", "customer_name", "region"],
            dropna=False,
        )[["nb_commandes_livrees", "nb_commandes_non_livrees"]]
        .sum()
        .reset_index()
    )
    grouped["total_commandes"] = grouped["nb_commandes_livrees"] + grouped["nb_commandes_non_livrees"]
    return grouped


def main():
    st.set_page_config(page_title="Dashboard Northwind DW", layout="wide")
    st.title("Dashboard")

    if not DB_PATH.exists():
        st.error(f"Base SQLite introuvable : {DB_PATH.resolve()}\n\nLance d'abord l'ETL.")
        return

    with st.spinner("chargement des données…"):
        df_fact = load_fact_joined()

    if df_fact.empty:
        st.error("aucune donnée. vérifiez l'etl.")
        return

    st.sidebar.header("filtres")

    # filtre dates
    min_date = df_fact["order_date"].min().date()
    max_date = df_fact["order_date"].max().date()

    date_range = st.sidebar.date_input(
        "période de commandes",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if isinstance(date_range, (list, tuple)):
        start_date = date_range[0]
        end_date = date_range[-1]
    else:
        start_date = end_date = date_range

    mask_date = (df_fact["order_date"] >= pd.to_datetime(start_date)) & (
        df_fact["order_date"] <= pd.to_datetime(end_date)
    )
    df_filtered = df_fact[mask_date].copy()

    # filtre employés
    employee_list = sorted(df_filtered["employee_name"].dropna().unique())
    employee_filter = st.sidebar.multiselect("employé(s)", options=employee_list, default=employee_list)
    if employee_filter:
        df_filtered = df_filtered[df_filtered["employee_name"].isin(employee_filter)]

    

    summary = compute_summary(df_filtered)
    if summary.empty:
        st.info("aucune donnée pour cette sélection.")
        return

    # KPIs
    total_livrees = int(summary["nb_commandes_livrees"].sum())
    total_non_livrees = int(summary["nb_commandes_non_livrees"].sum())
    total_commandes = int(summary["total_commandes"].sum())

    c1, c2, c3 = st.columns(3)
    c1.metric("total commandes", total_commandes)
    c2.metric("commandes livrées", total_livrees)
    c3.metric("commandes non livrées", total_non_livrees)

    st.markdown("---")

    # =====================================================
    # TABLEAU D'ABORD
    # =====================================================
    st.subheader("détail période x employé x client")

    detail = summary.sort_values(["order_date", "employee_name", "customer_name"]).copy()
    detail["order_date"] = pd.to_datetime(detail["order_date"]).dt.date

    st.dataframe(
        detail[
            [
                "order_date",
                "employee_name",
                "customer_name",
                "region",
                "nb_commandes_livrees",
                "nb_commandes_non_livrees",
                "total_commandes",
            ]
        ],
        use_container_width=True,
    )

    st.markdown("---")

    # =====================================================
    # GRAPHE 3D 
    # =====================================================
    st.subheader("analyse 3d : période x employé x client")

    summary_3d = summary.copy()
    summary_3d["date_str"] = pd.to_datetime(summary_3d["order_date"]).dt.strftime("%Y-%m-%d")

    fig_3d = px.scatter_3d(
        summary_3d,
        x="order_date",
        y="employee_name",
        z="customer_name",
        color="customer_name",
        hover_data={
            "date_str": True,
            "employee_name": True,
            "customer_name": True,
            "region": True,
            "nb_commandes_livrees": True,
            "nb_commandes_non_livrees": True,
            "total_commandes": True,
            "order_date": False,
        },
        labels={
            "order_date": "date",
            "employee_name": "employé",
            "customer_name": "client",
            "region": "région",
            "date_str": "date",
        },
        height=700,
    )

 
    fig_3d.update_traces(marker=dict(size=3))

    fig_3d.update_layout(
        scene=dict(
            xaxis_title="date",
            yaxis_title="employé",
            zaxis_title="client",
        )
    )

    st.plotly_chart(fig_3d, use_container_width=True)

    st.markdown("---")

    # =====================================================
    # GRAPHE  : 
    # =====================================================
    st.subheader("volume de commandes par mois")

    monthly = summary.copy()
    monthly["year_month"] = pd.to_datetime(monthly["order_date"]).dt.to_period("M").dt.to_timestamp()

    monthly_agg = (
        monthly.groupby("year_month", as_index=False)["total_commandes"]
        .sum()
        .sort_values("year_month")
    )

    fig_month = px.line(
        monthly_agg,
        x="year_month",
        y="total_commandes",
        labels={"year_month": "mois", "total_commandes": "volume de commandes"},
        height=350,
    )
    fig_month.update_traces(mode="lines+markers")
    st.plotly_chart(fig_month, use_container_width=True)

    st.markdown("---")


if __name__ == "__main__":
    main()
