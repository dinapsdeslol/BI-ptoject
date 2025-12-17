import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px  


from pathlib import Path

DB_PATH = Path("output_dw") / "northwind_dw.sqlite"  


def get_connection():
    return sqlite3.connect(DB_PATH)





# data access

@st.cache_data
def load_dim_date():
    with get_connection() as cnx:
        return pd.read_sql("SELECT * FROM dim_date", cnx)


@st.cache_data
def load_fact_joined():
    # on joint toutes les tables pour tout avoir
    query = """
    SELECT
        d.date                AS order_date,
        e.employee_key,
        c.customer_key,
        COALESCE(e.FirstName, '') || ' ' || COALESCE(e.LastName, '') AS employee_name,
        COALESCE(c.CompanyName, '(sans nom)') AS customer_name,
        f.nb_commandes_livrees,
        f.nb_commandes_non_livrees
    FROM fact_orders f
    LEFT JOIN dim_employee e ON f.employee_key = e.employee_key
    LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
    LEFT JOIN dim_date d      ON f.order_date_key = d.date_key
    """
    with get_connection() as cnx:
        df = pd.read_sql(query, cnx, parse_dates=["order_date"])
    return df


def compute_summary(df):
   
    grouped = (
        df.groupby(
            ["order_date", "employee_key", "employee_name", "customer_key", "customer_name"],
            dropna=False,
        )[["nb_commandes_livrees", "nb_commandes_non_livrees"]]
        .sum()
        .reset_index()
    )
    #  calcule le total commandes
    grouped["total_commandes"] = (
        grouped["nb_commandes_livrees"] + grouped["nb_commandes_non_livrees"]
    )
    return grouped




def main():
    st.set_page_config(page_title="Dashboard Northwind DW", layout="wide")

    st.title("Dashboard")

    # load the data
    with st.spinner("chargement des données…"):
        df_dates = load_dim_date()
        df_fact = load_fact_joined()

    if df_fact.empty:
        st.error("aucune donnée. vérifiez l'etl.")
        return






    # filtres



    st.sidebar.header("filtres")

    # filtre par dates
    min_date = df_fact["order_date"].min()
    max_date = df_fact["order_date"].max()

    date_range = st.sidebar.date_input(
        "période de commandes",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    #  filtre pour un seul jour 
    if isinstance(date_range, (list, tuple)):
        start_date = date_range[0]
        end_date = date_range[-1]
    else:
        start_date = end_date = date_range 


    mask_date = (df_fact["order_date"] >= pd.to_datetime(start_date)) & (
        df_fact["order_date"] <= pd.to_datetime(end_date)
    )
    df_filtered = df_fact[mask_date].copy()

    # filtre employé
    employee_list = sorted(df_filtered["employee_name"].dropna().unique())
    employee_filter = st.sidebar.multiselect(
        "employé(s)", options=employee_list, default=employee_list
    )
    if employee_filter:
        df_filtered = df_filtered[df_filtered["employee_name"].isin(employee_filter)]

    # filtre client
    customer_list = sorted(df_filtered["customer_name"].dropna().unique())
    customer_filter = st.sidebar.multiselect(
        "client(s)", options=customer_list, default=customer_list
    )
    if customer_filter:
        df_filtered = df_filtered[df_filtered["customer_name"].isin(customer_filter)]




    # kpis 
    summary = compute_summary(df_filtered)

    total_livrees = int(summary["nb_commandes_livrees"].sum())
    total_non_livrees = int(summary["nb_commandes_non_livrees"].sum())
    total_commandes = int(summary["total_commandes"].sum())

    col1, col2, col3 = st.columns(3)
    col1.metric("📦 total commandes", total_commandes)
    col2.metric("✅ commandes livrées", total_livrees)
    col3.metric("❌ commandes non livrées", total_non_livrees)

    st.markdown("---")

    


    # tableau
    
    summary_sorted = summary.sort_values(
        ["order_date", "employee_name", "customer_name"]
    )
    summary_sorted["order_date"] = summary_sorted["order_date"].dt.date

    st.dataframe(
        summary_sorted[
            [
                "order_date",
                "employee_name",
                "customer_name",
                "nb_commandes_livrees",
                "nb_commandes_non_livrees",
                "total_commandes",
            ]
        ],
        use_container_width=True,
    )

    
    # graph
    st.subheader("répartition client/employé dans le temps")

    if not summary.empty:
        summary_3d = summary.copy()
        # formatage de la date pour l'affichage
        summary_3d['date_str'] = summary_3d['order_date'].dt.strftime('%Y-%m-%d')
        
        #  date (x), employé (y), client (z/couleur)
        fig = px.scatter_3d(
            summary_3d,
            x='order_date',
            y='employee_name',
            z='customer_name',  # le client est l'axe z
            color='customer_name', # couleur selon le client
            hover_data={
                'date_str': True,                       # date (visible)
                'employee_name': True,                  # employé (visible)
                'customer_name': True,                  # client (visible)
                'nb_commandes_livrees': False,          # masqué
                'nb_commandes_non_livrees': False,      # masqué
                'total_commandes': False,               # masqué
                'order_date': False                     # masqué
            },
            labels={
                'order_date': 'date',
                'employee_name': 'employé',
                'customer_name': 'client', 
                'date_str': 'date'
            },
            height=700
        )
        
        fig.update_layout(
            scene=dict(
                xaxis_title='date',
                yaxis_title='employé',
                zaxis_title='client'
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.info("utilisez la souris pour faire tourner le graphe et voir les détails, vous pouvez zoomer aussi")
    else:
        st.info("aucune donnée pour cette sélection de filtres.")


if __name__ == "__main__":
    main()