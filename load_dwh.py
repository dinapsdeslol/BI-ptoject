import pandas as pd
import sqlite3
import pyodbc
import numpy as np
from pathlib import Path


# ============================
# CONFIGURATION (ALIGNÉE AVEC TON CODE)
# ============================

# même structure que ton ETL
SQLITE_DB = Path("data") / "final" / "northwind_dw.sqlite"

# même instance SQL Server que tu utilises
SQL_SERVER = r"localhost\SQLEXPRESS"

# même nom que :
# CREATE DATABASE northwind_dwh;
SQL_DATABASE = "northwind_dwh"

# table cible inchangée
TARGET_TABLE = "FactOrders_Final_V2"


# ============================
# CONNEXIONS
# ============================
def connect_sqlite():
    return sqlite3.connect(SQLITE_DB)


def connect_sqlserver():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


# ============================
# CREATE TABLE
# ============================
def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    if pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME2"
    return "NVARCHAR(MAX)"


def create_table(cursor, df, table_name):
    columns = []
    for col in df.columns:
        sql_type = map_dtype(df[col].dtype)
        columns.append(f"[{col}] {sql_type} NULL")

    sql = f"""
    IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        DROP TABLE [{table_name}];

    CREATE TABLE [{table_name}] (
        {', '.join(columns)}
    );
    """
    cursor.execute(sql)
    print(f"✅ Table créée : {table_name}")


# ============================
# INSERT DATA
# ============================
def insert_data(cursor, df, table_name):
    if df.empty:
        print("⚠️ Aucune donnée à insérer")
        return

    cols = ", ".join(f"[{c}]" for c in df.columns)
    placeholders = ", ".join("?" for _ in df.columns)
    sql = f"INSERT INTO [{table_name}] ({cols}) VALUES ({placeholders})"

    cursor.fast_executemany = True
    cursor.executemany(sql, df.where(pd.notna(df), None).values.tolist())
    print(f"✅ {len(df)} lignes insérées dans {table_name}")


# ============================
# MAIN
# ============================
def main():
    if not SQLITE_DB.exists():
        raise FileNotFoundError(f"SQLite introuvable : {SQLITE_DB.resolve()}")

    print("📥 Lecture SQLite...")
    sqlite_conn = connect_sqlite()
    df = pd.read_sql("SELECT * FROM fact_orders", sqlite_conn)
    sqlite_conn.close()

    # parsing dates si présentes
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="ignore")

    print("📤 Connexion SQL Server...")
    conn = connect_sqlserver()
    cursor = conn.cursor()

    create_table(cursor, df, TARGET_TABLE)
    insert_data(cursor, df, TARGET_TABLE)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"🎉 Données transférées vers SQL Server → {SQL_DATABASE}.{TARGET_TABLE}")


if __name__ == "__main__":
    main()
