import pandas as pd
import sqlite3
import pyodbc
import numpy as np
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent

SQLITE_DB_PATH = PROJECT_ROOT / "data" / "final" / "northwind_dw.sqlite"

SQLSERVER_SERVER = r"localhost\SQLEXPRESS"
SQLSERVER_DATABASE = "Northwind_DWH"

TARGET_TABLE = "FactOrders_Final"

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

SQL_SCRIPT_PATH = SCRIPTS_DIR / "Fact_Orders_Insert.sql"


if not SQLITE_DB_PATH.exists():
    print(f" SQLite introuvable : {SQLITE_DB_PATH.resolve()}")
    exit(1)

try:
    conn_sqlite = sqlite3.connect(SQLITE_DB_PATH)
    df = pd.read_sql("SELECT * FROM fact_orders", conn_sqlite)
    conn_sqlite.close()
    print(f" Données lues depuis SQLite : {len(df)} lignes")
except Exception as e:
    print(f" Erreur lecture SQLite : {e}")
    exit(1)


for col, dtype in df.dtypes.items():
    if np.issubdtype(dtype, np.integer) or np.issubdtype(dtype, np.floating):
        df[col] = df[col].replace({np.nan: None})
    elif np.issubdtype(dtype, np.datetime64):
        df[col] = df[col].apply(
            lambda x: x if pd.isna(x) or pd.Timestamp("1753-01-01") <= x <= pd.Timestamp("9999-12-31") else None
        )
    else:
        df[col] = df[col].astype(str).replace({"nan": None, "None": None})


try:
    conn_str = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        rf"SERVER={SQLSERVER_SERVER};"
        rf"DATABASE={SQLSERVER_DATABASE};"
        r"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute(f"IF OBJECT_ID('{TARGET_TABLE}', 'U') IS NOT NULL DROP TABLE {TARGET_TABLE};")

    # Création dynamique de la table
    columns_sql = []
    for col, dtype in df.dtypes.items():
        if np.issubdtype(dtype, np.integer):
            columns_sql.append(f"[{col}] INT")
        elif np.issubdtype(dtype, np.floating):
            columns_sql.append(f"[{col}] FLOAT")
        elif np.issubdtype(dtype, np.datetime64):
            columns_sql.append(f"[{col}] DATETIME2")
        else:
            columns_sql.append(f"[{col}] NVARCHAR(MAX)")

    create_table_sql = f"CREATE TABLE {TARGET_TABLE} ({', '.join(columns_sql)});"
    cursor.execute(create_table_sql)

    # Insertion rapide
    cursor.fast_executemany = True
    cols = ", ".join(f"[{c}]" for c in df.columns)
    placeholders = ", ".join("?" for _ in df.columns)
    insert_sql = f"INSERT INTO {TARGET_TABLE} ({cols}) VALUES ({placeholders})"

    cursor.executemany(insert_sql, df.values.tolist())
    conn.commit()

    print(f" Données chargées dans SQL Server : {TARGET_TABLE}")

    cursor.close()
    conn.close()

except Exception as e:
    print(f" Erreur SQL Server : {e}")
    exit(1)


with open(SQL_SCRIPT_PATH, "w", encoding="utf-8") as f:
    f.write(f"USE {SQLSERVER_DATABASE};\nGO\n\n")

    for _, row in df.iterrows():
        clean_values = []
        for v in row:
            if v is None or str(v).lower() == "nan":
                clean_values.append("NULL")
            else:
                val_escaped = str(v).replace("'", "''")
                clean_values.append(f"'{val_escaped}'")

        values_str = ", ".join(clean_values)
        f.write(f"INSERT INTO {TARGET_TABLE} VALUES ({values_str});\n")

print(f" Script SQL généré : {SQL_SCRIPT_PATH.resolve()}")
