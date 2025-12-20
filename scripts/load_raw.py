import pandas as pd
import pyodbc
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()


if CURRENT_FILE.parent.name == "scripts":
    PROJECT_ROOT = CURRENT_FILE.parent.parent
else:
    PROJECT_ROOT = CURRENT_FILE.parent

print(f" Racine du projet détectée : {PROJECT_ROOT}")

RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

print(f"Dossier RAW : {RAW_DIR.resolve()}")


SQLSERVER_SERVER = r"localhost\SQLEXPRESS"
SQLSERVER_DATABASE = "Northwind"


SQLSERVER_TABLES = [
    "Customers",
    "Orders",
    "[Order Details]",
    "Employees",
    "Categories",
    "Shippers",
    "Suppliers",
    "Products",
]


# ==================================================
# CONNEXION SQL SERVER
# ==================================================
def connect_sqlserver():
    conn_str = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        rf"SERVER={SQLSERVER_SERVER};"
        rf"DATABASE={SQLSERVER_DATABASE};"
        r"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)



def extract_sqlserver_to_raw():
    """
    Exporte les tables SQL Server vers data/raw/
    (couche RAW du projet BI)
    """
    print(f"Export RAW SQL Server → {RAW_DIR.resolve()}")

    try:
        conn = connect_sqlserver()
        print(" Connexion SQL Server réussie")
    except Exception as e:
        print(f" Erreur de connexion SQL Server : {e}")
        return

    for table in SQLSERVER_TABLES:
        try:
            
            file_name = (
                table.replace("[", "")
                .replace("]", "")
                .replace(" ", "_")
                .lower()
            )

            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, conn)

            output_file = RAW_DIR / f"sqlserver_{file_name}.csv"
            df.to_csv(output_file, index=False, encoding="utf-8")

            print(f"  ✓ {table} → {output_file.name} ({len(df)} lignes)")

        except Exception as e:
            print(f"   Erreur sur {table} : {e}")

    conn.close()
    print("\nExport RAW SQL Server terminé")


# ==================================================
# MAIN
# ==================================================
if __name__ == "__main__":
    extract_sqlserver_to_raw()