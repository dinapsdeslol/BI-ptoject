import pyodbc
import pandas as pd
from pathlib import Path
import sqlite3


# =========================
# CONFIG 
# =========================
ACCESS_DB_PATH = r"C:\Users\dinaz\Downloads\Northwind 2012 (1).accdb"  # à adapter si besoin
SQLSERVER_SERVER = r"localhost\SQLEXPRESS"
SQLSERVER_DB = "Northwind"



BASE_DIR = Path("data")
PROCESSED_DIR = BASE_DIR / "processed"
FINAL_DIR = BASE_DIR / "final"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
FINAL_DIR.mkdir(parents=True, exist_ok=True)


EXCEL_OUTPUT = FINAL_DIR / "northwind_dw.xlsx"
DW_DB_PATH = FINAL_DIR / "northwind_dw.sqlite"


# =========================
# CONNECTIONS
# =========================
def conn_access():
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={ACCESS_DB_PATH};"
    )
    return pyodbc.connect(conn_str)


def conn_sqlserver():
    conn_str = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        rf"SERVER={SQLSERVER_SERVER};"
        rf"DATABASE={SQLSERVER_DB};"
        r"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


# =========================
# EXTRACT
# =========================
def extract_access():
    cnx = conn_access()

    df_emp = pd.read_sql("SELECT * FROM Employees", cnx)
    df_cust = pd.read_sql("SELECT * FROM Customers", cnx)
    df_orders = pd.read_sql("SELECT * FROM Orders", cnx)

    # si dispo dans ce fichier Access
    try:
        df_region = pd.read_sql("SELECT * FROM Region", cnx)
        df_territories = pd.read_sql("SELECT * FROM Territories", cnx)
        df_emp_terr = pd.read_sql("SELECT * FROM EmployeeTerritories", cnx)
    except Exception:
        df_region = pd.DataFrame()
        df_territories = pd.DataFrame()
        df_emp_terr = pd.DataFrame()

    cnx.close()

    return {
        "employees": df_emp,
        "customers": df_cust,
        "orders": df_orders,
        "region": df_region,
        "territories": df_territories,
        "emp_terr": df_emp_terr,
    }


def extract_sqlserver():
    cnx = conn_sqlserver()

    df_emp = pd.read_sql("SELECT * FROM Employees", cnx)
    df_cust = pd.read_sql("SELECT * FROM Customers", cnx)
    df_orders = pd.read_sql("SELECT * FROM Orders", cnx)

    cnx.close()

    return {
        "employees": df_emp,
        "customers": df_cust,
        "orders": df_orders,
    }


# =========================
# TRANSFORM 
# =========================
def build_dim_employee(access_data, sql_data):
    emp_a = access_data["employees"].copy()
    emp_a["source_system"] = "access"

    if "EmployeeID" in emp_a.columns:
        id_col_a = "EmployeeID"
    elif "ID" in emp_a.columns:
        id_col_a = "ID"
    else:
        raise KeyError("pas de colonne id/employeeid trouvée dans access")

    if "LastName" in emp_a.columns:
        ln_col_a = "LastName"
    elif "Last Name" in emp_a.columns:
        ln_col_a = "Last Name"
    else:
        ln_col_a = None

    if "FirstName" in emp_a.columns:
        fn_col_a = "FirstName"
    elif "First Name" in emp_a.columns:
        fn_col_a = "First Name"
    else:
        fn_col_a = None

    rename_a = {id_col_a: "employee_id_source"}
    if ln_col_a:
        rename_a[ln_col_a] = "LastName"
    if fn_col_a:
        rename_a[fn_col_a] = "FirstName"

    emp_a = emp_a.rename(columns=rename_a)

    for col in ["LastName", "FirstName", "Title", "City", "Country"]:
        if col not in emp_a.columns:
            emp_a[col] = None

    emp_s = sql_data["employees"].copy()
    emp_s["source_system"] = "sqlserver"

    if "EmployeeID" not in emp_s.columns:
        raise KeyError("pas de colonne employeeid trouvée dans sql server")

    emp_s["employee_id_source"] = emp_s["EmployeeID"]

    for col in ["LastName", "FirstName", "Title", "City", "Country"]:
        if col not in emp_s.columns:
            emp_s[col] = None

    # petit champ en plus (si tu veux)
    emp_a["RegionDescription"] = None
    emp_s["RegionDescription"] = emp_s.get("Region", None)

    cols = [
        "source_system",
        "employee_id_source",
        "LastName",
        "FirstName",
        "Title",
        "City",
        "Country",
        "RegionDescription",
    ]

    dim_emp = pd.concat([emp_a[cols], emp_s[cols]], ignore_index=True).drop_duplicates()
    dim_emp.insert(0, "employee_key", range(1, len(dim_emp) + 1))
    return dim_emp


def build_dim_customer(access_data, sql_data):
    cust_a = access_data["customers"].copy()
    cust_a["source_system"] = "access"

    if "CustomerID" in cust_a.columns:
        id_col_a = "CustomerID"
    elif "ID" in cust_a.columns:
        id_col_a = "ID"
    else:
        raise KeyError("pas de colonne id/customerid trouvée dans access")

    if "CompanyName" in cust_a.columns:
        comp_col_a = "CompanyName"
    elif "Company" in cust_a.columns:
        comp_col_a = "Company"
    else:
        comp_col_a = None

    if "ContactName" in cust_a.columns:
        contact_col_a = "ContactName"
    elif "Contact Name" in cust_a.columns:
        contact_col_a = "Contact Name"
    else:
        contact_col_a = None

    rename_a = {id_col_a: "customer_id_source"}
    if comp_col_a:
        rename_a[comp_col_a] = "CompanyName"
    if contact_col_a:
        rename_a[contact_col_a] = "ContactName"

    cust_a = cust_a.rename(columns=rename_a)

    for col in ["CompanyName", "ContactName", "City", "Country", "PostalCode", "Address", "Phone"]:
        if col not in cust_a.columns:
            cust_a[col] = None

    cust_s = sql_data["customers"].copy()
    cust_s["source_system"] = "sqlserver"

    if "CustomerID" not in cust_s.columns:
        raise KeyError("pas de colonne customerid trouvée dans sql server")

    cust_s["customer_id_source"] = cust_s["CustomerID"]

    for col in ["CompanyName", "ContactName", "City", "Country", "PostalCode", "Address", "Phone"]:
        if col not in cust_s.columns:
            cust_s[col] = None

    cols = [
        "source_system",
        "customer_id_source",
        "CompanyName",
        "ContactName",
        "City",
        "Country",
        "PostalCode",
        "Address",
        "Phone",
    ]

    dim_cust = pd.concat([cust_a[cols], cust_s[cols]], ignore_index=True).drop_duplicates()
    dim_cust.insert(0, "customer_key", range(1, len(dim_cust) + 1))
    return dim_cust


def build_dim_date(start="1996-01-01", end="2030-12-31"):
    dates = pd.date_range(start=start, end=end, freq="D")
    dim_date = pd.DataFrame({"date": dates})

    dim_date["date_key"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["month_name"] = dim_date["date"].dt.month_name()
    dim_date["day_of_week"] = dim_date["date"].dt.day_name()
    dim_date["is_weekend"] = dim_date["day_of_week"].isin(["Saturday", "Sunday"])

    return dim_date[
        ["date_key", "date", "year", "month", "day", "month_name", "day_of_week", "is_weekend"]
    ]


# =========================
# TRANSFORM (fact)
# =========================
def build_fact_orders(access_data, sql_data, dim_emp, dim_cust, dim_date):
    ord_a = access_data["orders"].copy()
    ord_a["source_system"] = "access"

    if "OrderID" in ord_a.columns:
        order_id_col_a = "OrderID"
    elif "Order ID" in ord_a.columns:
        order_id_col_a = "Order ID"
    else:
        raise KeyError("pas de orderid/order id trouvée dans orders (access)")

    if "EmployeeID" in ord_a.columns:
        emp_id_col_a = "EmployeeID"
    elif "Employee ID" in ord_a.columns:
        emp_id_col_a = "Employee ID"
    else:
        emp_id_col_a = None

    if "CustomerID" in ord_a.columns:
        cust_id_col_a = "CustomerID"
    elif "Customer ID" in ord_a.columns:
        cust_id_col_a = "Customer ID"
    else:
        cust_id_col_a = None

    if "OrderDate" in ord_a.columns:
        order_date_col_a = "OrderDate"
    elif "Order Date" in ord_a.columns:
        order_date_col_a = "Order Date"
    else:
        order_date_col_a = None

    if "ShippedDate" in ord_a.columns:
        ship_date_col_a = "ShippedDate"
    elif "Shipped Date" in ord_a.columns:
        ship_date_col_a = "Shipped Date"
    else:
        ship_date_col_a = None

    rename_a = {order_id_col_a: "order_id_source"}
    if emp_id_col_a:
        rename_a[emp_id_col_a] = "EmployeeID"
    if cust_id_col_a:
        rename_a[cust_id_col_a] = "CustomerID"
    if order_date_col_a:
        rename_a[order_date_col_a] = "OrderDate"
    if ship_date_col_a:
        rename_a[ship_date_col_a] = "ShippedDate"

    ord_a = ord_a.rename(columns=rename_a)

    for col in ["order_id_source", "EmployeeID", "CustomerID", "OrderDate", "ShippedDate"]:
        if col not in ord_a.columns:
            ord_a[col] = None

    ord_s = sql_data["orders"].copy()
    ord_s["source_system"] = "sqlserver"

    if "OrderID" not in ord_s.columns:
        raise KeyError("pas de orderid trouvée dans orders (sql server)")

    ord_s["order_id_source"] = ord_s["OrderID"]

    for col in ["EmployeeID", "CustomerID", "OrderDate", "ShippedDate"]:
        if col not in ord_s.columns:
            ord_s[col] = None

    common_cols = [
        "source_system",
        "order_id_source",
        "EmployeeID",
        "CustomerID",
        "OrderDate",
        "ShippedDate",
    ]

    orders = pd.concat([ord_a[common_cols], ord_s[common_cols]], ignore_index=True)

    dim_date_index = dim_date.set_index("date")

    def date_to_key(d):
        if pd.isna(d):
            return None
        d2 = pd.to_datetime(d).normalize()
        try:
            return int(dim_date_index.loc[d2, "date_key"])
        except KeyError:
            return None

    orders["order_date_key"] = orders["OrderDate"].apply(date_to_key)
    orders["ship_date_key"] = orders["ShippedDate"].apply(date_to_key)

    emp_map = dim_emp.set_index(["source_system", "employee_id_source"])["employee_key"]

    def map_emp(row):
        return emp_map.get((row["source_system"], row["EmployeeID"]), None)

    orders["employee_key"] = orders.apply(map_emp, axis=1)

    cust_map = dim_cust.set_index(["source_system", "customer_id_source"])["customer_key"]

    def map_cust(row):
        return cust_map.get((row["source_system"], row["CustomerID"]), None)

    orders["customer_key"] = orders.apply(map_cust, axis=1)

    orders["nb_commandes_livrees"] = orders["ShippedDate"].notna().astype(int)
    orders["nb_commandes_non_livrees"] = orders["ShippedDate"].isna().astype(int)

    fact = orders[
        [
            "source_system",
            "order_id_source",
            "customer_key",
            "employee_key",
            "order_date_key",
            "ship_date_key",
            "nb_commandes_livrees",
            "nb_commandes_non_livrees",
        ]
    ].copy()

    fact.insert(0, "fact_order_key", range(1, len(fact) + 1))
    return fact



# =========================
# LOAD
# =========================
def load_processed_dims(dim_emp, dim_cust, dim_date):
    """
    data/processed : dimensions
    """
    dim_emp.to_csv(PROCESSED_DIR / "dim_employee.csv", index=False)
    dim_cust.to_csv(PROCESSED_DIR / "dim_customer.csv", index=False)
    dim_date.to_csv(PROCESSED_DIR / "dim_date.csv", index=False)


def load_final_fact_and_files(dim_emp, dim_cust, dim_date, fact_orders):
    """
    data/final : fact_orders + sqlite + excel
    """
    # fact en CSV dans final
    fact_orders.to_csv(FINAL_DIR / "fact_orders.csv", index=False)

    # excel (dans final)
    with pd.ExcelWriter(EXCEL_OUTPUT, engine="openpyxl") as writer:
        dim_emp.to_excel(writer, sheet_name="dim_employee", index=False)
        dim_cust.to_excel(writer, sheet_name="dim_customer", index=False)
        dim_date.to_excel(writer, sheet_name="dim_date", index=False)
        fact_orders.to_excel(writer, sheet_name="fact_orders", index=False)

  
    conn = sqlite3.connect(DW_DB_PATH)
    dim_emp.to_sql("dim_employee", conn, if_exists="replace", index=False)
    dim_cust.to_sql("dim_customer", conn, if_exists="replace", index=False)
    dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)
    fact_orders.to_sql("fact_orders", conn, if_exists="replace", index=False)
    conn.close()


def main():
    print("extraction access...")
    access_data = extract_access()

    print("extraction sql server...")
    sql_data = extract_sqlserver()

    print("construction dimensions...")
    dim_emp = build_dim_employee(access_data, sql_data)
    dim_cust = build_dim_customer(access_data, sql_data)
    dim_date = build_dim_date()

    print("construction fact_orders...")
    fact_orders = build_fact_orders(access_data, sql_data, dim_emp, dim_cust, dim_date)

    print("load processed (dimensions)...")
    load_processed_dims(dim_emp, dim_cust, dim_date)

    print("load final (fact + sqlite + excel)...")
    load_final_fact_and_files(dim_emp, dim_cust, dim_date, fact_orders)

    print("\n ETL terminé")
    print(f"PROCESSED-> {PROCESSED_DIR.resolve()}")
    print(f"FINAL    -> {FINAL_DIR.resolve()}")
    print(f"SQLite   -> {DW_DB_PATH.resolve()}")
    print(f"Excel    -> {EXCEL_OUTPUT.resolve()}")


if __name__ == "__main__":
    main()
