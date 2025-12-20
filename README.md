


## Prérequis

- Bibliothèques Python :
pyodbc : connexion a Access et SQL Server
pandas : transformation et nettoyage des donnees (jointures, agregations, exports)
sqlite3 : Data Warehouse
openpyx1 : export Excel pour verification et partage
streamlit : creation du dashboard interactif sans developpement front-end.
plotly : visualisations interactives adaptees a l'analyse exploratoire.
logging : suivi de 1'exécution

## Structure du projet
- load_raw.py : export des données brutes SQL Server vers la couche RAW (CSV)
- etl.py : extraction Access + SQL Server, transformation et création du Data Warehouse SQLite
- sql.py : chargement de la table de faits du Data Warehouse vers SQL Server
- dashboard.py : visualisation des données via Streamlit

## Ordre d’exécution
1. Exporter les données brutes :
   python scripts\load_raw.py

2. Construire le Data Warehouse :
   python scripts\etl.py


3. Charger les données finales dans SQL Server :
   python scripts\sql.py

4. Lancer le tableau de bord :
   streamlit run scripts\dashboard.py

## Résultats
- Data Warehouse SQLite : data/final/northwind_dw.sqlite
- Table SQL Server : FactOrders_Final
- Fichiers CSV RAW : data/raw
