# scripts/config.py

# URLs des APIs
URL_METEO = "https://api.meteo-concept.com/api/forecast/daily?insee=75056"
URL_RTE = "https://opendata.reseaux-energies.fr/api/explore/v2.1/catalog/datasets/consommation-quotidienne-brute-regionale/records"

# Configuration Spark / Databricks
CATALOG = "main.etl_meteo_elec"

# Google Cloud Storage
GCP_BUCKET = "gs://mon-bucket-meteo-elec"
GCP_PATH_METEO = f"{GCP_BUCKET}/meteo/"
GCP_PATH_RTE = f"{GCP_BUCKET}/rte/"
