# Databricks notebook source
from pyspark.sql import SparkSession
import requests
import pandas as pd

# COMMAND ----------

spark = SparkSession.builder.appName("extrac_rte_consumption").getOrCreate()

# COMMAND ----------

# Endpoint pour la consommation électrique
data_url = "https://opendata.reseaux-energies.fr/api/explore/v2.1/catalog/datasets/consommation-quotidienne-brute-regionale/records"
params = {
    "limit": 100,
    "order_by": "-date"
}

response = requests.get(data_url, params=params)


# COMMAND ----------

if response.status_code == 200:
    try:
        data = response.json()
    except Exception as e:
        print("Error decoding JSON:", e)
        print("Response text:", response.text)
        data = None
else:
    print(f"Request failed with status code {response.status_code}")
    print("Response text:", response.text)
    data = None

# COMMAND ----------

data = response.json()

# COMMAND ----------

# Transformer la réponse JSON en DataFrame Pandas ---
records = data.get("results", [])
df_pandas = pd.DataFrame(records)

# COMMAND ----------

print(f"Status code: {response.status_code}")
print(f"Nombre d'enregistrements récupérés: {len(data.get('results', []))}")

# COMMAND ----------

# Convertir en DataFrame Spark ---
df_spark = spark.createDataFrame(df_pandas)

# COMMAND ----------

df_spark.show(5)

# COMMAND ----------

# Sauvegarder dans Unity Catalog ---
# table: main.etl_meteo_elec.raw_meteo
df_spark.write.mode("overwrite").saveAsTable("main.etl_meteo_elec.raw_elec")

print("Données RTE sauvegardées dans main.etl_meteo_elec.raw_meteo")