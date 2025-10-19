# Databricks notebook source
import requests
import pandas as pd
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("ETL_Meteo_Elec_Transform_Meteo").getOrCreate()

# COMMAND ----------

# liste des régions
regions = {
    "iledefrance": (48.8566, 2.3522),
    "provencealpescotedazur": (43.9352, 6.0679),
    "nouvelleaquitaine": (44.8378, -0.5792),
    "occitanie": (43.6047, 1.4442),
    "grandest": (48.6921, 6.1844),
    "auvergnerhonealpes": (45.7640, 4.8357),
    "bourgognefranchecomte": (47.3220, 5.0415),
    "hautsdefrance": (50.6292, 3.0573),
    "normandie": (49.1829, 0.3707),
    "centrevaldeloire": (47.9029, 1.9093),
    "bretagne": (48.2020, -2.9326),
    "paysdelaloire": (47.2184, -1.5536),
    "corse": (42.0396, 9.0129)
}

# COMMAND ----------

API_KEY = "4f718bc29715470692c71460ba682d7c"
url = "https://api.openweathermap.org/data/2.5/weather"

records = []

for region, (lat, lon) in regions.items():
    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        records.append({
            "region": region,
            "date": pd.Timestamp.now().normalize(),
            "temperature": data["main"]["temp"]
        })
    else:
        print(f"Erreur {response.status_code} pour {region}")

# COMMAND ----------

# transformatoin et agregation
df_pandas = pd.DataFrame(records)
df_spark = spark.createDataFrame(df_pandas)

# Colonnes dérivées
df_spark = df_spark.withColumn("annee", F.year("date")) \
                   .withColumn("mois", F.month("date")) \
                   .withColumn("jour", F.dayofmonth("date"))

# Agrégation moyenne quotidienne par région
df_meteo_clean = df_spark.groupBy("region", "date").agg(
    F.mean("temperature").alias("temperature_moyenne")
)

# COMMAND ----------

# sauvergarde
df_meteo_clean.write.mode("overwrite").saveAsTable("main.etl_meteo_elec.clean_meteo")

print("Données météo sauvegardées dans main.etl_meteo_elec.clean_meteo")
