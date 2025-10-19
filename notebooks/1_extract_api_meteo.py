# Databricks notebook source
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

# COMMAND ----------

spark = SparkSession.builder.appName("Weather").getOrCreate()

# COMMAND ----------

# variable de config
API_KEY = "4f718bc29715470692c71460ba682d7c"
cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Nantes", "Rennes", "Strasbourg", "Lille", "Montpellier"]
base_url = "https://api.openweathermap.org/data/2.5/weather"

# COMMAND ----------

# extraction des données
weather_data =[]

for city in cities:
    params = {"q": f"{city},FR ", "appid": API_KEY, "unit": "metric"}
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        weather_data.append({
            "ville": city,
            "temperature": data["main"]["temp"],
            "description": data["weather"][0]["description"],
            "humidite": data["main"]["humidity"],
            "vent_vitesse": data["wind"]["speed"], 
            "timestamp": data["dt"]
        })
    else:
        print(f"Error: {response.status_code} - {response.text}")

# COMMAND ----------

# transformation des données
schema = StructType([
    StructField("ville", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("description", StringType(), True),
    StructField("humidite", FloatType(), True),
    StructField("vent_vitesse", FloatType(), True),
    StructField("timestamp", LongType(), True)
])

df_meteo = spark.createDataFrame(weather_data, schema)

display(df_meteo)

# COMMAND ----------

df_meteo.show(5)

# COMMAND ----------

# sauvegarde
df_meteo = spark.read.table(
    "main.etl_meteo_elec.raw_meteo"
)

df_meteo.write.mode("overwrite").saveAsTable("main.etl_meteo_elec.raw_meteo")

display(df_meteo)