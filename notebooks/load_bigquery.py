# Databricks notebook source
# MAGIC %pip install google-cloud-bigquery

# COMMAND ----------

from pyspark.sql import SparkSession

# Configuration de BigQuery dans Spark
spark.conf.set("temporaryGcsBucket", "etl-meteo-elec-temp")
spark.conf.set("parentProject", "etl-meteo-elec")

# Chargement des données transformées
df_final = spark.table("main.etl_meteo_elec.curated_meteo_elec")

# Écriture vers BigQuery
df_final.write \
    .format("bigquery") \
    .option("table", "etl_meteo_elec.conso_meteo_joined") \
    .mode("overwrite") \
    .save()