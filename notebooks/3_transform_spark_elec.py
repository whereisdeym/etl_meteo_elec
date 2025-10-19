# Databricks notebook source
from pyspark.sql import functions as F

# Charger les données brutes depuis Unity Catalog
df_raw = spark.table("main.etl_meteo_elec.raw_elec")

print("Données chargées depuis raw_elec")
df_raw.printSchema()
df_raw.show(5)

# COMMAND ----------

# selection colonnes importantes
colonnes_utiles = ["date", "region", "consommation_brute_electricite_rte"]

df = df_raw.select(*colonnes_utiles)

# Convertir la date en format date Spark
df = df.withColumn("date", F.to_date("date"))

# Supprimer les valeurs manquantes
df = df.dropna(subset=["date", "region", "consommation_brute_electricite_rte"])

# Renommer la colonne de consommation pour plus de clarté
df = df.withColumnRenamed("consommation_brute_electricite_rte", "consommation_mwh")

# Nettoyer la colonne région (minuscules, sans accents)
df = df.withColumn("region", F.lower(F.regexp_replace("region", "[^a-zA-Z0-9]", "")))

# Ajouter des colonnes de granularité temporelle
df = df.withColumn("annee", F.year("date")) \
       .withColumn("mois", F.month("date")) \
       .withColumn("jour", F.dayofmonth("date"))


# COMMAND ----------

df.groupBy("region").agg(
    F.mean("consommation_mwh").alias("moyenne_mwh")
).orderBy(F.desc("moyenne_mwh")).show(10)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("main.etl_meteo_elec.clean_elec")

print("Données nettoyées sauvegardées dans main.etl_meteo_elec.clean_elec")