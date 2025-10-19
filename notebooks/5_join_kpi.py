# Databricks notebook source
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("ETL_Meteo_Elec_Join_KPI").getOrCreate()

# Charger les tables clean
df_elec = spark.table("main.etl_meteo_elec.clean_elec")
df_meteo = spark.table("main.etl_meteo_elec.clean_meteo")

print("Tables chargées :")
print(f"- Électricité : {df_elec.count()} lignes")
print(f"- Météo : {df_meteo.count()} lignes")


# COMMAND ----------

# jointure
df_join = (
    df_elec.alias("elec")
    .join(df_meteo.alias("meteo"),
          on=["region", "date"],
          how="inner")
)

df_join = df_join.select(
    "date",
    "region",
    "temperature_moyenne",
    "consommation_mwh"
)

print("Jointure effectuée")
df_join.show(5)

# COMMAND ----------

# kpi par temperature
df_kpi = df_join.groupBy("region").agg(
    F.round(F.avg("temperature_moyenne"), 2).alias("temp_moyenne_region"),
    F.round(F.avg("consommation_mwh"), 2).alias("conso_moyenne_region")
)

df_kpi.show()

# COMMAND ----------

# correlation temperature et consommation
correlation = df_join.stat.corr("temperature_moyenne", "consommation_mwh")
print(f"Corrélation température et consommation : {correlation:.3f}")

# COMMAND ----------

# sauvegarde
df_kpi.write.mode("overwrite").saveAsTable("main.etl_meteo_elec.kpi_temp_conso")
print("Table KPI sauvegardée : main.etl_meteo_elec.kpi_temp_conso")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, temp_moyenne_region, conso_moyenne_region
# MAGIC FROM main.etl_meteo_elec.kpi_temp_conso
# MAGIC ORDER BY conso_moyenne_region DESC;