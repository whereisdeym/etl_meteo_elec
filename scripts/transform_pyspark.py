from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Transform_ETL_Meteo_Elec").getOrCreate()

# Chargement des données brutes
df_meteo = spark.read.json("../data_samples/meteo_sample.json")
df_rte = spark.read.json("../data_samples/rte_sample.json")

# Nettoyage
df_meteo = df_meteo.dropna(subset=["temperature"]).dropDuplicates(["region", "date"])
df_rte = df_rte.dropna(subset=["consommation_brute_electricite"]).dropDuplicates(["region", "date"])

# Jointure
df_joined = df_meteo.join(df_rte, ["region", "date"], "inner")

# Calcul du KPI
df_kpi = df_joined.groupBy("region").agg(
    F.avg("temperature").alias("temperature_moyenne"),
    F.avg("consommation_brute_electricite").alias("conso_moyenne")
)

# Sauvegarde en parquet
df_joined.write.mode("overwrite").parquet("../data_samples/curated_meteo_elec.parquet")
df_kpi.write.mode("overwrite").parquet("../data_samples/kpi_meteo_elec.parquet")
