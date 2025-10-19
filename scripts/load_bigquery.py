from pyspark.sql import SparkSession
from config import BQ_PROJECT, BQ_DATASET, TEMP_BUCKET

spark = SparkSession.builder.appName("Load_to_BigQuery").getOrCreate()

df_final = spark.read.parquet("../data_samples/curated_meteo_elec.parquet")

spark.conf.set("temporaryGcsBucket", TEMP_BUCKET)
spark.conf.set("parentProject", BQ_PROJECT)

df_final.write \
    .format("bigquery") \
    .option("table", f"{BQ_DATASET}.conso_meteo_joined") \
    .mode("overwrite") \
    .save()
