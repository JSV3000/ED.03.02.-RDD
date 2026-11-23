from pyspark.sql import SparkSession
from pyspark import StorageLevel
import os, shutil

spark = SparkSession.builder \
    .appName("Actividad_RDDs") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


precios_usd = sc.parallelize([10, 25, 40, 5, 100, 15])

precios_eur = (precios_usd
               .filter(lambda x: x >= 20)
               .map(lambda x: round(x * 0.9, 2)))

print("Precios en EUR (>= 20 USD):", precios_eur.collect())