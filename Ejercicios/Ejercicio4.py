from pyspark.sql import SparkSession
from pyspark import StorageLevel
import os, shutil

spark = SparkSession.builder \
    .appName("Actividad_RDDs") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


csv_lines = sc.parallelize(["Leche, Huevos, Pan", "Manzanas, Peras", "Leche, Chocolate"])

productos_unicos = (csv_lines
                    .flatMap(lambda linea: linea.split(","))
                    .map(lambda prod: prod.strip()) # Elimina espacios extra
                    .distinct())

print("Productos únicos:", productos_unicos.collect())
print("Cantidad de productos únicos:", productos_unicos.count())
# Salida esperada: 6 productos únicos