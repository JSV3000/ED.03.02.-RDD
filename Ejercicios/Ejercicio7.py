from pyspark.sql import SparkSession
from pyspark import StorageLevel
import os, shutil

spark = SparkSession.builder \
    .appName("Actividad_RDDs") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


frases = sc.parallelize(["Hola mundo", "Apache Spark es guay"])

# 1. map
con_map = frases.map(lambda f: f.split(" "))
print("Con map():", con_map.collect())

# 2. flatMap
con_flatmap = frases.flatMap(lambda f: f.split(" "))
print("Con flatMap():", con_flatmap.collect())