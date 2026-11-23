from pyspark.sql import SparkSession
from pyspark import StorageLevel
import os, shutil

spark = SparkSession.builder \
    .appName("Actividad_RDDs") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


ventas = sc.parallelize([("Madrid", 100), ("Barcelona", 200), ("Madrid", 50)])

resultado_ventas = (ventas
                    .reduceByKey(lambda a, b: a + b)       
                    .filter(lambda x: x[1] > 100)          
                    .sortBy(lambda x: x[1], ascending=False)) 

print("Ventas filtradas y ordenadas:", resultado_ventas.collect())