from pyspark.sql import SparkSession
from pyspark import StorageLevel
import os, shutil

spark = SparkSession.builder \
    .appName("Actividad_RDDs") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


ventas_ej6 = sc.parallelize([("Madrid", 100), ("Barcelona", 200), ("Madrid", 50), ("Valencia", 50)])


pipeline_ventas = (ventas_ej6
                   .filter(lambda x: x[1] < 60)
                   .reduceByKey(lambda a, b: a + b)
                   .sortBy(lambda x: x[1], ascending=False))

print("Facturación final:", pipeline_ventas.collect())
