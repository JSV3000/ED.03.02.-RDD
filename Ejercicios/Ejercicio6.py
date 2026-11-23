from pyspark.sql import SparkSession
from pyspark import StorageLevel
import os, shutil

spark = SparkSession.builder \
    .appName("Actividad_RDDs") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


ventas_ej6 = sc.parallelize([("Madrid", 100), ("Barcelona", 200), ("Madrid", 50), ("Valencia", 50)])

# Si tu profesor se refiere a seleccionar solo las < 60, cambia ">=" por "<".
pipeline_ventas = (ventas_ej6
                   .filter(lambda x: x[1] >= 60)
                   .reduceByKey(lambda a, b: a + b)
                   .sortBy(lambda x: x[1], ascending=False))

print("Facturación final:", pipeline_ventas.collect())
# Salida esperada: [('Barcelona', 200), ('Madrid', 100)]