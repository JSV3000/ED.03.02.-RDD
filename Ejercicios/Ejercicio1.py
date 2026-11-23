from pyspark.sql import SparkSession
from pyspark import StorageLevel
import os, shutil

spark = SparkSession.builder \
    .appName("Actividad_RDDs") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

texto = ["hola mundo", "hola spark", "mundo spark"]
rdd_texto = sc.parallelize(texto)

word_count = (rdd_texto
              .flatMap(lambda linea: linea.split(" "))
              .map(lambda palabra: (palabra, 1))
              .reduceByKey(lambda a, b: a + b))

print("Conteo de palabras:", word_count.collect())
