from pyspark.sql import SparkSession
from pyspark import StorageLevel
import os, shutil

spark = SparkSession.builder \
    .appName("Actividad_RDDs") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext



logs_data = """192.168.1.1, 10:00, GET,/index
192.168.1.2, 10:01, POST,/login
192.168.1.1, 10:02, GET,/imagen.jpg"""

with open("logs.txt", "w", encoding="utf-8") as f:
    f.write(logs_data)

rdd_logs = sc.textFile("logs.txt")


ips_frecuentes = (rdd_logs
                  .map(lambda linea: (linea.split(",")[0].strip(), 1))
                  .reduceByKey(lambda a, b: a + b)
                  .filter(lambda x: x[1] > 1))

print("IPs con más de 1 petición:", ips_frecuentes.collect())

ruta_salida = "output_ips"
if os.path.exists(ruta_salida):
    shutil.rmtree(ruta_salida)

ips_frecuentes.saveAsTextFile(ruta_salida)
print(f"Resultado guardado en la carpeta '{ruta_salida}'")