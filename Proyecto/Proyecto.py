from pyspark.sql import SparkSession
from pyspark import StorageLevel
import os, shutil

spark = SparkSession.builder \
    .appName("Actividad_RDDs") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


# Crear los archivos de entrada simulados
with open("notas_mates.txt", "w", encoding="utf-8") as f:
    f.write("Angel,6\nMaria, 2\nRamon, 4.5")

with open("notas_fisica.txt", "w", encoding="utf-8") as f:
    f.write("Angel,9\nMaria, 3")

with open("notas_ingles.txt", "w", encoding="utf-8") as f:
    f.write("Angel, 4\nMaria, 6")

# Función auxiliar para parsear y limpiar cada línea -> (nombre, nota_float)
def parsear_nota(linea):
    partes = linea.split(",")
    nombre = partes[0].strip()
    nota = float(partes[1].strip())
    return (nombre, nota)

# Cargar y limpiar cada asignatura
rdd_mates  = sc.textFile("notas_mates.txt").map(parsear_nota)
rdd_fisica = sc.textFile("notas_fisica.txt").map(parsear_nota)
rdd_ingles = sc.textFile("notas_ingles.txt").map(parsear_nota)

# Unificar todos los RDDs y persistirlo porque lo usaremos varias veces
todas_las_notas = rdd_mates.union(rdd_fisica).union(rdd_ingles).persist(StorageLevel.MEMORY_AND_DISK)

# Calcular promedio por alumno usando aggregateByKey (suma, conteo)
promedios = (todas_las_notas
             .aggregateByKey(
                 (0.0, 0),
                 lambda acc, val: (acc[0] + val, acc[1] + 1),
                 lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
             )
             .mapValues(lambda x: round(x[0] / x[1], 2))
             .sortBy(lambda x: x[1], ascending=False))

# Nota máxima y mínima por alumno
nota_max = todas_las_notas.reduceByKey(lambda a, b: a if a > b else b)
nota_min = todas_las_notas.reduceByKey(lambda a, b: a if a < b else b)

# Alumnos que cursaron las 3 asignaturas (intersección de claves)
alumnos_todas = (rdd_mates.keys()
                 .intersection(rdd_fisica.keys())
                 .intersection(rdd_ingles.keys()))

print("=== INFORME ESTADÍSTICO DE CALIFICACIONES ===")
print("Promedio general por alumno:", promedios.collect())
print("Nota máxima por alumno:", nota_max.collect())
print("Nota mínima por alumno:", nota_min.collect())
print("Alumnos con las 3 materias registradas:", alumnos_todas.collect())

# Liberar memoria
todas_las_notas.unpersist()