# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - Ingesta de transacciones BYMA
# MAGIC
# MAGIC Simula un proceso batch particionado por día. No transforma nada del dato original:
# MAGIC cada registro entra tal cual llegó, con columnas de auditoría que marcan (sin descartar)
# MAGIC las anomalías detectadas. Idempotente: reprocesar el mismo día no duplica registros.

# COMMAND ----------

from pyspark.sql import functions as F
from databricks.sdk.runtime import spark, dbutils, display

# AJUSTAR: ruta real del CSV en tu Volume
RUTA_CSV = "/Volumes/workspace/default/raw_data/data-set-challenge-6-.csv"
CATALOGO = "workspace"
SCHEMA = "default"
TABLA_BRONZE = f"{CATALOGO}.{SCHEMA}.bronze_transacciones"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura del CSV origen

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RUTA_CSV)
)

print(f"Filas leídas: {df_raw.count()}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Marcado de calidad
# MAGIC
# MAGIC Criterios adoptados (documentados también en el README):
# MAGIC - **Día inhábil**: la fecha de la transacción cae en sábado o domingo (BYMA no opera esos días)
# MAGIC - **Cantidad extrema**: fuera de un rango razonable de nominales operados (se marca, no se recorta)
# MAGIC - Los registros marcados **no se descartan** — quedan en Bronze con su flag, la decisión de
# MAGIC   excluirlos o no se toma en capas posteriores según el caso de uso

# COMMAND ----------

# Límite de cantidad: se usa el percentil 99.9 del propio dataset como referencia de "extremo",
# documentado así porque no hay una regla de negocio explícita para el límite superior
LIMITE_CANTIDAD = df_raw.approxQuantile("cantidad", [0.999], 0.001)[0]

df_bronze = (
    df_raw
    .withColumn("fecha", F.to_timestamp("fecha"))
    .withColumn("fecha_particion", F.to_date("fecha"))
    .withColumn("dia_semana", F.dayofweek("fecha"))  # 1=domingo, 7=sábado
    .withColumn(
        "flag_dia_inhabil",
        F.col("dia_semana").isin([1, 7])
    )
    .withColumn(
        "flag_cantidad_extrema",
        F.col("cantidad") > F.lit(LIMITE_CANTIDAD)
    )
    .withColumn(
        "flag_calidad_ok",
        ~(F.col("flag_dia_inhabil") | F.col("flag_cantidad_extrema"))
    )
    .withColumn("fecha_ingesta", F.current_timestamp())
    .drop("dia_semana")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de calidad (para trazabilidad, visible en el output)

# COMMAND ----------

total = df_bronze.count()
dia_inhabil = df_bronze.filter(F.col("flag_dia_inhabil")).count()
cantidad_extrema = df_bronze.filter(F.col("flag_cantidad_extrema")).count()
ok = df_bronze.filter(F.col("flag_calidad_ok")).count()

print(f"Total de registros:          {total}")
print(f"Marcados día inhábil:        {dia_inhabil} ({dia_inhabil/total:.2%})")
print(f"Marcados cantidad extrema:   {cantidad_extrema} ({cantidad_extrema/total:.2%})")
print(f"Sin anomalías detectadas:    {ok} ({ok/total:.2%})")
print(f"Límite de cantidad usado:    {LIMITE_CANTIDAD:,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura idempotente a Delta
# MAGIC
# MAGIC Se particiona por `fecha_particion` y se sobrescribe partición por partición
# MAGIC (`replaceWhere`), simulando cargas diarias: reprocesar un día ya cargado no duplica
# MAGIC registros, solo reemplaza esa partición puntual.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")

fechas_a_cargar = [r["fecha_particion"] for r in df_bronze.select("fecha_particion").distinct().collect()]

tabla_existe = spark.catalog.tableExists(TABLA_BRONZE)

if not tabla_existe:
    (
        df_bronze.write
        .format("delta")
        .partitionBy("fecha_particion")
        .mode("overwrite")
        .saveAsTable(TABLA_BRONZE)
    )
    print(f"Tabla {TABLA_BRONZE} creada con {total} registros.")
else:
    fecha_min = min(fechas_a_cargar)
    fecha_max = max(fechas_a_cargar)
    (
        df_bronze.write
        .format("delta")
        .option("replaceWhere", f"fecha_particion >= '{fecha_min}' AND fecha_particion <= '{fecha_max}'")
        .mode("overwrite")
        .saveAsTable(TABLA_BRONZE)
    )
    print(f"Particiones entre {fecha_min} y {fecha_max} reemplazadas en {TABLA_BRONZE}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación final

# COMMAND ----------

spark.sql(f"SELECT flag_calidad_ok, COUNT(*) as registros FROM {TABLA_BRONZE} GROUP BY flag_calidad_ok").show()

# COMMAND ----------