# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Limpieza y estructura de transacciones BYMA
# MAGIC
# MAGIC Toma Bronze, tipa correctamente, y resuelve las variantes de liquidación de símbolo
# MAGIC (ej. AL30 vs AL30D: mismo instrumento base, distinta moneda de liquidación).
# MAGIC Solo pasan a Silver los registros con `flag_calidad_ok = true` — los marcados quedan
# MAGIC documentados en Bronze como fuente de trazabilidad, no se replican en capas posteriores.

# COMMAND ----------

from pyspark.sql import functions as F
from databricks.sdk.runtime import spark, dbutils, display

CATALOGO = "workspace"
SCHEMA = "default"
TABLA_BRONZE = f"{CATALOGO}.{SCHEMA}.bronze_transacciones"
TABLA_SILVER = f"{CATALOGO}.{SCHEMA}.silver_transacciones"

# COMMAND ----------

df_bronze = spark.table(TABLA_BRONZE).filter(F.col("flag_calidad_ok") == True)

print(f"Registros que pasan a Silver: {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolución de variantes de símbolo
# MAGIC
# MAGIC Un símbolo con sufijo `D` (ej. `AL30D`) es el mismo instrumento base que su versión sin
# MAGIC sufijo (`AL30`), pero liquidado en dólar cable en vez de pesos. Se separa en dos columnas:
# MAGIC `simbolo_base` (para agrupar el mismo instrumento sin importar la liquidación) y
# MAGIC `liquidacion` (ARS o USD).

# COMMAND ----------

df_silver = (
    df_bronze
    .withColumn(
        "simbolo_base",
        F.when(
            F.col("simbolo_titulo").endswith("D"),
            F.expr("substring(simbolo_titulo, 1, length(simbolo_titulo) - 1)")
        ).otherwise(F.col("simbolo_titulo"))
    )
    .withColumn(
        "liquidacion",
        F.when(F.col("simbolo_titulo").endswith("D"), F.lit("USD")).otherwise(F.col("moneda"))
    )
    .withColumn("cantidad", F.col("cantidad").cast("long"))
    .withColumn("precio", F.col("precio").cast("decimal(18,4)"))
    .select(
        "id_transaccion",
        "fecha",
        "fecha_particion",
        "tipoTran",
        "id_cliente",
        "descripcion_titulo",
        "simbolo_titulo",
        "simbolo_base",
        "moneda",
        "liquidacion",
        "cantidad",
        "precio",
        "origen",
        "fecha_ingesta"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación del mapeo de símbolos

# COMMAND ----------

df_silver.select("simbolo_titulo", "simbolo_base", "liquidacion").distinct().orderBy("simbolo_base").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura idempotente a Delta

# COMMAND ----------

fechas_a_cargar = [r["fecha_particion"] for r in df_silver.select("fecha_particion").distinct().collect()]
tabla_existe = spark.catalog.tableExists(TABLA_SILVER)

if not tabla_existe:
    (
        df_silver.write
        .format("delta")
        .partitionBy("fecha_particion")
        .mode("overwrite")
        .saveAsTable(TABLA_SILVER)
    )
    print(f"Tabla {TABLA_SILVER} creada con {df_silver.count()} registros.")
else:
    fecha_min = min(fechas_a_cargar)
    fecha_max = max(fechas_a_cargar)
    (
        df_silver.write
        .format("delta")
        .option("replaceWhere", f"fecha_particion >= '{fecha_min}' AND fecha_particion <= '{fecha_max}'")
        .mode("overwrite")
        .saveAsTable(TABLA_SILVER)
    )
    print(f"Particiones entre {fecha_min} y {fecha_max} reemplazadas en {TABLA_SILVER}.")
