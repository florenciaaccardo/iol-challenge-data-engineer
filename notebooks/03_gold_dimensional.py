# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Modelo dimensional de transacciones BYMA
# MAGIC
# MAGIC Arma las 4 dimensiones y la tabla de hechos a partir de Silver. Grano de
# MAGIC `fact_transacciones`: una fila por transacción. `precio_mercado` y `desvio_pct`
# MAGIC quedan en NULL hasta el enriquecimiento con la API de cotizaciones (Task 4).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from databricks.sdk.runtime import spark, dbutils, display

CATALOGO = "workspace"
SCHEMA = "default"
TABLA_SILVER = f"{CATALOGO}.{SCHEMA}.silver_transacciones"

df_silver = spark.table(TABLA_SILVER)
print(f"Registros en Silver: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_fecha

# COMMAND ----------

df_dim_fecha = (
    df_silver
    .select("fecha_particion")
    .distinct()
    .withColumnRenamed("fecha_particion", "fecha")
    .withColumn("anio", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("nombre_mes", F.date_format("fecha", "MMMM"))
    .withColumn("dia_semana_num", F.dayofweek("fecha"))
    .withColumn("es_habil", ~F.col("dia_semana_num").isin([1, 7]))
    .drop("dia_semana_num")
    .withColumn("sk_fecha", F.row_number().over(Window.orderBy("fecha")))
    .select("sk_fecha", "fecha", "anio", "mes", "nombre_mes", "es_habil")
)

(
    df_dim_fecha.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOGO}.{SCHEMA}.dim_fecha")
)
print(f"dim_fecha: {df_dim_fecha.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_canal

# COMMAND ----------

df_dim_canal = (
    df_silver
    .select("origen")
    .distinct()
    .withColumn("sk_canal", F.row_number().over(Window.orderBy("origen")))
    .select("sk_canal", "origen")
)

(
    df_dim_canal.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOGO}.{SCHEMA}.dim_canal")
)
print(f"dim_canal: {df_dim_canal.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_cliente
# MAGIC
# MAGIC Solo el identificador del cliente — sin atributos de comportamiento, para
# MAGIC mantener la dimensión estable.

# COMMAND ----------

df_dim_cliente = (
    df_silver
    .select("id_cliente")
    .distinct()
    .withColumn("sk_cliente", F.row_number().over(Window.orderBy("id_cliente")))
    .select("sk_cliente", "id_cliente")
)

(
    df_dim_cliente.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOGO}.{SCHEMA}.dim_cliente")
)
print(f"dim_cliente: {df_dim_cliente.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_instrumento
# MAGIC
# MAGIC Clasificación de tipo_instrumento:
# MAGIC 1. Símbolo con patrón 2 letras + 2 números (ej. AL30) → bono soberano
# MAGIC 2. Símbolo en la lista de acciones argentinas conocidas → acción local
# MAGIC 3. Cualquier otro caso → CEDEAR

# COMMAND ----------

ACCIONES_LOCALES = [
    "YPFD", "GGAL", "PAMP", "BMA", "CRES", "EDN", "TGSU2", "TGNO4",
    "LOMA", "MIRG", "CEPU", "SUPV", "VALO", "BBAR", "COME", "ALUA",
    "TXAR", "TRAN", "CVH", "BYMA", "HARG"
]

df_dim_instrumento = (
    df_silver
    .groupBy("simbolo_base")
    .agg(F.first("descripcion_titulo").alias("descripcion_titulo"))
    .withColumn(
        "tipo_instrumento",
        F.when(
            F.col("simbolo_base").rlike("^[A-Z]{2}[0-9]{2}$"),
            F.lit("Bono soberano")
        ).when(
            F.col("simbolo_base").isin(ACCIONES_LOCALES),
            F.lit("Acción local")
        ).otherwise(F.lit("CEDEAR"))
    )
    .withColumn("sk_instrumento", F.row_number().over(Window.orderBy("simbolo_base")))
    .select("sk_instrumento", "simbolo_base", "descripcion_titulo", "tipo_instrumento")
)

(
    df_dim_instrumento.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOGO}.{SCHEMA}.dim_instrumento")
)
print(f"dim_instrumento: {df_dim_instrumento.count()} filas")
df_dim_instrumento.groupBy("tipo_instrumento").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_transacciones
# MAGIC
# MAGIC precio_mercado y desvio_pct quedan en NULL — se completan en el enriquecimiento (Task 4).

# COMMAND ----------

df_fact = (
    df_silver
    .join(df_dim_cliente, on="id_cliente", how="left")
    .join(df_dim_instrumento, on="simbolo_base", how="left")
    .join(
        df_dim_fecha.withColumnRenamed("fecha", "fecha_particion"),
        on="fecha_particion", how="left"
    )
    .join(df_dim_canal, on="origen", how="left")
    .withColumn("precio_mercado", F.lit(None).cast("decimal(18,4)"))
    .withColumn("desvio_pct", F.lit(None).cast("decimal(10,4)"))
    .select(
        F.col("id_transaccion"),
        F.col("sk_cliente"),
        F.col("sk_instrumento"),
        F.col("sk_fecha"),
        F.col("sk_canal"),
        F.col("fecha_particion"),
        F.col("tipoTran"),
        F.col("liquidacion"),
        F.col("cantidad"),
        F.col("precio").alias("precio_operado"),
        F.col("precio_mercado"),
        F.col("desvio_pct")
    )
)

fechas_a_cargar = [r["fecha_particion"] for r in df_fact.select("fecha_particion").distinct().collect()]
tabla_fact = f"{CATALOGO}.{SCHEMA}.fact_transacciones"

tabla_existe = spark.catalog.tableExists(tabla_fact)

try:
    if not tabla_existe:
        (
            df_fact.write.format("delta")
            .partitionBy("fecha_particion")
            .mode("overwrite")
            .saveAsTable(tabla_fact)
        )
        print(f"fact_transacciones creada con {df_fact.count()} registros.")
    else:
        fecha_min = min(fechas_a_cargar)
        fecha_max = max(fechas_a_cargar)
        (
            df_fact.write.format("delta")
            .option("replaceWhere", f"fecha_particion >= '{fecha_min}' AND fecha_particion <= '{fecha_max}'")
            .mode("overwrite")
            .saveAsTable(tabla_fact)
        )
        print(f"Particiones entre {fecha_min} y {fecha_max} reemplazadas en fact_transacciones.")
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {str(e)[:500]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación

# COMMAND ----------

spark.sql(f"""
    SELECT COUNT(*) as total_hechos,
           COUNT(DISTINCT sk_cliente) as clientes_unicos,
           COUNT(DISTINCT sk_instrumento) as instrumentos_unicos
    FROM {tabla_fact}
""").show()
