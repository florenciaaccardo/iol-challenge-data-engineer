# Databricks notebook source
# MAGIC %md
# MAGIC # Enriquecimiento con cotizaciones de mercado
# MAGIC
# MAGIC Fuente: [data912](https://data912.apidocs.ar/) — API pública gratuita con históricos
# MAGIC OHLCV específicos para el mercado argentino, con endpoints dedicados para acciones
# MAGIC locales, CEDEARs y bonos soberanos (las tres categorías del dataset), sin necesidad
# MAGIC de API key. El pipeline no falla si un ticker no tiene datos: se registra y se continúa.

# COMMAND ----------

import requests
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from databricks.sdk.runtime import spark, dbutils, display

CATALOGO = "workspace"
SCHEMA = "default"
TABLA_FACT = f"{CATALOGO}.{SCHEMA}.fact_transacciones"
TABLA_DIM_INSTRUMENTO = f"{CATALOGO}.{SCHEMA}.dim_instrumento"
TABLA_DIM_FECHA = f"{CATALOGO}.{SCHEMA}.dim_fecha"
TABLA_SILVER = f"{CATALOGO}.{SCHEMA}.silver_transacciones"

BASE_URL = "https://data912.com/historical"

ENDPOINT_POR_TIPO = {
    "Acción local": "stocks",
    "CEDEAR": "cedears",
    "Bono soberano": "bonds",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Descarga de históricos por instrumento
# MAGIC
# MAGIC Un llamado por símbolo, usando el endpoint que corresponde a su tipo. Errores de un
# MAGIC ticker no interrumpen el resto — se registran para el reporte de cobertura.

# COMMAND ----------

df_instrumentos = spark.table(TABLA_DIM_INSTRUMENTO).toPandas()

resultados = []
tickers_con_error = []

for _, row in df_instrumentos.iterrows():
    simbolo_base = row["simbolo_base"]
    tipo = row["tipo_instrumento"]
    endpoint = ENDPOINT_POR_TIPO.get(tipo)

    if endpoint is None:
        tickers_con_error.append((simbolo_base, "tipo de instrumento sin endpoint asignado"))
        continue

    print(f"Consultando {simbolo_base}...")
    try:
        resp = requests.get(f"{BASE_URL}/{endpoint}/{simbolo_base}", timeout=5)
        resp.raise_for_status()
        datos = resp.json()
        if not datos:
            tickers_con_error.append((simbolo_base, "respuesta vacía"))
            continue
        if isinstance(datos, dict):
            datos = [datos]
        df_hist = pd.json_normalize(datos)
        if "date" not in df_hist.columns or "c" not in df_hist.columns:
            tickers_con_error.append((simbolo_base, "faltan columnas 'date'/'c' en la respuesta"))
            continue
        df_hist = df_hist[["date", "c"]].rename(columns={"c": "precio_cierre"})
        df_hist["simbolo_base"] = simbolo_base
        df_hist["fecha"] = pd.to_datetime(df_hist["date"]).dt.date
        resultados.append(df_hist[["simbolo_base", "fecha", "precio_cierre"]])
    except Exception as e:
        tickers_con_error.append((simbolo_base, str(e)))

df_cotizaciones_pd = pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame(
    columns=["simbolo_base", "fecha", "precio_cierre"]
)

print(f"Instrumentos con datos: {len(resultados)} de {len(df_instrumentos)}")
print(f"Instrumentos con error: {len(tickers_con_error)}")
for t, err in tickers_con_error:
    print(f"  - {t}: {err}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imputación para días sin cotización
# MAGIC
# MAGIC Si una transacción cayó en un día sin cotización disponible, se usa el último
# MAGIC precio de cierre conocido hacia atrás (forward fill).

# COMMAND ----------

df_cotizaciones = spark.createDataFrame(df_cotizaciones_pd) if not df_cotizaciones_pd.empty else None
if df_cotizaciones is not None:
    df_cotizaciones = df_cotizaciones.withColumn(
        "precio_cierre", F.col("precio_cierre").cast("decimal(18,4)")
    )
df_instrumentos_spark = spark.createDataFrame(df_instrumentos[["simbolo_base"]])

df_calendario = spark.table(TABLA_DIM_FECHA).select("fecha").crossJoin(df_instrumentos_spark)

if df_cotizaciones is not None:
    df_cotiz_completo = (
        df_calendario
        .join(df_cotizaciones, on=["simbolo_base", "fecha"], how="left")
        .withColumn(
            "precio_mercado",
            F.last("precio_cierre", ignorenulls=True).over(
                Window.partitionBy("simbolo_base").orderBy("fecha")
                .rowsBetween(Window.unboundedPreceding, 0)
            )
        )
        .select("simbolo_base", "fecha", "precio_mercado")
    )
else:
    df_cotiz_completo = df_calendario.withColumn("precio_mercado", F.lit(None).cast("decimal(18,4)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Actualización de fact_transacciones

# COMMAND ----------

df_fact = spark.table(TABLA_FACT).drop("precio_mercado", "desvio_pct")
df_dim_instrumento = spark.table(TABLA_DIM_INSTRUMENTO).select("sk_instrumento", "simbolo_base")
df_dim_fecha = spark.table(TABLA_DIM_FECHA).select("sk_fecha", "fecha")

df_cotiz_con_sk = (
    df_cotiz_completo
    .join(df_dim_instrumento, on="simbolo_base", how="inner")
    .join(df_dim_fecha, on="fecha", how="inner")
    .select("sk_instrumento", "sk_fecha", "precio_mercado")
    .dropDuplicates(["sk_instrumento", "sk_fecha"])
)

df_fact_enriquecido = (
    df_fact
    .join(df_cotiz_con_sk, on=["sk_instrumento", "sk_fecha"], how="left")
    .withColumn(
        "desvio_pct",
        F.when(
            F.col("precio_mercado").isNotNull() & (F.col("precio_mercado") != 0),
            (F.col("precio_operado") - F.col("precio_mercado")) / F.col("precio_mercado")
        ).otherwise(F.lit(None))
    )
)

(
    df_fact_enriquecido.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("fecha_particion")
    .saveAsTable(TABLA_FACT)
)

print(f"fact_transacciones actualizada: {df_fact_enriquecido.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación de integridad
# MAGIC
# MAGIC El join con las cotizaciones no debe multiplicar filas: el conteo final de
# MAGIC `fact_transacciones` tiene que coincidir con el de Silver.

# COMMAND ----------

conteo_silver = spark.table(TABLA_SILVER).count()
conteo_fact = spark.table(TABLA_FACT).count()

assert conteo_fact == conteo_silver, (
    f"El conteo de fact_transacciones ({conteo_fact}) no coincide con el de Silver "
    f"({conteo_silver}) — probablemente el join con cotizaciones duplicó filas."
)
print(f"OK: fact_transacciones ({conteo_fact}) coincide con Silver ({conteo_silver})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla de cobertura por símbolo (para el README)

# COMMAND ----------

spark.sql(f"""
    SELECT
        i.simbolo_base,
        i.tipo_instrumento,
        COUNT(*) as transacciones,
        COUNT(f.precio_mercado) as con_cotizacion,
        ROUND(COUNT(f.precio_mercado) / COUNT(*) * 100, 1) as pct_cobertura
    FROM {TABLA_FACT} f
    JOIN {TABLA_DIM_INSTRUMENTO} i ON f.sk_instrumento = i.sk_instrumento
    GROUP BY i.simbolo_base, i.tipo_instrumento
    ORDER BY pct_cobertura ASC
""").show(50, truncate=False)

# COMMAND ----------

spark.sql(f"""
    SELECT COUNT(*) as total, SUM(CASE WHEN precio_mercado IS NOT NULL THEN 1 ELSE 0 END) as con_precio
    FROM {TABLA_FACT}
""").show()

# COMMAND ----------
