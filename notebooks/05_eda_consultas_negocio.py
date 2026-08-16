# Databricks notebook source
# MAGIC %md
# MAGIC # Análisis exploratorio y consultas de negocio
# MAGIC
# MAGIC Consultas SQL sobre la capa Gold, respondiendo preguntas de negocio del challenge.

# COMMAND ----------

from databricks.sdk.runtime import spark, dbutils, display

CATALOGO = "workspace"
SCHEMA = "default"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clientes que operaron consistentemente por encima del precio de mercado
# MAGIC
# MAGIC "Consistentemente" se interpreta como: en más del 70% de sus compras, el precio
# MAGIC operado superó al precio de mercado. Solo se consideran clientes con al menos
# MAGIC 5 compras con cotización disponible, para evitar que 1-2 operaciones aisladas
# MAGIC distorsionen el resultado.

# COMMAND ----------

spark.sql(f"""
    SELECT
        c.id_cliente,
        i.simbolo_base,
        i.tipo_instrumento,
        COUNT(*) as compras_con_cotizacion,
        SUM(CASE WHEN f.precio_operado > f.precio_mercado THEN 1 ELSE 0 END) as compras_sobre_mercado,
        ROUND(
            SUM(CASE WHEN f.precio_operado > f.precio_mercado THEN 1 ELSE 0 END) / COUNT(*) * 100, 1
        ) as pct_sobre_mercado
    FROM {CATALOGO}.{SCHEMA}.fact_transacciones f
    JOIN {CATALOGO}.{SCHEMA}.dim_cliente c ON f.sk_cliente = c.sk_cliente
    JOIN {CATALOGO}.{SCHEMA}.dim_instrumento i ON f.sk_instrumento = i.sk_instrumento
    WHERE f.tipoTran = 'Compra' AND f.precio_mercado IS NOT NULL
    GROUP BY c.id_cliente, i.simbolo_base, i.tipo_instrumento
    HAVING COUNT(*) >= 5
    ORDER BY pct_sobre_mercado DESC, compras_con_cotizacion DESC
    LIMIT 20
""").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Instrumentos con mayor desvío promedio entre precio operado y precio de mercado

# COMMAND ----------

spark.sql(f"""
    SELECT
        i.simbolo_base,
        i.tipo_instrumento,
        COUNT(*) as transacciones_con_cotizacion,
        ROUND(AVG(ABS(f.desvio_pct)) * 100, 2) as desvio_promedio_pct,
        ROUND(AVG(f.desvio_pct) * 100, 2) as desvio_promedio_con_signo_pct
    FROM {CATALOGO}.{SCHEMA}.fact_transacciones f
    JOIN {CATALOGO}.{SCHEMA}.dim_instrumento i ON f.sk_instrumento = i.sk_instrumento
    WHERE f.desvio_pct IS NOT NULL
    GROUP BY i.simbolo_base, i.tipo_instrumento
    HAVING COUNT(*) >= 5
    ORDER BY desvio_promedio_pct DESC
    LIMIT 20
""").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Evolución de la proporción de operaciones por canal, mes a mes

# COMMAND ----------

spark.sql(f"""
    WITH totales_mes AS (
        SELECT d.anio, d.mes, COUNT(*) as total_mes
        FROM {CATALOGO}.{SCHEMA}.fact_transacciones f
        JOIN {CATALOGO}.{SCHEMA}.dim_fecha d ON f.sk_fecha = d.sk_fecha
        GROUP BY d.anio, d.mes
    )
    SELECT
        d.anio,
        d.mes,
        d.nombre_mes,
        ca.origen,
        COUNT(*) as operaciones,
        ROUND(COUNT(*) / t.total_mes * 100, 1) as pct_del_mes
    FROM {CATALOGO}.{SCHEMA}.fact_transacciones f
    JOIN {CATALOGO}.{SCHEMA}.dim_fecha d ON f.sk_fecha = d.sk_fecha
    JOIN {CATALOGO}.{SCHEMA}.dim_canal ca ON f.sk_canal = ca.sk_canal
    JOIN totales_mes t ON d.anio = t.anio AND d.mes = t.mes
    GROUP BY d.anio, d.mes, d.nombre_mes, ca.origen, t.total_mes
    ORDER BY d.anio, d.mes, pct_del_mes DESC
""").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Retención de clientes: qué % de clientes de enero volvió a operar en febrero y marzo

# COMMAND ----------

spark.sql(f"""
    WITH clientes_por_mes AS (
        SELECT DISTINCT f.sk_cliente, d.mes
        FROM {CATALOGO}.{SCHEMA}.fact_transacciones f
        JOIN {CATALOGO}.{SCHEMA}.dim_fecha d ON f.sk_fecha = d.sk_fecha
    ),
    clientes_enero AS (
        SELECT sk_cliente FROM clientes_por_mes WHERE mes = 1
    )
    SELECT
        (SELECT COUNT(*) FROM clientes_enero) as clientes_enero,
        COUNT(DISTINCT CASE WHEN cm.mes = 2 THEN cm.sk_cliente END) as volvieron_febrero,
        COUNT(DISTINCT CASE WHEN cm.mes = 3 THEN cm.sk_cliente END) as volvieron_marzo,
        ROUND(
            COUNT(DISTINCT CASE WHEN cm.mes = 2 THEN cm.sk_cliente END) /
            (SELECT COUNT(*) FROM clientes_enero) * 100, 1
        ) as pct_retencion_febrero,
        ROUND(
            COUNT(DISTINCT CASE WHEN cm.mes = 3 THEN cm.sk_cliente END) /
            (SELECT COUNT(*) FROM clientes_enero) * 100, 1
        ) as pct_retencion_marzo
    FROM clientes_por_mes cm
    WHERE cm.sk_cliente IN (SELECT sk_cliente FROM clientes_enero)
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Correlación entre canal de origen y probabilidad de comprar por encima del mercado

# COMMAND ----------

spark.sql(f"""
    SELECT
        ca.origen,
        COUNT(*) as compras_con_cotizacion,
        SUM(CASE WHEN f.precio_operado > f.precio_mercado THEN 1 ELSE 0 END) as compras_sobre_mercado,
        ROUND(
            SUM(CASE WHEN f.precio_operado > f.precio_mercado THEN 1 ELSE 0 END) / COUNT(*) * 100, 1
        ) as pct_compras_sobre_mercado
    FROM {CATALOGO}.{SCHEMA}.fact_transacciones f
    JOIN {CATALOGO}.{SCHEMA}.dim_canal ca ON f.sk_canal = ca.sk_canal
    WHERE f.tipoTran = 'Compra' AND f.precio_mercado IS NOT NULL
    GROUP BY ca.origen
    ORDER BY pct_compras_sobre_mercado DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pregunta propia: segmentación de clientes por actividad
# MAGIC
# MAGIC ¿Cómo se distribuyen los clientes según su nivel de actividad (cantidad de
# MAGIC operaciones en el período), y qué porcentaje del volumen total de transacciones
# MAGIC concentra cada segmento? Relevante para priorizar a qué clientes dirigir
# MAGIC comunicación o soporte comercial.

# COMMAND ----------

spark.sql(f"""
    WITH actividad_cliente AS (
        SELECT
            f.sk_cliente,
            COUNT(*) as total_operaciones
        FROM {CATALOGO}.{SCHEMA}.fact_transacciones f
        GROUP BY f.sk_cliente
    ),
    segmentado AS (
        SELECT
            sk_cliente,
            total_operaciones,
            CASE
                WHEN total_operaciones >= 20 THEN 'Alta'
                WHEN total_operaciones >= 5 THEN 'Media'
                ELSE 'Baja'
            END as segmento_actividad
        FROM actividad_cliente
    )
    SELECT
        segmento_actividad,
        COUNT(*) as cantidad_clientes,
        SUM(total_operaciones) as total_operaciones_segmento,
        ROUND(SUM(total_operaciones) / (SELECT SUM(total_operaciones) FROM segmentado) * 100, 1) as pct_del_volumen_total
    FROM segmentado
    GROUP BY segmento_actividad
    ORDER BY total_operaciones_segmento DESC
""").show(truncate=False)
