from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, desc
import matplotlib
matplotlib.use("Agg")  # evita problemas de ventana en Windows
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName("AnalisisRenovacionRUES") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ruta = r"C:\Users\crist\Proyecto\data\14abd612-587a-4c99-bb41-8083c3cb8642.csv"
df = spark.read.csv(ruta, header=True, inferSchema=True)

print("=" * 60)
print("1. DATOS ORIGINALES")
print("=" * 60)
print(f"Total registros: {df.count()}")
print(f"Columnas: {len(df.columns)}")

# ---------------------------------------------------------
# 2. LIMPIEZA
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("2. LIMPIEZA DE DATOS")
print("=" * 60)

df_clean = df.na.drop(subset=[
    "antiguedad_empresa", "renovo", "segmento_antiguedad",
    "tipo_sociedad", "camara_comercio", "anos_sin_renovar"
])
df_clean = df_clean.filter(col("antiguedad_empresa") >= 0)

print(f"Registros después de limpieza: {df_clean.count()}")
print(f"Registros eliminados: {df.count() - df_clean.count()}")

# ---------------------------------------------------------
# 3. TASA DE RENOVACIÓN GLOBAL
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("3. TASA DE RENOVACIÓN GLOBAL")
print("=" * 60)

total = df_clean.count()
renovaron = df_clean.filter(col("renovo") == 1).count()
tasa_global = (renovaron / total) * 100
print(f"Empresas que renovaron: {renovaron:,}")
print(f"Empresas que NO renovaron: {total - renovaron:,}")
print(f"Tasa de renovación global: {tasa_global:.2f}%")

# ---------------------------------------------------------
# 4. RENOVACIÓN POR SEGMENTO DE ANTIGÜEDAD
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("4. RENOVACIÓN POR SEGMENTO DE ANTIGÜEDAD")
print("=" * 60)

por_segmento = df_clean.groupBy("segmento_antiguedad") \
    .agg(
        count("*").alias("total_empresas"),
        spark_round(avg("renovo") * 100, 2).alias("tasa_renovacion_%")
    ) \
    .orderBy(desc("total_empresas"))

por_segmento.show(truncate=False)

# ---------------------------------------------------------
# 5. TOP 10 CÁMARAS DE COMERCIO
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("5. TOP 10 CÁMARAS DE COMERCIO (por volumen)")
print("=" * 60)

top_camaras = df_clean.groupBy("camara_comercio") \
    .agg(
        count("*").alias("total_empresas"),
        spark_round(avg("renovo") * 100, 2).alias("tasa_renovacion_%")
    ) \
    .orderBy(desc("total_empresas")) \
    .limit(10)

top_camaras.show(truncate=False)

# ---------------------------------------------------------
# 6. TOP 10 TIPOS DE SOCIEDAD
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("6. TOP 10 TIPOS DE SOCIEDAD (por volumen)")
print("=" * 60)

por_sociedad = df_clean.groupBy("tipo_sociedad") \
    .agg(
        count("*").alias("total_empresas"),
        spark_round(avg("renovo") * 100, 2).alias("tasa_renovacion_%")
    ) \
    .orderBy(desc("total_empresas")) \
    .limit(10)

por_sociedad.show(truncate=False)

# ---------------------------------------------------------
# 7. AÑOS SIN RENOVAR (CORREGIDO: filtra outliers <= 50)
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("7. AÑOS SIN RENOVAR (NO renovaron, filtrado <= 50 años)")
print("=" * 60)

no_renovaron = df_clean.filter(
    (col("renovo") == 0) & (col("anos_sin_renovar") <= 50)
)
promedio = no_renovaron.agg(avg("anos_sin_renovar")).collect()[0][0]
print(f"Promedio años sin renovar (válido): {promedio:.2f}")
print(f"Registros usados: {no_renovaron.count()}")

no_renovaron.groupBy("segmento_antiguedad") \
    .agg(spark_round(avg("anos_sin_renovar"), 2).alias("promedio_anos_sin_renovar")) \
    .orderBy("segmento_antiguedad") \
    .show(truncate=False)

# ---------------------------------------------------------
# 8. VISUALIZACIONES
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("8. GENERANDO GRÁFICOS...")
print("=" * 60)

pdf_segmento = por_segmento.toPandas()
pdf_camaras = top_camaras.toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 6))

axes[0].bar(pdf_segmento["segmento_antiguedad"],
            pdf_segmento["tasa_renovacion_%"], color="steelblue")
axes[0].set_title("Tasa de Renovación por Segmento de Antigüedad")
axes[0].set_xlabel("Segmento")
axes[0].set_ylabel("Tasa de Renovación (%)")
axes[0].tick_params(axis='x', rotation=45)

axes[1].barh(pdf_camaras["camara_comercio"],
             pdf_camaras["tasa_renovacion_%"], color="coral")
axes[1].set_title("Top 10 Cámaras - Tasa de Renovación")
axes[1].set_xlabel("Tasa de Renovación (%)")
axes[1].invert_yaxis()

plt.tight_layout()
plt.savefig("grafico_batch.png", dpi=100)
print("Gráfico guardado: grafico_batch.png")

por_segmento.toPandas().to_csv("resultado_segmento.csv", index=False)
top_camaras.toPandas().to_csv("resultado_camaras.csv", index=False)
por_sociedad.toPandas().to_csv("resultado_sociedad.csv", index=False)
print("Resultados guardados en CSV")

spark.stop()
print("\n=== ANÁLISIS BATCH COMPLETADO ===")