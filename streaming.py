from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, avg, round as spark_round, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Crear sesión con el conector de Kafka para Spark 4.2.0
spark = SparkSession.builder \
    .appName("StreamingRUES") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Esquema de los datos (todos como String porque el CSV llega así)
schema = StructType([
    StructField("codigo_camara", StringType()),
    StructField("camara_comercio", StringType()),
    StructField("tipo_sociedad", StringType()),
    StructField("organizacion_juridica", StringType()),
    StructField("categoria_matricula", StringType()),
    StructField("actividad_economica", StringType()),
    StructField("tipo_persona", StringType()),
    StructField("antiguedad_empresa", StringType()),
    StructField("ultimo_ano_renovado", StringType()),
    StructField("renovo", StringType()),
    StructField("segmento_antiguedad", StringType()),
    StructField("anos_sin_renovar", StringType()),
])

# 1. Leer stream desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rues_stream") \
    .option("startingOffsets", "latest") \
    .load()

# 2. Parsear el JSON que viene en "value"
df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_ts")
).select("data.*", "kafka_ts")

# 3. Convertir renovo a entero para poder promediar
df_parsed = df_parsed.withColumn("renovo_int", col("renovo").cast(IntegerType()))

# 4. Análisis en tiempo real: tasa de renovación por segmento en ventanas de 10s
resultado = df_parsed \
    .withWatermark("kafka_ts", "10 seconds") \
    .groupBy(
        window(col("kafka_ts"), "10 seconds"),
        col("segmento_antiguedad")
    ) \
    .agg(
        count("*").alias("eventos"),
        spark_round(avg("renovo_int") * 100, 2).alias("tasa_renovacion_%")
    )

# 5. Mostrar en consola
query = resultado.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("Esperando datos de Kafka... (Ctrl+C para detener)")
query.awaitTermination()