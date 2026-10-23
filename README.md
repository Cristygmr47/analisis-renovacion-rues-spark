Análisis de Renovación Empresarial en Colombia (RUES)
Proyecto de Big Data con Apache Spark y Kafka.

Dataset
Predicción de Renovación Empresarial en Colombia (Kaggle, 500,000 registros).
El archivo CSV original debe descargarse y colocarse en la carpeta data/.

Requisitos
Python 3.13

PySpark 4.2.0

Kafka 3.9.2

Java 21

winutils.exe en C:\hadoop\bin (necesario para Spark en Windows)

Procesamiento Batch
Ejecutar el análisis exploratorio de datos y guardar resultados:
python batch.py
Genera grafico_batch.png y tres archivos CSV con los resultados del análisis.

Procesamiento Streaming (Kafka + Spark)
Arrancar Zookeeper:
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
Arrancar Kafka:
bin\windows\kafka-server-start.bat config\server.properties
Crear el topic:
bin\windows\kafka-topics.bat --create --topic rues_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
Ejecutar el consumidor de Spark Streaming:
python streaming.py
En otra terminal, ejecutar el productor para simular datos en tiempo real:
python productor.py
Resultados principales
Tasa global de renovación: 45.10%

Empresas nuevas: 91.91% de renovación

Empresas maduras: 32.27% de renovación
