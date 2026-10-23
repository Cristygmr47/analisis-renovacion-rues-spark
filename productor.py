import csv
import json
import time
from kafka import KafkaProducer

# Configuración
RUTA_CSV = r"C:\Users\crist\Proyecto\data\14abd612-587a-4c99-bb41-8083c3cb8642.csv"
TOPIC = "rues_stream"
DELAY = 0.3  # segundos entre cada mensaje (simula tiempo real)
MAX_FILAS = 2000  # envía solo 2000 filas para no saturar; cambia si quieres más

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

print(f"Enviando datos a Kafka topic '{TOPIC}'...")
print(f"Delay entre mensajes: {DELAY}s")
print("Presiona Ctrl+C para detener.\n")

with open(RUTA_CSV, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for i, fila in enumerate(reader):
        if i >= MAX_FILAS:
            break
        producer.send(TOPIC, fila)
        if i % 50 == 0:
            print(f"Enviadas {i} filas...")
        time.sleep(DELAY)

producer.flush()
print(f"\nTotal enviado: {min(i+1, MAX_FILAS)} filas")