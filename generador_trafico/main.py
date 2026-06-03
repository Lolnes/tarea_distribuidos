import time
import requests
import numpy as np
import random
import os
import json
import uuid
from datetime import datetime, timezone

URL_CACHE = "http://api_cache:8000"
URL_METRICAS = "http://api_metricas:8000"
ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
CONSULTAS = ["q1", "q2", "q3", "q4", "q5"]
TIPO_DISTRIBUCION = os.getenv("DISTRIBUCION", "ZIPF")
MODO = os.getenv("MODO", "KAFKA")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_QUERIES = "queries"
DELAY = float(os.getenv("DELAY", "0.1"))

print(f"Iniciando Generador de Tráfico | MODO={MODO} | DISTRIBUCION={TIPO_DISTRIBUCION} | DELAY={DELAY}s")


def elegir_zona_zipf():
    while True:
        r = np.random.zipf(1.5)
        if r <= len(ZONAS):
            return ZONAS[r - 1]


def elegir_zona_uniforme():
    return random.choice(ZONAS)


def elegir_zona():
    if TIPO_DISTRIBUCION == "ZIPF":
        return elegir_zona_zipf()
    return elegir_zona_uniforme()


def reportar_publicada():
    try:
        requests.post(f"{URL_METRICAS}/registrar_kafka", json={"evento": "PUBLICADA", "latencia": 0.0}, timeout=1)
    except Exception:
        pass


time.sleep(15)

if MODO == "KAFKA":
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Conectado a Kafka. Publicando consultas...")
            break
        except NoBrokersAvailable:
            print("Esperando Kafka...")
            time.sleep(3)

    try:
        while True:
            tipo_q = random.choice(CONSULTAS)
            zona = elegir_zona()
            confianza = random.choice([0.0, 0.5, 0.8])

            msg = {
                "id": str(uuid.uuid4()),
                "tipo": tipo_q,
                "zona": zona,
                "confidence_min": confianza,
                "retry_count": 0,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            if tipo_q == "q4":
                msg["zona_b"] = random.choice([z for z in ZONAS if z != zona])

            producer.send(TOPIC_QUERIES, msg)
            reportar_publicada()
            print(f"[KAFKA] Publicado {tipo_q.upper()} zona={zona}")
            time.sleep(DELAY)
    except KeyboardInterrupt:
        print("Generador de tráfico detenido.")

else:
    print("Modo SYNC. Consultando api_cache directamente...")
    try:
        while True:
            tipo_q = random.choice(CONSULTAS)
            zona = elegir_zona()
            confianza = random.choice([0.0, 0.5, 0.8])

            if tipo_q == "q4":
                zona_b = random.choice([z for z in ZONAS if z != zona])
                url = f"{URL_CACHE}/{tipo_q}/{zona}/{zona_b}?confidence_min={confianza}"
            else:
                url = f"{URL_CACHE}/{tipo_q}/{zona}?confidence_min={confianza}"

            try:
                inicio = time.time()
                respuesta = requests.get(url, timeout=2)
                latencia = time.time() - inicio
                data = respuesta.json()
                origen = data.get("origen", "ERROR")
                print(f"[{origen}] {tipo_q.upper()} a {zona} | Latencia: {latencia:.4f}s")
            except Exception as e:
                print(f"Fallo en la petición: {e}")

            time.sleep(DELAY)
    except KeyboardInterrupt:
        print("Generador de tráfico detenido.")
