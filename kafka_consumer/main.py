import json
import time
import os
import redis
import requests
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_QUERIES = "queries"
TOPIC_RETRY = "retry_queries"
TOPIC_DLQ = "dlq"
GROUP_ID = "query_consumers"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
URL_RESPUESTAS = "http://api_respuestas:8000"
URL_METRICAS = "http://api_metricas:8000"
REDIS_HOST = "sistema_cache"
TTL_CACHE = 300  # segundos


r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)


def build_cache_key(msg):
    tipo = msg["tipo"]
    zona = msg["zona"]
    confidence = msg.get("confidence_min", 0.0)
    if tipo == "q4":
        zona_b = msg.get("zona_b", "")
        return f"compare:density:{zona}:{zona_b}:conf={confidence}"
    elif tipo == "q5":
        bins = msg.get("bins", 5)
        return f"confidence_dist:{zona}:bins={bins}"
    elif tipo == "q1":
        return f"count:{zona}:conf={confidence}"
    elif tipo == "q2":
        return f"area:{zona}:conf={confidence}"
    elif tipo == "q3":
        return f"density:{zona}:conf={confidence}"
    return f"{tipo}:{zona}:conf={confidence}"


def build_respuestas_url(msg):
    tipo = msg["tipo"]
    zona = msg["zona"]
    confidence = msg.get("confidence_min", 0.0)
    if tipo == "q4":
        zona_b = msg.get("zona_b", "")
        return f"{URL_RESPUESTAS}/q4/{zona}/{zona_b}?confidence_min={confidence}"
    elif tipo == "q5":
        bins = msg.get("bins", 5)
        return f"{URL_RESPUESTAS}/q5/{zona}?bins={bins}"
    else:
        return f"{URL_RESPUESTAS}/{tipo}/{zona}?confidence_min={confidence}"


def reportar(evento, latencia=0.0):
    try:
        requests.post(
            f"{URL_METRICAS}/registrar_kafka",
            json={"evento": evento, "latencia": latencia},
            timeout=1,
        )
    except Exception:
        pass


def wait_for_kafka():
    while True:
        try:
            p = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
            p.close()
            return
        except NoBrokersAvailable:
            print("Esperando Kafka...")
            time.sleep(3)


wait_for_kafka()
print(f"Kafka disponible. Iniciando consumidor (grupo={GROUP_ID}, MAX_RETRIES={MAX_RETRIES})...")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

consumer = KafkaConsumer(
    TOPIC_QUERIES,
    TOPIC_RETRY,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

print("Consumidor activo, esperando mensajes...")

for record in consumer:
    msg = record.value
    tipo = msg.get("tipo", "?")
    zona = msg.get("zona", "?")
    retry_count = msg.get("retry_count", 0)
    inicio = time.time()

    cache_key = build_cache_key(msg)
    cached = r.get(cache_key)

    if cached:
        latencia = time.time() - inicio
        reportar("HIT", latencia)
        print(f"[HIT] {tipo.upper()} zona={zona} retry={retry_count} lat={latencia:.4f}s")
        continue

    url = build_respuestas_url(msg)
    try:
        resp = requests.get(url, timeout=5)
        latencia = time.time() - inicio

        if resp.status_code == 200:
            data = resp.json()
            r.setex(cache_key, TTL_CACHE, json.dumps(data))
            evento = "RECOVERY" if record.topic == TOPIC_RETRY else "MISS"
            reportar(evento, latencia)
            print(f"[{evento}] {tipo.upper()} zona={zona} retry={retry_count} lat={latencia:.4f}s")
        else:
            raise Exception(f"HTTP {resp.status_code}")

    except Exception as e:
        latencia = time.time() - inicio
        new_retry_count = retry_count + 1

        if new_retry_count >= MAX_RETRIES:
            producer.send(TOPIC_DLQ, {**msg, "retry_count": new_retry_count, "error": str(e)})
            reportar("DLQ", latencia)
            print(f"[DLQ] {tipo.upper()} zona={zona} intentos={new_retry_count} error={e}")
        else:
            producer.send(TOPIC_RETRY, {**msg, "retry_count": new_retry_count})
            reportar("RETRY", latencia)
            print(f"[RETRY] {tipo.upper()} zona={zona} intento={new_retry_count} error={e}")
