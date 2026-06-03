from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np
import time

app = FastAPI()

# --- Métricas Tarea 1 (SYNC) ---
registro_hits = 0
registro_misses = 0
latencias = []

# --- Métricas Tarea 2 (Kafka) ---
kafka_hits = 0
kafka_misses = 0
kafka_retries = 0
kafka_recoveries = 0
kafka_dlq = 0
kafka_publicadas = 0
kafka_latencias = []
inicio_sistema = time.time()


class Metrica(BaseModel):
    tipo: str
    latencia: float
    consulta: str
    zona: str


class MetricaKafka(BaseModel):
    evento: str
    latencia: float = 0.0


@app.get("/")
def estado():
    return {"status": "API de Métricas online"}


@app.post("/registrar")
def registrar_evento(metrica: Metrica):
    global registro_hits, registro_misses
    if metrica.tipo == "HIT":
        registro_hits += 1
    else:
        registro_misses += 1
    latencias.append(metrica.latencia)
    return {"status": "Registrado"}


@app.post("/registrar_kafka")
def registrar_kafka(metrica: MetricaKafka):
    global kafka_hits, kafka_misses, kafka_retries, kafka_recoveries, kafka_dlq, kafka_publicadas
    evento = metrica.evento.upper()
    if evento == "HIT":
        kafka_hits += 1
    elif evento == "MISS":
        kafka_misses += 1
    elif evento == "RETRY":
        kafka_retries += 1
    elif evento == "RECOVERY":
        kafka_recoveries += 1
        kafka_misses += 1
    elif evento == "DLQ":
        kafka_dlq += 1
        kafka_misses += 1
    elif evento == "PUBLICADA":
        kafka_publicadas += 1
    if metrica.latencia > 0:
        kafka_latencias.append(metrica.latencia)
    return {"status": "Registrado"}


@app.get("/stats")
def obtener_estadisticas():
    total_consultas = registro_hits + registro_misses
    if total_consultas == 0:
        return {"error": "Aún no hay datos"}
    hit_rate = registro_hits / total_consultas
    p50 = np.percentile(latencias, 50)
    p95 = np.percentile(latencias, 95)
    return {
        "total_peticiones": total_consultas,
        "hits": registro_hits,
        "misses": registro_misses,
        "hit_rate": round(hit_rate, 4),
        "latencia_p50": round(p50, 4),
        "latencia_p95": round(p95, 4),
    }


@app.get("/stats/kafka")
def obtener_estadisticas_kafka():
    total_procesadas = kafka_hits + kafka_misses
    uptime = round(time.time() - inicio_sistema, 1)
    throughput = round(total_procesadas / uptime, 2) if uptime > 0 else 0

    if total_procesadas == 0:
        return {"error": "Aún no hay datos Kafka", "publicadas": kafka_publicadas}

    backlog_size = max(0, kafka_publicadas - total_procesadas)
    hit_rate = kafka_hits / total_procesadas
    retry_rate = kafka_retries / total_procesadas
    recovery_rate = round(kafka_recoveries / kafka_retries, 4) if kafka_retries > 0 else 0.0
    dlq_rate = kafka_dlq / total_procesadas

    p50 = round(float(np.percentile(kafka_latencias, 50)), 4) if kafka_latencias else 0.0
    p95 = round(float(np.percentile(kafka_latencias, 95)), 4) if kafka_latencias else 0.0

    return {
        "uptime_s": uptime,
        "throughput_rps": throughput,
        "publicadas": kafka_publicadas,
        "total_procesadas": total_procesadas,
        "hits": kafka_hits,
        "misses": kafka_misses,
        "hit_rate": round(hit_rate, 4),
        "retries": kafka_retries,
        "recoveries": kafka_recoveries,
        "dlq_count": kafka_dlq,
        "retry_rate": round(retry_rate, 4),
        "recovery_rate": recovery_rate,
        "dlq_rate": round(dlq_rate, 4),
        "backlog_size": backlog_size,
        "latencia_p50": p50,
        "latencia_p95": p95,
    }
