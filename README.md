# Plataforma de Análisis Geoespacial Distribuida
### Sistemas Distribuidos 2026-1 — Universidad Diego Portales

Sistema distribuido para análisis de edificaciones en la Región Metropolitana de Santiago, basado en el dataset **Google Open Buildings**. Incorpora caché Redis, mensajería asíncrona con Apache Kafka, reintentos automáticos y Dead Letter Queue (DLQ).

---

## Arquitectura

```
Generador de Tráfico
        │
        ▼
  Kafka (topic: queries)
        │
        ▼
  Consumidores Kafka ──► Cache Redis ──► HIT → respuesta inmediata
        │                                MISS ↓
        │                         Generador de Respuestas
        │                         (Google Open Buildings en RAM)
        │
        ├── Falla → retry_queries (máx. 3 intentos)
        └── Max reintentos → DLQ
        
  Todo registra métricas → api_metricas
```

### Servicios

| Servicio | Tecnología | Puerto | Descripción |
|---|---|---|---|
| `app_trafico` | Python | — | Genera consultas Q1–Q5 (Zipf o Uniforme) |
| `kafka` | Confluent Kafka 7.5.0 | 9092 | Broker de mensajería |
| `zookeeper` | Confluent Zookeeper 7.5.0 | 2181 | Coordinación de Kafka |
| `kafka_consumer` | Python | — | Consume topics, consulta caché y generador |
| `sistema_cache` | Redis Alpine | 6379 | Caché en memoria (200MB, LRU, TTL=60s) |
| `api_cache` | FastAPI | 8000 | Proxy inverso del caché (modo SYNC) |
| `api_respuestas` | FastAPI + Pandas | — | Cálculos sobre datos precargados en RAM |
| `api_metricas` | FastAPI + NumPy | 8001 | Registro y exposición de métricas |

---

## Requisitos

- Docker >= 24.0
- Docker Compose >= 2.0
- Dataset Google Open Buildings (ver instrucciones abajo)
- 3 GB de RAM disponible mínimo

---

## Instalación y despliegue

### 1. Clonar el repositorio

```bash
git clone https://github.com/Lolnes/tarea_distribuidos
cd tarea1-distribuidos
```

### 2. Descargar y preparar el dataset real

Descargar el archivo `967_buildings.csv.gz` desde [Google Open Buildings](https://sites.research.google/gr/open-buildings/) y colocarlo en `~/Downloads/`. Luego ejecutar:

```bash
python preparar_datos.py
```

Esto filtra los edificios de las 5 zonas de Santiago y genera los archivos `Z1.csv` a `Z5.csv` en `generador_respuestas/`:

| Zona | Sector | Edificios |
|---|---|---|
| Z1 | Providencia | 15.639 |
| Z2 | Las Condes | 27.853 |
| Z3 | Maipú | 74.117 |
| Z4 | Santiago Centro | 24.453 |
| Z5 | Pudahuel | 21.345 |

### 3. Iniciar el sistema

```bash
docker compose up --build -d
```

Esperar ~30 segundos a que Kafka esté listo (healthcheck). Verificar:

```bash
docker compose ps
```

Todos los servicios deben estar en estado `Up` o `healthy`.

---

## Uso

### Consultar métricas en tiempo real

**Modo Kafka (Tarea 2):**
```bash
curl http://localhost:8001/stats/kafka
```

**Modo Síncrono (Tarea 1):**
```bash
curl http://localhost:8001/stats
```

### Cambiar distribución de tráfico

En `docker-compose.yml`, dentro de `app_trafico`:
```yaml
environment:
  - DISTRIBUCION=ZIPF     # ZIPF | UNIFORME
  - MODO=KAFKA            # KAFKA | SYNC
  - DELAY=0.1             # segundos entre consultas
```

Reiniciar tras cambios:
```bash
docker compose down && docker compose up --build -d
```

### Escalar consumidores Kafka

```bash
docker compose up --scale kafka_consumer=3 -d
```

### Simular falla del Generador de Respuestas

```bash
# Detener el generador
docker stop api_respuestas

# Observar reintentos y DLQ en métricas
curl http://localhost:8001/stats/kafka

# Recuperar el generador
docker start api_respuestas
```

### Simular spike de tráfico

Cambiar en `docker-compose.yml`:
```yaml
- DELAY=0.01
```
Reiniciar y observar el throughput subir a ~66 req/s.

---

## Consultas implementadas

| Query | Cache Key | Descripción |
|---|---|---|
| Q1 | `count:{zona}:conf={c}` | Conteo de edificios en la zona |
| Q2 | `area:{zona}:conf={c}` | Área promedio y total |
| Q3 | `density:{zona}:conf={c}` | Densidad por km² |
| Q4 | `compare:density:{zA}:{zB}:conf={c}` | Comparación entre dos zonas |
| Q5 | `confidence_dist:{zona}:bins={b}` | Histograma de confianza |

---

## Experimentos y resultados clave

| Escenario | Throughput | Hit Rate | Retries | DLQ |
|---|---|---|---|---|
| SYNC base (Tarea 1) | 6.00 req/s | 92.25% | — | — |
| Kafka + 1 consumer | 8.89 req/s | 95.30% | 0 | 0 |
| Kafka + 3 consumers | 8.87 req/s | 85.07% | 0 | 0 |
| Kafka + 5 consumers | 9.39 req/s | 94.28% | 0 | 0 |
| Falla temporal | 9.49 req/s | 94.95% | 16 | 8 |
| Spike de tráfico | 66.30 req/s | 99.08% | 0 | 0 |
| Spike + falla | 67.77 req/s | 99.41% | 18 | 9 |

---

## Estructura del repositorio

```
tarea1-distribuidos/
├── api_cache/              # Proxy inverso del caché (FastAPI)
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── generador_respuestas/   # Cómputo sobre datos en RAM (FastAPI + Pandas)
│   ├── main.py
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── Z1.csv ... Z5.csv   # Generados por preparar_datos.py
│   └── preparar_datos.py
├── generador_trafico/      # Productor Kafka / cliente SYNC (Python)
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── kafka_consumer/         # Consumidor Kafka con reintentos y DLQ (Python)
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── metricas/               # API de métricas (FastAPI + NumPy)
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── preparar_datos.py       # Script de filtrado del dataset
└── README.md
```

---

## Zonas geográficas

| Zona | Sector | lat_min | lat_max | lon_min | lon_max |
|---|---|---|---|---|---|
| Z1 | Providencia | −33.445 | −33.420 | −70.640 | −70.600 |
| Z2 | Las Condes | −33.420 | −33.390 | −70.600 | −70.550 |
| Z3 | Maipú | −33.530 | −33.490 | −70.790 | −70.740 |
| Z4 | Santiago Centro | −33.460 | −33.430 | −70.670 | −70.630 |
| Z5 | Pudahuel | −33.470 | −33.430 | −70.810 | −70.760 |

---

## Tecnologías utilizadas

- **Python 3.12** — lenguaje principal
- **FastAPI** — microservicios HTTP asíncronos
- **Apache Kafka 7.5.0** — mensajería y desacoplamiento
- **Redis Alpine** — caché en memoria
- **Pandas / NumPy** — procesamiento de datos geoespaciales
- **Docker / Docker Compose** — orquestación de servicios

---


