from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np

app = FastAPI()

registro_hits = 0
registro_misses = 0
latencias = []


class Metrica(BaseModel):
    tipo: str  
    latencia: float
    consulta: str
    zona: str

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
        "latencia_p95": round(p95, 4)
    }
