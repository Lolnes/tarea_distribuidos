import pandas as pd
import numpy as np
from fastapi import FastAPI
import os

app = FastAPI()

memoria_zonas = {}
areas_km2 = {"Z1": 14.4, "Z2": 99.4, "Z3": 133.0, "Z4": 22.4, "Z5": 197.0}

@app.on_event("startup")
def cargar_datos():
    print("Cargando dataset real Google Open Buildings en RAM...")
    base = os.path.dirname(__file__)
    for zona in areas_km2.keys():
        path = os.path.join(base, f"{zona}.csv")
        memoria_zonas[zona] = pd.read_csv(path)
        print(f"  {zona}: {len(memoria_zonas[zona]):,} edificios cargados")
    print("Dataset listo.")

@app.get("/")
def estado(): return {"status": "OK"}

@app.get("/q1/{zona_id}")
def q1_conteo(zona_id: str, confidence_min: float = 0.0):
    if zona_id not in memoria_zonas: return {"error": "Zona no existe"}
    df = memoria_zonas[zona_id]
    return {"conteo": int(len(df[df["confidence"] >= confidence_min]))}

@app.get("/q2/{zona_id}")
def q2_area(zona_id: str, confidence_min: float = 0.0):
    if zona_id not in memoria_zonas: return {"error": "Zona no existe"}
    df = memoria_zonas[zona_id][memoria_zonas[zona_id]["confidence"] >= confidence_min]
    if df.empty: return {"avg_area": 0, "total_area": 0, "n": 0}
    return {
        "avg_area": float(df["area_in_meters"].mean()),
        "total_area": float(df["area_in_meters"].sum()),
        "n": int(len(df))
    }

@app.get("/q3/{zona_id}")
def q3_densidad(zona_id: str, confidence_min: float = 0.0):
    if zona_id not in memoria_zonas: return {"error": "Zona no existe"}
    conteo = int(len(memoria_zonas[zona_id][memoria_zonas[zona_id]["confidence"] >= confidence_min]))
    return {"densidad_por_km2": conteo / areas_km2[zona_id]}

@app.get("/q4/{zona_a}/{zona_b}")
def q4_comparar(zona_a: str, zona_b: str, confidence_min: float = 0.0):
    da = q3_densidad(zona_a, confidence_min).get("densidad_por_km2", 0)
    db = q3_densidad(zona_b, confidence_min).get("densidad_por_km2", 0)
    return {"zone_a": da, "zone_b": db, "winner": zona_a if da > db else zona_b}

@app.get("/q5/{zona_id}")
def q5_histograma(zona_id: str, bins: int = 5):
    if zona_id not in memoria_zonas: return {"error": "Zona no existe"}
    counts, edges = np.histogram(memoria_zonas[zona_id]["confidence"], bins=bins, range=(0.0, 1.0))
    return {"distribucion": [{"bucket": i, "count": int(counts[i])} for i in range(bins)]}
