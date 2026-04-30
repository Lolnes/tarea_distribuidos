from fastapi import FastAPI
import redis
import httpx
import json
import time  

app = FastAPI()

r = redis.Redis(host="sistema_cache", port=6379, db=0, decode_responses=True)
URL_RESPUESTAS = "http://api_respuestas:8000"
URL_METRICAS = "http://api_metricas:8000/registrar"

@app.get("/")
def estado(): return {"status": "API de Caché online"}

async def enviar_metrica(tipo, latencia, url):
    
    async with httpx.AsyncClient() as client:
        payload = {"tipo": tipo, "latencia": latencia, "consulta": "Q", "zona": "Z"}
        try:
            await client.post(URL_METRICAS, json=payload)
        except:
            pass

async def fetch_and_cache(cache_key: str, endpoint_url: str):
    inicio = time.time()
    cached_data = r.get(cache_key)
    
    if cached_data:
        latencia = time.time() - inicio
        await enviar_metrica("HIT", latencia, endpoint_url)
        
        respuesta = json.loads(cached_data)
        respuesta["origen"] = "CACHE HIT"
        return respuesta
    else:
        async with httpx.AsyncClient() as client:
            res = await client.get(endpoint_url)
            latencia = time.time() - inicio
            await enviar_metrica("MISS", latencia, endpoint_url)
            
            if res.status_code == 200:
                data = res.json()
                r.setex(cache_key, 300, json.dumps(data))
                data["origen"] = "CACHE MISS"
                return data
            return {"error": "Error interno"}


@app.get("/q1/{zona_id}")
async def proxy_q1(zona_id: str, confidence_min: float = 0.0):
    cache_key = f"count:{zona_id}:conf={confidence_min}"
    url = f"{URL_RESPUESTAS}/q1/{zona_id}?confidence_min={confidence_min}"
    return await fetch_and_cache(cache_key, url)

@app.get("/q2/{zona_id}")
async def proxy_q2(zona_id: str, confidence_min: float = 0.0):
    cache_key = f"area:{zona_id}:conf={confidence_min}"
    url = f"{URL_RESPUESTAS}/q2/{zona_id}?confidence_min={confidence_min}"
    return await fetch_and_cache(cache_key, url)

@app.get("/q3/{zona_id}")
async def proxy_q3(zona_id: str, confidence_min: float = 0.0):
    cache_key = f"density:{zona_id}:conf={confidence_min}"
    url = f"{URL_RESPUESTAS}/q3/{zona_id}?confidence_min={confidence_min}"
    return await fetch_and_cache(cache_key, url)

@app.get("/q4/{zona_a}/{zona_b}")
async def proxy_q4(zona_a: str, zona_b: str, confidence_min: float = 0.0):
    cache_key = f"compare:density:{zona_a}:{zona_b}:conf={confidence_min}"
    url = f"{URL_RESPUESTAS}/q4/{zona_a}/{zona_b}?confidence_min={confidence_min}"
    return await fetch_and_cache(cache_key, url)
    
@app.get("/q5/{zona_id}")
async def proxy_q5(zona_id: str, bins: int = 5, confidence_min: float = 0.0):
    cache_key = f"confidence_dist:{zona_id}:bins={bins}"
    url = f"{URL_RESPUESTAS}/q5/{zona_id}?bins={bins}"
    return await fetch_and_cache(cache_key, url)
