import time
import requests
import numpy as np
import random
import os

URL_CACHE = "http://api_cache:8000"
ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
CONSULTAS = ["q1", "q2", "q3", "q5"] 
TIPO_DISTRIBUCION = os.getenv("DISTRIBUCION", "ZIPF") 

print(f"Iniciando Generador de Tráfico en modo: {TIPO_DISTRIBUCION}")
time.sleep(10) 
def elegir_zona_zipf():
    
        r = np.random.zipf(1.5)
        if r <= len(ZONAS):
            return ZONAS[r-1]

def elegir_zona_uniforme():
   
    return random.choice(ZONAS)

print("¡Comenzando bombardeo de consultas!")

try:
    while True:
       
        tipo_q = random.choice(CONSULTAS)
        
       
        if TIPO_DISTRIBUCION == "ZIPF":
            zona = elegir_zona_zipf()
        else:
            zona = elegir_zona_uniforme()
            
       
        confianza = random.choice([0.0, 0.5, 0.8])
        
       
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
            

        time.sleep(0.1)
        
except KeyboardInterrupt:
    print("Generador de tráfico detenido manualmente.")
