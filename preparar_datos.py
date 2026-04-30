import pandas as pd
import numpy as np

print("Cargando dataset completo...")
df = pd.read_csv(
    "/home/lolnes/Downloads/967_buildings.csv.gz",
    usecols=["latitude", "longitude", "area_in_meters", "confidence"]
)
print(f"Total registros: {len(df):,}")

# Bounding boxes de las 5 zonas
zonas = {
    "Z1": (-33.445, -33.420, -70.640, -70.600),  # Providencia
    "Z2": (-33.420, -33.390, -70.600, -70.550),  # Las Condes
    "Z3": (-33.530, -33.490, -70.790, -70.740),  # Maipú
    "Z4": (-33.460, -33.430, -70.670, -70.630),  # Santiago Centro
    "Z5": (-33.470, -33.430, -70.810, -70.760),  # Pudahuel
}

for zona_id, (lat_min, lat_max, lon_min, lon_max) in zonas.items():
    filtrado = df[
        (df.latitude  >= lat_min) & (df.latitude  <= lat_max) &
        (df.longitude >= lon_min) & (df.longitude <= lon_max)
    ]
    print(f"{zona_id}: {len(filtrado):,} edificios")
    filtrado.to_csv(f"/home/lolnes/Desktop/tarea1-distribuidos/generador_respuestas/{zona_id}.csv", index=False)

print("Listo. CSVs guardados.")
