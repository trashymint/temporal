import zmq
import json
import time
import random
import os
from datetime import datetime, timezone

def cargar_config():
    path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.json'))
    with open(path, encoding='utf-8') as f:
        return json.load(f)

config    = cargar_config()
IP_PC1    = config['ips']['PC1']
PUERTO    = config['puertos']['sensores_broker']
INTERVALO = config['sensores']['intervalo_gps']

filas          = config['ciudad']['filas']
columnas       = config['ciudad']['columnas']
intersecciones = [f"INT_{f}{c}" for f in filas for c in columnas]

context = zmq.Context()
socket  = context.socket(zmq.PUB)
socket.connect(f"tcp://{IP_PC1}:{PUERTO}")

time.sleep(1)

print("=" * 50)
print("  SENSOR GPS - PC1")
print("=" * 50)
print(f"  Intersecciones: {len(intersecciones)}")
print(f"  Broker: {IP_PC1}:{PUERTO}")
print(f"  Intervalo: {INTERVALO}s")
print("=" * 50)

def nivel_congestion(velocidad):
    if velocidad < 10:
        return "ALTA"
    elif velocidad <= 39:
        return "NORMAL"
    else:
        return "BAJA"

while True:
    for interseccion in intersecciones:
        codigo    = interseccion.split('_')[1]
        sensor_id = f"GPS-{codigo}"
        velocidad = round(random.uniform(5, 65), 1)

        evento = {
            "sensor_id":          sensor_id,
            "tipo_sensor":        "gps",
            "interseccion":       interseccion,
            "nivel_congestion":   nivel_congestion(velocidad),
            "velocidad_promedio": velocidad,
            "timestamp":          datetime.now(timezone.utc).isoformat()
        }

        socket.send_multipart([b"gps", json.dumps(evento).encode()])
        print(f"[GPS] {sensor_id}: vel={velocidad} km/h, "
              f"congestion={evento['nivel_congestion']} @ {interseccion}")

    time.sleep(INTERVALO)
