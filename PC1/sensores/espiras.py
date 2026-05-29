import zmq
import json
import time
import random
import os
from datetime import datetime, timezone, timedelta

def cargar_config():
    path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.json'))
    with open(path, encoding='utf-8') as f:
        return json.load(f)

config    = cargar_config()
IP_PC1    = config['ips']['PC1']
PUERTO    = config['puertos']['sensores_broker']
INTERVALO = config['sensores']['intervalo_espira']

filas          = config['ciudad']['filas']
columnas       = config['ciudad']['columnas']
intersecciones = [f"INT_{f}{c}" for f in filas for c in columnas]

context = zmq.Context()
socket  = context.socket(zmq.PUB)
socket.connect(f"tcp://{IP_PC1}:{PUERTO}")

time.sleep(1)

print("=" * 50)
print("  SENSOR ESPIRA INDUCTIVA - PC1")
print("=" * 50)
print(f"  Intersecciones: {len(intersecciones)}")
print(f"  Broker: {IP_PC1}:{PUERTO}")
print(f"  Intervalo: {INTERVALO}s")
print("=" * 50)

while True:
    for interseccion in intersecciones:
        codigo    = interseccion.split('_')[1]
        sensor_id = f"ESP-{codigo}"
        ts_inicio = datetime.now(timezone.utc)
        ts_fin    = ts_inicio + timedelta(seconds=INTERVALO)

        evento = {
            "sensor_id":          sensor_id,
            "tipo_sensor":        "espira_inductiva",
            "interseccion":       interseccion,
            "vehiculos_contados": random.randint(0, 25),
            "intervalo_segundos": INTERVALO,
            "timestamp_inicio":   ts_inicio.isoformat(),
            "timestamp_fin":      ts_fin.isoformat()
        }

        socket.send_multipart([b"espira", json.dumps(evento).encode()])
        print(f"[ESP] {sensor_id}: {evento['vehiculos_contados']} vehículos "
              f"en {INTERVALO}s @ {interseccion}")

    time.sleep(INTERVALO)
