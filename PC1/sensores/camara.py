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
INTERVALO = config['sensores']['intervalo_camara']

filas         = config['ciudad']['filas']
columnas      = config['ciudad']['columnas']
intersecciones = [f"INT_{f}{c}" for f in filas for c in columnas]

context = zmq.Context()
socket  = context.socket(zmq.PUB)
socket.connect(f"tcp://{IP_PC1}:{PUERTO}")

time.sleep(1)  # Tiempo para que se establezca la conexión con el broker

print("=" * 50)
print("  SENSOR CÁMARA - PC1")
print("=" * 50)
print(f"  Intersecciones: {len(intersecciones)}")
print(f"  Broker: {IP_PC1}:{PUERTO}")
print(f"  Intervalo: {INTERVALO}s")
print("=" * 50)

while True:
    for interseccion in intersecciones:
        codigo = interseccion.split('_')[1]
        sensor_id = f"CAM-{codigo}"

        evento = {
            "sensor_id":          sensor_id,
            "tipo_sensor":        "camara",
            "interseccion":       interseccion,
            "volumen":            random.randint(0, 15),
            "velocidad_promedio": round(random.uniform(10, 60), 1),
            "timestamp":          datetime.now(timezone.utc).isoformat()
        }

        socket.send_multipart([b"camara", json.dumps(evento).encode()])
        print(f"[CAM] {sensor_id}: volumen={evento['volumen']} veh, "
              f"vel={evento['velocidad_promedio']} km/h @ {interseccion}")

    time.sleep(INTERVALO)
