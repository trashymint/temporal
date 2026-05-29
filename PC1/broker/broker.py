import zmq
import json
import os

def cargar_config():
    path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.json'))
    with open(path, encoding='utf-8') as f:
        return json.load(f)

config = cargar_config()
PUERTO_SENSORES  = config['puertos']['sensores_broker']
PUERTO_ANALITICA = config['puertos']['broker_analitica']

context = zmq.Context()

# Recibe mensajes de los sensores (PUB → XSUB)
frontend = context.socket(zmq.XSUB)
frontend.bind(f"tcp://*:{PUERTO_SENSORES}")

# Reenvía mensajes a la analítica (XPUB → SUB)
backend = context.socket(zmq.XPUB)
backend.bind(f"tcp://*:{PUERTO_ANALITICA}")

print("=" * 50)
print("  BROKER ZMQ - PC1")
print("=" * 50)
print(f"  Escuchando sensores en  puerto {PUERTO_SENSORES}")
print(f"  Publicando analítica en puerto {PUERTO_ANALITICA}")
print("=" * 50)

zmq.proxy(frontend, backend)
