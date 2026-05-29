import zmq
import threading
import queue as Q
import json
import os
import time

def cargar_config():
    path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.json'))
    with open(path, encoding='utf-8') as f:
        return json.load(f)

config = cargar_config()
PUERTO_ENTRADA = config['puertos']['sensores_broker']
PUERTO_SALIDA  = config['puertos']['broker_analitica']

# Cola compartida entre hilos (thread-safe)
cola_mensajes = Q.Queue(maxsize=10000)

# Contadores para estadísticas
stats = {"recibidos": 0, "enviados": 0}
lock_stats = threading.Lock()

# ── Hilo 1: Receptor ─────────────────────────────────────────────────────────
def hilo_receptor():
    """
    Recibe mensajes de los sensores (PUB/connect → SUB/bind).
    Los coloca en la cola compartida para que el reenviador los procese.
    """
    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.bind(f"tcp://*:{PUERTO_ENTRADA}")
    sock.setsockopt_string(zmq.SUBSCRIBE, "")   # Aceptar todos los tópicos

    print(f"[BROKER-MT] Hilo RECEPTOR activo → puerto {PUERTO_ENTRADA}")

    while True:
        try:
            msg = sock.recv_multipart()
            cola_mensajes.put(msg)
            with lock_stats:
                stats["recibidos"] += 1
        except Exception as e:
            print(f"[BROKER-MT][RECEPTOR] Error: {e}")

# ── Hilo 2: Reenviador ────────────────────────────────────────────────────────
def hilo_reenviador():
    """
    Toma mensajes de la cola compartida y los reenvía a la analítica
    usando un socket PUB (bind). La analítica usa SUB/connect.
    """
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.bind(f"tcp://*:{PUERTO_SALIDA}")

    print(f"[BROKER-MT] Hilo REENVIADOR activo → puerto {PUERTO_SALIDA}")

    while True:
        try:
            msg = cola_mensajes.get()
            sock.send_multipart(msg)
            with lock_stats:
                stats["enviados"] += 1
        except Exception as e:
            print(f"[BROKER-MT][REENVIADOR] Error: {e}")

# ── Hilo 3: Estadísticas ──────────────────────────────────────────────────────
def hilo_estadisticas():
    """Imprime estadísticas cada 30 segundos para monitorear el rendimiento."""
    while True:
        time.sleep(30)
        with lock_stats:
            r = stats["recibidos"]
            e = stats["enviados"]
        print(f"[BROKER-MT] Recibidos: {r} | Enviados: {e} | En cola: {cola_mensajes.qsize()}")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  BROKER ZMQ MULTIHILO - PC1")
    print("=" * 55)
    print(f"  Entrada (SUB bind): puerto {PUERTO_ENTRADA}")
    print(f"  Salida  (PUB bind): puerto {PUERTO_SALIDA}")
    print(f"  Hilos: receptor + reenviador + estadísticas")
    print(f"  Cola máxima: 10.000 mensajes")
    print("=" * 55)

    threads = [
        threading.Thread(target=hilo_receptor,     daemon=True, name="Receptor"),
        threading.Thread(target=hilo_reenviador,    daemon=True, name="Reenviador"),
        threading.Thread(target=hilo_estadisticas,  daemon=True, name="Stats"),
    ]

    for t in threads:
        t.start()

    print("[BROKER-MT] Broker multihilo corriendo. Ctrl+C para detener.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[BROKER-MT] Deteniendo... Total recibidos: {stats['recibidos']} | Enviados: {stats['enviados']}")

if __name__ == "__main__":
    main()
