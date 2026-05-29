import zmq
import json
import os
import threading
import time
from datetime import datetime, timezone

def cargar_config():
    path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.json'))
    with open(path, encoding='utf-8') as f:
        return json.load(f)

config = cargar_config()
P      = config['puertos']

filas          = config['ciudad']['filas']
columnas       = config['ciudad']['columnas']
intersecciones = [f"INT_{f}{c}" for f in filas for c in columnas]

semaforos = {
    inter: {
        "FILA":    "VERDE",   # inicia en VERDE
        "CARRERA": "ROJO",    # inicia en ROJO (opuesto)
        "modo":        "NORMAL",
        "tiempo_verde": config['semaforos']['tiempo_verde_normal'],
        "motivo":      ""
    }
    for inter in intersecciones
}

lock_sem = threading.Lock()

def ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def ciclo_semaforo(interseccion):
    """
    Alterna los dos semáforos de la intersección:
      - Fase 1: FILA=VERDE, CARRERA=ROJO  durante tiempo_verde segundos
      - Fase 2: FILA=ROJO,  CARRERA=VERDE durante tiempo_verde segundos
    En modo PRIORIDAD el ciclo se pausa y ambas quedan fijas.
    """
    while True:
        with lock_sem:
            modo        = semaforos[interseccion]["modo"]
            tiempo      = semaforos[interseccion]["tiempo_verde"]

        if modo == "PRIORIDAD":
            time.sleep(2)
            continue

        with lock_sem:
            semaforos[interseccion]["FILA"]    = "VERDE"
            semaforos[interseccion]["CARRERA"] = "ROJO"
        print(f"[SEM {ts()}] {interseccion}  FILA=VERDE / CARRERA=ROJO  "
              f"(modo={modo}, {tiempo}s)")
        time.sleep(tiempo)

        with lock_sem:
            modo   = semaforos[interseccion]["modo"]
            tiempo = semaforos[interseccion]["tiempo_verde"]
        if modo == "PRIORIDAD":
            continue

        with lock_sem:
            semaforos[interseccion]["FILA"]    = "ROJO"
            semaforos[interseccion]["CARRERA"] = "VERDE"
        print(f"[SEM {ts()}] {interseccion}  FILA=ROJO  / CARRERA=VERDE "
              f"(modo={modo}, {tiempo}s)")
        time.sleep(tiempo)

def procesar_comando(comando):
    tipo = comando.get("tipo")

    if tipo == "cambio_estado":
        interseccion = comando.get("interseccion")
        estado       = comando.get("estado")
        tiempo_verde = comando.get("tiempo_verde",
                                   config['semaforos']['tiempo_verde_normal'])
        motivo       = comando.get("motivo", "")

        if interseccion not in semaforos:
            return

        with lock_sem:
            semaforos[interseccion]["modo"]        = estado
            semaforos[interseccion]["tiempo_verde"] = tiempo_verde
            semaforos[interseccion]["motivo"]      = motivo

        print(f"[SEM {ts()}] {interseccion}: modo → {estado}, "
              f"tiempo_verde={tiempo_verde}s, motivo={motivo}")

    elif tipo == "ola_verde":
        via    = comando.get("via", [])
        motivo = comando.get("motivo", "")

        with lock_sem:
            for inter in via:
                if inter in semaforos:
                    semaforos[inter]["FILA"]        = "VERDE"
                    semaforos[inter]["CARRERA"]     = "VERDE" 
                    semaforos[inter]["modo"]        = "PRIORIDAD"
                    semaforos[inter]["tiempo_verde"] = 999
                    semaforos[inter]["motivo"]      = motivo

        print(f"[SEM {ts()}] OLA VERDE — vía: {via} — motivo: {motivo}")
        for inter in via:
            print(f"           {inter}: FILA=VERDE / CARRERA=VERDE (PRIORIDAD)")

def main():
    context = zmq.Context()
    socket  = context.socket(zmq.PULL)
    socket.bind(f"tcp://*:{P['analitica_semaforos']}")

    print("=" * 60)
    print("  CONTROL DE SEMÁFOROS - PC2")
    print("=" * 60)
    print(f"  Puerto PULL: {P['analitica_semaforos']}")
    print(f"  Intersecciones: {len(intersecciones)}")
    print(f"  Semáforos por intersección: 2 (FILA + CARRERA)")
    print(f"  Total semáforos gestionados: {len(intersecciones) * 2}")
    print("=" * 60)

    for inter in intersecciones:
        t = threading.Thread(target=ciclo_semaforo, args=(inter,), daemon=True)
        t.start()

    print("[SEM] Ciclos automáticos iniciados\n")

    while True:
        try:
            comando = socket.recv_json()
            procesar_comando(comando)
        except Exception as e:
            print(f"[SEM] Error: {e}")

if __name__ == "__main__":
    main()
