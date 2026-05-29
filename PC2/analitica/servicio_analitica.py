import zmq
import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone

def cargar_config():
    path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.json'))
    with open(path, encoding='utf-8') as f:
        return json.load(f)

config = cargar_config()
IP_PC1 = config['ips']['PC1']
IP_PC2 = config['ips']['PC2']
IP_PC3 = config['ips']['PC3']
P      = config['puertos']
REGLAS = config['reglas']

# ── Estado compartido ─────────────────────────────────────────────────────────
lock_estado = threading.Lock()
lock_pc3    = threading.Lock()

filas          = config['ciudad']['filas']
columnas       = config['ciudad']['columnas']
intersecciones = [f"INT_{f}{c}" for f in filas for c in columnas]

estado_intersecciones = {
    inter: {
        "cola":           None,
        "velocidad":      None,
        "densidad":       None,
        "congestion_gps": None,
        "estado":         "NORMAL",
        "sem_fila":       "VERDE",
        "sem_carrera":    "ROJO"
    }
    for inter in intersecciones
}

# ── Health check y buffer de sincronización ───────────────────────────────────
pc3_disponible   = True
pc3_estaba_caido = False
buffer_sync      = deque(maxlen=5000)   # eventos pendientes de enviar a PC3

# ── Reglas del sistema ────────────────────────────────────────────────────────
REGLAS_DESCRIPCION = f"""
╔══════════════════════════════════════════════════════════╗
║           REGLAS DEL SISTEMA DE TRÁFICO                  ║
╠══════════════════════════════════════════════════════════╣
║  Estado NORMAL:                                          ║
║    Cola (Q) < {REGLAS['max_cola_normal']} vehículos                            ║
║    Velocidad (Vp) > {REGLAS['min_velocidad_normal']} km/h                         ║
║    Densidad (D) < {REGLAS['max_densidad_normal']} veh/min                          ║
║                                                          ║
║  Estado CONGESTION (correlación: ≥2 condiciones):        ║
║    Cola (Q) ≥ {REGLAS['max_cola_normal']} vehículos                            ║
║    Velocidad (Vp) ≤ {REGLAS['max_velocidad_congestion']} km/h                         ║
║    Densidad (D) ≥ {REGLAS['max_densidad_normal']} veh/min                          ║
║                                                          ║
║  Estado PRIORIDAD (manual):                              ║
║    Ola verde activada por usuario (ej. ambulancia)       ║
║                                                          ║
║  Tiempos de semáforo:                                    ║
║    Normal:     {config['semaforos']['tiempo_verde_normal']}s por fase (FILA/CARRERA)             ║
║    Congestion: {config['semaforos']['tiempo_verde_congestion']}s por fase                          ║
╚══════════════════════════════════════════════════════════╝
"""

def ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def evaluar_estado(datos):
    """
    CONGESTION requiere correlación: al menos 2 de 3 condiciones deben cumplirse.
    Esto evita falsos positivos por un solo sensor con datos ruidosos.
    """
    if datos["estado"] == "PRIORIDAD":
        return "PRIORIDAD"

    cola      = datos.get("cola")
    velocidad = datos.get("velocidad")
    densidad  = datos.get("densidad")

    violaciones = 0
    detalle     = []

    if cola is not None and cola >= REGLAS["max_cola_normal"]:
        violaciones += 1
        detalle.append(f"Q={cola}≥{REGLAS['max_cola_normal']}")

    if velocidad is not None and velocidad <= REGLAS["max_velocidad_congestion"]:
        violaciones += 1
        detalle.append(f"Vp={velocidad}≤{REGLAS['max_velocidad_congestion']}")

    if densidad is not None and densidad >= REGLAS["max_densidad_normal"]:
        violaciones += 1
        detalle.append(f"D={densidad}≥{REGLAS['max_densidad_normal']}")

    return ("CONGESTION", detalle) if violaciones >= 2 else ("NORMAL", detalle)

def imprimir_cambio_estado(interseccion, estado_anterior, nuevo_estado, datos, detalle):
    print(f"\n[ANALITICA {ts()}] {'='*52}")
    print(f"[ANALITICA] CAMBIO DE ESTADO: {interseccion}")
    print(f"[ANALITICA] {estado_anterior} → {nuevo_estado}")
    print(f"[ANALITICA] Métricas actuales:")
    print(f"[ANALITICA]   Cola (Q):       {datos.get('cola','N/A')} veh    | umbral ≥ {REGLAS['max_cola_normal']}")
    print(f"[ANALITICA]   Velocidad (Vp): {datos.get('velocidad','N/A')} km/h  | umbral ≤ {REGLAS['max_velocidad_congestion']}")
    print(f"[ANALITICA]   Densidad (D):   {datos.get('densidad','N/A')} v/min | umbral ≥ {REGLAS['max_densidad_normal']}")
    if detalle:
        print(f"[ANALITICA] Condiciones de congestión detectadas ({len(detalle)}/3):")
        for d in detalle:
            print(f"[ANALITICA] {d}")
    if nuevo_estado == "CONGESTION":
        t = config['semaforos']['tiempo_verde_congestion']
        print(f"[ANALITICA] Acción: Extender verde a {t}s por fase")
    elif nuevo_estado == "NORMAL":
        t = config['semaforos']['tiempo_verde_normal']
        print(f"[ANALITICA] Acción: Verde normal {t}s por fase")
    print(f"[ANALITICA] {'='*52}\n")

def procesar_evento(topic, evento, socket_db_principal, socket_db_replica, socket_semaforos):
    interseccion = evento.get("interseccion")
    if not interseccion or interseccion not in estado_intersecciones:
        return

    with lock_estado:
        datos          = estado_intersecciones[interseccion]
        estado_anterior = datos["estado"]

        if topic == "camara":
            datos["cola"]      = evento.get("volumen")
            datos["velocidad"] = evento.get("velocidad_promedio")

        elif topic in ("espira", "espira_inductiva"):
            vehiculos = evento.get("vehiculos_contados", 0)
            intervalo = evento.get("intervalo_segundos", 30)
            datos["densidad"] = round(vehiculos / intervalo * 60, 2)

        elif topic == "gps":
            datos["velocidad"]      = evento.get("velocidad_promedio")
            datos["congestion_gps"] = evento.get("nivel_congestion")

        resultado, detalle = evaluar_estado(datos)
        nuevo_estado = resultado
        datos["estado"] = nuevo_estado

        if nuevo_estado == "CONGESTION":
            datos["sem_fila"]    = "VERDE"
            datos["sem_carrera"] = "ROJO"

        estado_detectado = nuevo_estado

    with lock_pc3:
        estado_pc3 = "OK" if pc3_disponible else "CAIDO"
    print(f"[ANALITICA {ts()}] {interseccion}: {estado_detectado} | "
          f"Q={datos['cola']} Vp={datos['velocidad']} D={datos['densidad']} | "
          f"FILA={datos['sem_fila']} CAR={datos['sem_carrera']} | PC3:{estado_pc3}")

    if nuevo_estado != estado_anterior:
        imprimir_cambio_estado(interseccion, estado_anterior, nuevo_estado, datos, detalle)

    evento_db = dict(evento)
    evento_db["estado_detectado"] = estado_detectado
    evento_db["topic"]            = topic

    with lock_pc3:
        pc3_up = pc3_disponible

    if pc3_up:
        socket_db_principal.send_json(evento_db)
    else:
        buffer_sync.append(dict(evento_db)) 

    socket_db_replica.send_json(evento_db)

    if nuevo_estado != estado_anterior:
        tiempo_verde = (
            config['semaforos']['tiempo_verde_congestion']
            if nuevo_estado == "CONGESTION"
            else config['semaforos']['tiempo_verde_normal']
        )
        comando = {
            "tipo":         "cambio_estado",
            "interseccion": interseccion,
            "estado":       nuevo_estado,
            "tiempo_verde": tiempo_verde,
            "motivo":       nuevo_estado,
            "timestamp":    datetime.now(timezone.utc).isoformat()
        }
        socket_semaforos.send_json(comando)

def sincronizar_con_principal(socket_db_principal):
    global buffer_sync
    pendientes = len(buffer_sync)
    if pendientes == 0:
        print("[SYNC] No hay eventos pendientes")
        return

    print(f"[SYNC] Sincronizando {pendientes} eventos a PC3...")
    count = 0
    while buffer_sync:
        try:
            evento = buffer_sync.popleft()
            evento["sync_recovery"] = True
            socket_db_principal.send_json(evento)
            count += 1
        except Exception as e:
            print(f"[SYNC] Error en evento {count}: {e}")
            break
    print(f"[SYNC] {count} eventos sincronizados a BD Principal (PC3)")

# ── Health check de PC3 ───────────────────────────────────────────────────────
def hilo_healthcheck(context, socket_db_principal):
    global pc3_disponible, pc3_estaba_caido

    while True:
        try:
            s = context.socket(zmq.REQ)
            s.setsockopt(zmq.RCVTIMEO, 2000)
            s.setsockopt(zmq.LINGER, 0)
            s.connect(f"tcp://{IP_PC3}:{P['db_principal_query']}")
            s.send_json({"tipo": "ping"})
            s.recv_json()
            s.close()

            with lock_pc3:
                recuperado       = not pc3_disponible
                pc3_disponible   = True

            if recuperado:
                print(f"\n[HEALTHCHECK {ts()}] PC3 recuperado — sincronizando...")
                pc3_estaba_caido = False
                sincronizar_con_principal(socket_db_principal)

        except Exception:
            try:
                s.close()
            except Exception:
                pass
            with lock_pc3:
                if pc3_disponible:
                    print(f"\n[HEALTHCHECK {ts()}]  PC3 caído — redirigiendo a BD Réplica")
                    pc3_estaba_caido = True
                pc3_disponible = False

        time.sleep(5)

# ── Hilo monitoreo REP ────────────────────────────────────────────────────────
def hilo_monitoreo(context, socket_semaforos, socket_db_principal, socket_db_replica):
    socket_rep = context.socket(zmq.REP)
    socket_rep.bind(f"tcp://*:{P['analitica_monitoreo']}")
    print(f"[ANALITICA] Esperando comandos de monitoreo en puerto {P['analitica_monitoreo']}")

    while True:
        try:
            comando = socket_rep.recv_json()
            tipo    = comando.get("tipo")

            if tipo == "estado_actual":
                interseccion = comando.get("interseccion")
                with lock_estado:
                    if interseccion and interseccion in estado_intersecciones:
                        resp = {"exito": True, "datos": estado_intersecciones[interseccion]}
                    else:
                        resp = {"exito": True, "datos": dict(estado_intersecciones)}

            elif tipo == "priorizar":
                via    = comando.get("via", [])
                motivo = comando.get("motivo", "AMBULANCIA")
                with lock_estado:
                    for inter in via:
                        if inter in estado_intersecciones:
                            estado_intersecciones[inter]["estado"]      = "PRIORIDAD"
                            estado_intersecciones[inter]["sem_fila"]    = "VERDE"
                            estado_intersecciones[inter]["sem_carrera"] = "VERDE"

                cmd_ola = {
                    "tipo":      "ola_verde",
                    "via":       via,
                    "motivo":    motivo,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                socket_semaforos.send_json(cmd_ola)

                ev_prioridad = {
                    "topic":            "prioridad",
                    "tipo_sensor":      "manual",
                    "interseccion":     via[0] if via else "",
                    "via":              via,
                    "motivo":           motivo,
                    "estado_detectado": "PRIORIDAD",
                    "timestamp":        datetime.now(timezone.utc).isoformat()
                }
                with lock_pc3:
                    pc3_up = pc3_disponible
                if pc3_up:
                    socket_db_principal.send_json(ev_prioridad)
                else:
                    buffer_sync.append(dict(ev_prioridad))
                socket_db_replica.send_json(ev_prioridad)

                print(f"[ANALITICA {ts()}] OLA VERDE activada en {via} — motivo: {motivo}")
                resp = {"exito": True, "mensaje": f"Ola verde activada en {via}"}

            elif tipo == "ping":
                resp = {"exito": True, "mensaje": "pong"}

            else:
                resp = {"exito": False, "error": f"Tipo desconocido: {tipo}"}

            socket_rep.send_json(resp)

        except Exception as e:
            try:
                socket_rep.send_json({"exito": False, "error": str(e)})
            except Exception:
                pass

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(REGLAS_DESCRIPCION)

    context = zmq.Context()

    socket_sub = context.socket(zmq.SUB)
    socket_sub.connect(f"tcp://{IP_PC1}:{P['broker_analitica']}")
    socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")

    socket_db_principal = context.socket(zmq.PUSH)
    socket_db_principal.connect(f"tcp://{IP_PC3}:{P['analitica_db_principal']}")

    socket_db_replica = context.socket(zmq.PUSH)
    socket_db_replica.connect(f"tcp://{IP_PC2}:{P['analitica_db_replica']}")

    socket_semaforos = context.socket(zmq.PUSH)
    socket_semaforos.connect(f"tcp://{IP_PC2}:{P['analitica_semaforos']}")

    print("=" * 60)
    print("  SERVICIO DE ANALÍTICA - PC2")
    print("=" * 60)
    print(f"  Broker (PC1):       {IP_PC1}:{P['broker_analitica']}")
    print(f"  BD Principal (PC3): {IP_PC3}:{P['analitica_db_principal']}")
    print(f"  BD Réplica (PC2):   {IP_PC2}:{P['analitica_db_replica']}")
    print(f"  Semáforos (PC2):    {IP_PC2}:{P['analitica_semaforos']}")
    print(f"  Health check PC3:   cada 5s")
    print("=" * 60)

    # Hilo monitoreo REP
    threading.Thread(
        target=hilo_monitoreo,
        args=(context, socket_semaforos, socket_db_principal, socket_db_replica),
        daemon=True
    ).start()

    # Hilo health check
    threading.Thread(
        target=hilo_healthcheck,
        args=(context, socket_db_principal),
        daemon=True
    ).start()

    # Bucle principal: procesar eventos de sensores
    print("[ANALITICA] Esperando eventos de sensores...\n")
    while True:
        try:
            partes = socket_sub.recv_multipart()
            if len(partes) != 2:
                continue
            topic  = partes[0].decode()
            evento = json.loads(partes[1].decode())
            procesar_evento(topic, evento, socket_db_principal,
                            socket_db_replica, socket_semaforos)
        except json.JSONDecodeError as e:
            print(f"[ANALITICA] Error JSON: {e}")
        except Exception as e:
            print(f"[ANALITICA] Error: {e}")

if __name__ == "__main__":
    main()
