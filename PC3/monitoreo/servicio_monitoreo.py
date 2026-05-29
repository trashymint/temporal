import zmq
import json
import os
import time

def cargar_config():
    path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.json'))
    with open(path, encoding='utf-8') as f:
        return json.load(f)

config = cargar_config()
IP_PC2 = config['ips']['PC2']
IP_PC3 = config['ips']['PC3']
P      = config['puertos']

TIMEOUT_MS = 3000

# ── Helpers de sockets REQ ────────────────────────────────────────────────────
def crear_socket_req(context, ip, puerto):
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(f"tcp://{ip}:{puerto}")
    return s

def enviar_query_db(context, peticion):
    """
    Intenta primero la BD Principal (PC3).
    Si no responde en el timeout, usa la BD Replica (PC2).
    """
    try:
        s = crear_socket_req(context, IP_PC3, P['db_principal_query'])
        s.send_json(peticion)
        resp = s.recv_json()
        s.close()
        return resp, "principal"
    except zmq.error.Again:
        s.close()
        print("[MONITOREO] PC3 no disponible — usando BD Replica (PC2)...")

    # Fallback a BD Replica
    try:
        s = crear_socket_req(context, IP_PC2, P['db_replica_query'])
        s.send_json(peticion)
        resp = s.recv_json()
        s.close()
        return resp, "replica"
    except zmq.error.Again:
        s.close()
        return {"exito": False, "error": "Ambas bases de datos no disponibles"}, "ninguna"

def enviar_comando_analitica(context, comando):
    "Envía un comando al servicio de analítica (PC2) y espera respuesta."
    try:
        s = crear_socket_req(context, IP_PC2, P['analitica_monitoreo'])
        s.send_json(comando)
        resp = s.recv_json()
        s.close()
        return resp
    except zmq.error.Again:
        s.close()
        return {"exito": False, "error": "Analítica (PC2) no disponible"}

def imprimir_resultados(resp, fuente):
    if not resp.get("exito"):
        print(f"  Error: {resp.get('error', 'desconocido')}")
        return

    datos = resp.get("datos", [])
    print(f"  Fuente: BD {fuente.upper()}  |  Registros encontrados: {len(datos)}")

    if isinstance(datos, dict) and not datos.get("exito") is False:
        if all(isinstance(v, dict) and "estado" in v for v in datos.values()):
            for inter, est in datos.items():
                print(f"  {inter}: {est['estado']} | "
                      f"FILA={est.get('sem_fila','?')} CARRERA={est.get('sem_carrera','?')} | "
                      f"cola={est['cola']} vel={est['velocidad']} dens={est['densidad']}")
            return

    for item in datos[:20]:  # Limitar a 20 para no saturar consola
        linea = " | ".join(f"{k}={v}" for k, v in item.items()
                           if k not in ("id", "created_at"))
        print(f"  {linea}")
    if len(datos) > 20:
        print(f"  ... y {len(datos) - 20} registros más")

AYUDA = """
╔══════════════════════════════════════════════════════╗
║          MONITOREO Y CONSULTA - PC3                  ║
╠══════════════════════════════════════════════════════╣
║  Comandos disponibles:                               ║
║                                                      ║
║  estado <INT>                                        ║
║    Ej: estado INT_C3                                 ║
║    → Estado actual de una intersección               ║
║                                                      ║
║  historico <INT> <desde> <hasta>                     ║
║    Ej: historico INT_C3 2026-05-26 08:00:00          ║
║                        2026-05-26 10:00:00           ║
║    → Eventos históricos en rango de tiempo           ║
║                                                      ║
║  semaforos                                           ║
║    → Historial de cambios de semáforo                ║
║                                                      ║
║  priorizar <INT_inicio> <INT_fin>                    ║
║    Ej: priorizar INT_A1 INT_A4                       ║
║    → Activa ola verde en la vía (A1→A2→A3→A4)        ║
║                                                      ║
║  todos                                               ║
║    → Estado actual de todas las intersecciones       ║
║                                                      ║
║  ayuda  → Muestra este menú                          ║
║  salir  → Sale del programa                          ║
╚══════════════════════════════════════════════════════╝
"""

def parsear_via(inter_inicio, inter_fin, config):
    """
    Genera la lista de intersecciones entre inter_inicio e inter_fin
    asumiendo misma fila o misma columna.
    Ej: INT_A1 → INT_A4  →  [INT_A1, INT_A2, INT_A3, INT_A4]
    """
    filas    = config['ciudad']['filas']
    columnas = config['ciudad']['columnas']

    def parse(inter):
        codigo = inter.split('_')[1]
        fila   = codigo[0]
        col    = int(codigo[1:])
        return fila, col

    f1, c1 = parse(inter_inicio)
    f2, c2 = parse(inter_fin)
    via = []

    if f1 == f2:
        col_min, col_max = min(c1, c2), max(c1, c2)
        for c in range(col_min, col_max + 1):
            if c in columnas:
                via.append(f"INT_{f1}{c}")
    else:
        fila_min_idx = min(filas.index(f1), filas.index(f2))
        fila_max_idx = max(filas.index(f1), filas.index(f2))
        col = c1
        for fi in range(fila_min_idx, fila_max_idx + 1):
            via.append(f"INT_{filas[fi]}{col}")

    return via

REGLAS_INFO = f"""
╔══════════════════════════════════════════════════════════╗
║           SISTEMA DE GESTIÓN DE TRÁFICO                  ║
╠══════════════════════════════════════════════════════════╣
║  REGLAS DE TRÁFICO (correlación ≥2 condiciones):         ║
║                                                          ║
║  NORMAL:     Q<{config['reglas']['max_cola_normal']}  AND  Vp>{config['reglas']['min_velocidad_normal']}km/h  AND  D<{config['reglas']['max_densidad_normal']}veh/min        ║
║  CONGESTION: Q≥{config['reglas']['max_cola_normal']}  OR   Vp≤{config['reglas']['max_velocidad_congestion']}km/h  OR   D≥{config['reglas']['max_densidad_normal']}veh/min        ║
║             (mínimo 2 de 3 condiciones)                  ║
║  PRIORIDAD:  Comando manual (ambulancia/emergencia)       ║
║                                                          ║
║  Semáforos por intersección: FILA (calle) + CARRERA      ║
║  Tiempo verde normal:     {config['semaforos']['tiempo_verde_normal']}s                           ║
║  Tiempo verde congestión: {config['semaforos']['tiempo_verde_congestion']}s                           ║
║                                                          ║
║  FALLO PC3: Failover automático a BD Réplica (PC2)       ║
║  RECUPERACIÓN: Sincronización automática al reconectar   ║
╚══════════════════════════════════════════════════════════╝
"""

def main():
    context = zmq.Context()

    print(REGLAS_INFO)
    print(AYUDA)
    print(f"  BD Principal (PC3): {IP_PC3}:{P['db_principal_query']}")
    print(f"  BD Réplica   (PC2): {IP_PC2}:{P['db_replica_query']}")
    print(f"  Analítica    (PC2): {IP_PC2}:{P['analitica_monitoreo']}\n")

    while True:
        try:
            entrada = input("monitoreo> ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n[MONITOREO] Cerrando...")
            break

        if not entrada:
            continue

        partes = entrada.split()
        cmd    = partes[0].lower()

        if cmd == "estado" and len(partes) >= 2:
            interseccion = partes[1].upper()
            print(f"\n[MONITOREO] Consultando estado actual de {interseccion}...")

            resp = enviar_comando_analitica(context, {
                "tipo":         "estado_actual",
                "interseccion": interseccion
            })
            if resp.get("exito"):
                datos = resp.get("datos", {})
                if isinstance(datos, dict) and "estado" in datos:
                    print(f"  {interseccion}: estado={datos['estado']}")
                    print(f"    Semáforo FILA(calle):    {datos.get('sem_fila','?')}")
                    print(f"    Semáforo CARRERA(carrera):{datos.get('sem_carrera','?')}")
                    print(f"    Cola={datos['cola']} veh | "
                          f"Velocidad={datos['velocidad']} km/h | "
                          f"Densidad={datos['densidad']} veh/min")
                else:
                    imprimir_resultados(resp, "analítica")
            else:
                # Fallback a BD replica
                resp_db, fuente = enviar_query_db(context, {
                    "tipo":         "estado_actual",
                    "interseccion": interseccion
                })
                imprimir_resultados(resp_db, fuente)

        elif cmd == "historico" and len(partes) >= 5:
            interseccion = partes[1].upper()
            desde        = f"{partes[2]} {partes[3]}"
            hasta        = f"{partes[4]} {partes[5]}" if len(partes) >= 6 else f"{partes[4]} 23:59:59"

            print(f"\n[MONITOREO] Histórico de {interseccion} desde {desde} hasta {hasta}...")
            resp, fuente = enviar_query_db(context, {
                "tipo":         "historico",
                "interseccion": interseccion,
                "desde":        desde,
                "hasta":        hasta
            })
            imprimir_resultados(resp, fuente)

        elif cmd == "semaforos":
            print("\n[MONITOREO] Consultando historial de semáforos...")
            resp, fuente = enviar_query_db(context, {"tipo": "semaforos"})
            imprimir_resultados(resp, fuente)

        elif cmd == "priorizar" and len(partes) >= 3:
            inter_inicio = partes[1].upper()
            inter_fin    = partes[2].upper()
            via          = parsear_via(inter_inicio, inter_fin, config)

            if not via:
                print("  Error: no se pudo determinar la vía entre las intersecciones")
                continue

            print(f"\n[MONITOREO] Activando OLA VERDE en: {via}")
            t_inicio = time.time()
            resp = enviar_comando_analitica(context, {
                "tipo":   "priorizar",
                "via":    via,
                "motivo": "AMBULANCIA"
            })
            t_fin = time.time()
            latencia_ms = (t_fin - t_inicio) * 1000
            if resp.get("exito"):
                print(f"  OK: {resp.get('mensaje', '')}")
                print(f"  ⏱  Latencia (solicitud → confirmación analítica): {latencia_ms:.1f} ms")
            else:
                print(f"  Error: {resp.get('error')}")

        elif cmd == "todos":
            print("\n[MONITOREO] Estado de todas las intersecciones...")
            resp = enviar_comando_analitica(context, {
                "tipo": "estado_actual"
            })
            imprimir_resultados(resp, "analítica")

        elif cmd == "ayuda":
            print(AYUDA)

        elif cmd == "salir":
            print("[MONITOREO] Cerrando...")
            break

        else:
            print(f"  Comando no reconocido: '{entrada}'")
            print("  Escribe 'ayuda' para ver los comandos disponibles")

        print()

if __name__ == "__main__":
    main()
