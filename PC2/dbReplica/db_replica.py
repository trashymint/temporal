import zmq
import json
import os
import mysql.connector
from datetime import datetime, timezone

def cargar_config():
    path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.json'))
    with open(path, encoding='utf-8') as f:
        return json.load(f)

config   = cargar_config()
P        = config['puertos']
DB_CONF  = config['mysql_pc2']

# ── Conexión MySQL ────────────────────────────────────────────────────────────
def get_conn():
    return mysql.connector.connect(**DB_CONF)

def inicializar_bd():
    conn   = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS eventos_camara (
            id               INT AUTO_INCREMENT PRIMARY KEY,
            sensor_id        VARCHAR(30),
            interseccion     VARCHAR(15),
            volumen          INT,
            velocidad_promedio FLOAT,
            estado_detectado VARCHAR(20),
            timestamp        DATETIME,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS eventos_espira (
            id               INT AUTO_INCREMENT PRIMARY KEY,
            sensor_id        VARCHAR(30),
            interseccion     VARCHAR(15),
            vehiculos_contados INT,
            intervalo_segundos INT,
            estado_detectado VARCHAR(20),
            timestamp_inicio DATETIME,
            timestamp_fin    DATETIME,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS eventos_gps (
            id               INT AUTO_INCREMENT PRIMARY KEY,
            sensor_id        VARCHAR(30),
            interseccion     VARCHAR(15),
            nivel_congestion VARCHAR(20),
            velocidad_promedio FLOAT,
            estado_detectado VARCHAR(20),
            timestamp        DATETIME,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cambios_semaforo (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            interseccion VARCHAR(15),
            color        VARCHAR(10),
            modo         VARCHAR(20),
            motivo       VARCHAR(50),
            timestamp    DATETIME,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("[BD REPLICA] Tablas verificadas/creadas correctamente")

def ts_parse(valor):
    """Convierte ISO timestamp a formato MySQL DATETIME."""
    if not valor:
        return None
    try:
        return datetime.fromisoformat(valor.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

# ── Inserción de eventos ──────────────────────────────────────────────────────
def guardar_evento(cursor, evento):
    topic  = evento.get("topic", evento.get("tipo_sensor", ""))
    estado = evento.get("estado_detectado", "NORMAL")

    if topic == "camara":
        cursor.execute(
            "INSERT INTO eventos_camara "
            "(sensor_id, interseccion, volumen, velocidad_promedio, estado_detectado, timestamp) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                evento.get("sensor_id"),
                evento.get("interseccion"),
                evento.get("volumen"),
                evento.get("velocidad_promedio"),
                estado,
                ts_parse(evento.get("timestamp"))
            )
        )

    elif topic == "espira" or topic == "espira_inductiva":
        cursor.execute(
            "INSERT INTO eventos_espira "
            "(sensor_id, interseccion, vehiculos_contados, intervalo_segundos, "
            "estado_detectado, timestamp_inicio, timestamp_fin) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                evento.get("sensor_id"),
                evento.get("interseccion"),
                evento.get("vehiculos_contados"),
                evento.get("intervalo_segundos"),
                estado,
                ts_parse(evento.get("timestamp_inicio")),
                ts_parse(evento.get("timestamp_fin"))
            )
        )

    elif topic == "gps":
        cursor.execute(
            "INSERT INTO eventos_gps "
            "(sensor_id, interseccion, nivel_congestion, velocidad_promedio, "
            "estado_detectado, timestamp) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                evento.get("sensor_id"),
                evento.get("interseccion"),
                evento.get("nivel_congestion"),
                evento.get("velocidad_promedio"),
                estado,
                ts_parse(evento.get("timestamp"))
            )
        )

    elif topic in ("prioridad", "semaforo"):
        via = ", ".join(evento.get("via", [evento.get("interseccion", "")]))
        cursor.execute(
            "INSERT INTO cambios_semaforo "
            "(interseccion, color, modo, motivo, timestamp) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                via,
                "VERDE",
                "PRIORIDAD",
                evento.get("motivo", ""),
                ts_parse(evento.get("timestamp"))
            )
        )

# ── Consultas ────────────────────────────────────────────────────────────────
def manejar_query(cursor, peticion):
    tipo = peticion.get("tipo")

    if tipo == "historico":
        interseccion = peticion.get("interseccion", "")
        desde        = peticion.get("desde", "")
        hasta        = peticion.get("hasta", "")
        resultados   = []

        for tabla, col_ts in [("eventos_camara", "timestamp"),
                               ("eventos_espira", "timestamp_inicio"),
                               ("eventos_gps",    "timestamp")]:
            cursor.execute(
                f"SELECT * FROM {tabla} WHERE interseccion = %s "
                f"AND {col_ts} BETWEEN %s AND %s ORDER BY {col_ts}",
                (interseccion, desde, hasta)
            )
            cols = [d[0] for d in cursor.description]
            for row in cursor.fetchall():
                resultados.append(dict(zip(cols, [str(v) for v in row])))

        return {"exito": True, "datos": resultados, "fuente": "replica"}

    elif tipo == "estado_actual":
        interseccion = peticion.get("interseccion")
        resultados   = []
        for tabla, col_ts in [("eventos_camara", "timestamp"),
                               ("eventos_espira", "timestamp_inicio"),
                               ("eventos_gps",    "timestamp")]:
            query = (
                f"SELECT * FROM {tabla} WHERE interseccion = %s "
                f"ORDER BY {col_ts} DESC LIMIT 1"
            )
            cursor.execute(query, (interseccion,))
            cols = [d[0] for d in cursor.description]
            for row in cursor.fetchall():
                resultados.append(dict(zip(cols, [str(v) for v in row])))
        return {"exito": True, "datos": resultados, "fuente": "replica"}

    elif tipo == "semaforos":
        cursor.execute("SELECT * FROM cambios_semaforo ORDER BY timestamp DESC LIMIT 50")
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, [str(v) for v in r])) for r in cursor.fetchall()]
        return {"exito": True, "datos": rows, "fuente": "replica"}

    elif tipo == "ping":
        return {"exito": True, "mensaje": "pong", "fuente": "replica"}

    else:
        return {"exito": False, "error": f"Tipo de consulta desconocido: {tipo}"}

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    inicializar_bd()

    context      = zmq.Context()
    socket_pull  = context.socket(zmq.PULL)
    socket_pull.bind(f"tcp://*:{P['analitica_db_replica']}")

    socket_rep   = context.socket(zmq.REP)
    socket_rep.bind(f"tcp://*:{P['db_replica_query']}")

    poller = zmq.Poller()
    poller.register(socket_pull, zmq.POLLIN)
    poller.register(socket_rep,  zmq.POLLIN)

    print("=" * 55)
    print("  BASE DE DATOS RÉPLICA - PC2")
    print("=" * 55)
    print(f"  PULL eventos:  puerto {P['analitica_db_replica']}")
    print(f"  REP consultas: puerto {P['db_replica_query']}")
    print(f"  MySQL: {DB_CONF['host']}:{DB_CONF['port']} / {DB_CONF['database']}")
    print("=" * 55)

    while True:
        try:
            eventos = dict(poller.poll())

            if socket_pull in eventos:
                evento = socket_pull.recv_json()
                conn   = get_conn()
                cur    = conn.cursor()
                guardar_evento(cur, evento)
                conn.commit()
                cur.close()
                conn.close()
                print(f"[BD REPLICA] Guardado: {evento.get('topic','?')} — "
                      f"{evento.get('interseccion', evento.get('sensor_id','?'))}")

            if socket_rep in eventos:
                peticion = socket_rep.recv_json()
                conn     = get_conn()
                cur      = conn.cursor()
                resp     = manejar_query(cur, peticion)
                cur.close()
                conn.close()
                socket_rep.send_json(resp)
                print(f"[BD REPLICA] Consulta atendida: {peticion.get('tipo')}")

        except mysql.connector.Error as e:
            print(f"[BD REPLICA] Error MySQL: {e}")
        except Exception as e:
            print(f"[BD REPLICA] Error: {e}")

if __name__ == "__main__":
    main()
