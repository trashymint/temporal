import json

def generar_intersecciones():
	with open("config/config.json") as f:
		config = json.load(f)

	filas = config["filas"]
	columnas = config["columnas"]

	intersecciones = []

	for f in filas:
		for c in columnas:
			intersecciones.append(f"INT_{f}{c}")

	return intersecciones
