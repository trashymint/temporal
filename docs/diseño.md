# Diseño inicial - sistema de trafico distribuido

## Objetivo
Construir un sistema distribuido que simule la gestion inteligente del trafico mediante sensores y semaforos inteligentes

## Arquitectura general
El sistema se divide en 3 nodos
PC1
Sensores de trafico
Broker de comunicaion ZeroMQ

PC2
Servicio de analitica
Servicio de control de semaforos
Base de datos replica

PC3
Servicio de monitoreo
Base de datos principal

## Flujo de informacion
1. Los sensores generan eventos de trafico
2. Los eventos se envian al broker mediante ZeroMQ
3. El broker distribuye los eventos al servicio analitico
4. EL servicio de analitica detecta condiciones de trafico
5. Se envian comandos al servicio de semaforos
6. Los eventos se almacenan en la base de datos
7. El usuario puede consultar el sistema desde el servicio de monitoreo

------------------------------------------------------------------
# Definición del sistema
## Tamaño de la ciudad
N = 4 filas 
M = 4 columnas
Ciudad tendra 16 intersecciones
Ejemplos de intersecciones
INT-A1
INT-A2
INT-A3
INT-A4

INT-B1
INT-B2
INT-B3
INT-B4

INT-C1
INT-C2
INT-C3
INT-C4

INT-D1
INT-D2
INT-D3
INT-D4

## Distribución de sensores
Cada interseccion tendra 3 sensores
1. Espira inductiva
2. Cámara
3. GPS

Por lo tanto habran un total de 48 sensores

Ejemplo de una interseccion
ESP-C3 ------> Espira inductiva
CAM-C3 ------> Cámara
GPS-C3 ------> GPS

## Frecuencia de eventos
Sensor				Frecuecia
------------------------/-------------------------------
Espira inductiva	/	Cada 30 segundos Cámara
------------------------/-------------------------------
Cámara			/	Cada 10 segundos
------------------------/-------------------------------
GPS 			/	Cada 15 segundos

## Estados del sistemas
Q = Longitud de cola		
Vp = Velocidad promedio
D = Densidad

El sistema tendra 3 estados principales
1. Tráfico normal
Condición:
Q < 5		
Vp > 35		
D < 20

Acción:
El semaforo va a estar cambiando cada 15 segundos

2. Congestión
Condición:
Q >= 5
Vp < 25
D >= 20

Acción:
Extender luz verde a 30 segundos

3. Priorización de via (Si se presenta algún vehiculo de emergencia) 
Condición:
Evento especial enviado por usuario

Acción:
Activar "ola verde" -----> todos los semaforos de la vía se ponene en verde

## Formato de eventos
### Evento cámara:
Mide
- Longitud de cola (Q)
- Velocidad promedio del auto en un punto en especifico

ejemplo JSON
{
	"sensor_id": "CAM-C5",
	"tipo_sensor": "camara",
	"interseccion": "INT-C5",
	"volumen": 10,
	"velocidad_promedio": 25,
	"timestamp": "2026-02-09T15:00Z"
}

### Evento espira:
Mide
- Conteo de vehiculos en un intervalo de tiempo

ejemplo JSON
{
	"sensor_id": "ESP-C5",
	"tipo_sensor": "espira",
	"interseccion": "INT-C5",
	"vehiculos_contados": 12,
	"intervalo_segundos": 30,
	"timestamp_inicio": "2026-02-09T15:20:00Z"
	"timestamp_fin": "2026-02-09T15:20:30Z"
}

### Evento GPS:
Mide
- Densidad del trafico (D)
- Velocidad promedio del auto (Vp)

ejemplo JSON
{
	"sensor_id": "GPS-C5",
	"tipo_sensor": "gps",
	"interseccion": "INT-C5",
	"nivel_congestion": "NORMAL",
	"velocidad_promedio": 18,
	"timestamp": "2026-02-09T15:20:10Z"
}

## Consultas del usuario
- Consulta de estado actual
Ejemplo
consultar estado INT-C3
Respuesta:
estado: congestion
semaforo: rojo
cola: 7 vehiculos

- Consulta histórica
Ejemplo
consultar trafico INT-C3
desde 08:00
hasta 10:00

- Priorizar vía
Ejemplo
priorizar INT-A1 -> INT-A4

## Parametros configurables
El sistema permitirá modificar algunos parámetros que determinan el comportamiento de la simulación del tráfico y de los sensores. Estos parámetros permiten ajustar el sistema para diferentes escenarios de prueba
- Tamaño de la ciudad: número de filas y columnas de la cuadrícula que representa la ciudad
- Frecuencia de generación de eventos de los sensores: Intervalod de tiempo entre eventos generados por los sensores de cámara, espira inductiva y GPS
- Tiempo de cambio del semáforo en condiciones normales: duración del estado verde o rojo cuando el tráfico es normal
- Tiempo de extensión del semáforo en condiciones de congestión: tiempo adicional que se mantiene la luz verde cuando se detecta congestion
- Número de sensores por intersección: Cantidad de sensores simulados que operan en cada interseccion


