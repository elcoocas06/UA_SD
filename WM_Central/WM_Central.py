import socket 
import threading
import sys
import time
import sqlite3                  # base de datos SQLite
import os                       # variables de entorno (Railway)
from collections import deque   # lista con tamaño máximo (para los mensajes del panel)

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
SEP = "#"

# Estados que se muestran en el panel
DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA DE SERVICIO"
DESCONECTADA = "DESCONECTADA"


DB_FILE = os.environ.get("WM_DB", "wm_central.db")     # en Railway: ruta dentro de un volumen
OPERARIOS_INICIALES = [("FO-01", "Operario 1"), ("FO-02", "Operario 2"), ("FO-03", "Operario 3"),
                       ("FO-04", "Operario 4"), ("FO-05", "Operario 5")]


########## VARIABLES COMPARTIDAS ##########

estaciones = {}                 # id de WS -> diccionario con su información (ver nueva_estacion)
lock_est = threading.Lock()     # Protege el acceso al estado de las estaciones
lock_db = threading.Lock()      # Protege el acceso a SQLite
mensajes = deque(maxlen=60)     # últimos mensajes (se muestran en el panel)


########## LOG ##########

def log(texto):
    linea = f"{time.strftime('%H:%M:%S')} {texto}"
    print(linea, flush=True)
    mensajes.append(linea)


########## BASE DE DATOS (SQLite) ##########

# Ejecuta operaciones SQL
def db_ejecutar(sql, params=(), leer=False):
    # Abre la BD, ejecuta una sentencia SQL y la cierra. El cerrojo evita accesos simultáneos.
    with lock_db:
        con = sqlite3.connect(DB_FILE)
        try:
            cursor = con.execute(sql, params)
            filas = cursor.fetchall() if leer else None
            con.commit()
            return filas
        finally:
            con.close()


# Crea las tablas e inicializa los operarios
def iniciar_db():
    db_ejecutar("CREATE TABLE IF NOT EXISTS operarios (id TEXT PRIMARY KEY, nombre TEXT)")
    db_ejecutar("CREATE TABLE IF NOT EXISTS estaciones "
                "(id TEXT PRIMARY KEY, ubicacion TEXT, estado TEXT)")
    db_ejecutar("CREATE TABLE IF NOT EXISTS riegos "
                "(id INTEGER PRIMARY KEY AUTOINCREMENT, ws TEXT, operario TEXT, "
                "fecha TEXT, duracion REAL)")
    for id_op, nombre in OPERARIOS_INICIALES:
        db_ejecutar("INSERT OR IGNORE INTO operarios VALUES (?, ?)", (id_op, nombre))


# Comprobar si un operario está registrado
def operario_existe(id_op):
    return len(db_ejecutar("SELECT id FROM operarios WHERE id = ?", (id_op,), leer=True)) > 0


########## ESTACIONES (estado en memoria) ##########

def nueva_estacion(ws_id, ubicacion):
    return {"id": ws_id, "ubicacion": ubicacion,
            "conectado": False,      # ¿hay un Monitor conectado por socket?
            "conn": None,            # socket de ese Monitor
            "monitor": "WAIT",       # último estado enviado por el Monitor: WAIT / OK / KO
            "engine": None,          # último estado enviado por el Engine (por Kafka)
            "riego_op": None,        # operario del riego en curso (None = no hay riego)
            "riego_inicio": None,
            "pendiente": None}       # petición esperando autorización: (operario, hora)


def cargar_estaciones():
    # Al arrancar: cargamos de la BD las estaciones ya dadas de alta. Todas DESCONECTADAS
    # hasta que su Monitor conecte (no conocemos su estado real).
    for ws_id, ubicacion in db_ejecutar("SELECT id, ubicacion FROM estaciones", leer=True):
        estaciones[ws_id] = nueva_estacion(ws_id, ubicacion)
        log(f"[BD] Estación {ws_id} ({ubicacion}) cargada: DESCONECTADA hasta que conecte")


def estado_visible(ws):
    # Calcula el estado que se muestra en el panel. Llamar SIEMPRE con lock_est cogido.
    if not ws["conectado"]:
        return DESCONECTADA
    if ws["monitor"] == "KO" or ws["engine"] == "LEAK":
        return FUGA
    if ws["engine"] is None:
        return DESCONECTADA            # el Engine aún no ha dicho nada
    if ws["engine"] == "OUT_OF_SERVICE":
        return FUERA_SERVICIO
    if ws["engine"] == "WATERING":
        return REGANDO
    if ws["engine"] == "READY":
        return DISPONIBLE
    
    return DESCONECTADA


######################### MAIN ##########################

print("****** WELCOME TO WM_CENTRAL: SISTEMA CENTRAL DE WATERMANAGEMENT ****")

# Parametros: 
# - Puerto para monitores
# - Dirección del broker de Kafka (ip y puerto)
if len(sys.argv) == 4:
    PUERTO = int(sys.argv[1])
    KAFKA_ADDR = f"{sys.argv[2]}:{sys.argv[3]}"

    # Inicialización de la base de datos
    iniciar_db()
    cargar_estaciones()

# Parametros mal introducidos
else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: <PuertoSockets> <KafkaIP> <KafkaPuerto>")