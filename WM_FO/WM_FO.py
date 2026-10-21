# WM_FO: Aplicación del operario de campo (Field Operator).
# Uso: python WM_FO.py <KafkaIP> <KafkaPuerto> <ID_Operario>

import threading
import sys
import time
from kafka import KafkaProducer, KafkaConsumer

FORMAT = 'utf-8'
SEP = "#"                           # separador de campos
ESPERA = 4                          # segundos entre servicios del fichero

TOPIC_REQUESTS = "wm_fo_requests"   # FO -> CENTRAL   (REQUEST)
TOPIC_RESPONSES = "wm_fo_responses" # CENTRAL -> FO   (AUTH, DENIED, INFO, DATA, END)
TOPIC_STATIONS = "wm_stations"      # CENTRAL -> FO   (WS: estado de cada estación)

estaciones = {}                     # ID de WS -> (ubicación, estado)
concluido = threading.Event()       # se activa al terminar un servicio (éxito o fracaso)
producer = None                     # productor Kafka (se crea al primer envío)

########## KAFKA: PETICIONES HACIA CENTRAL ##########

def enviar_peticion(ws, segundos):
    # Publica la petición de riego. Devuelve True si el broker la ha recibido
    global producer
    try:
        if producer is None:
            producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR, max_block_ms=5000)
        msg = f"REQUEST{SEP}{OP_ID}{SEP}{ws}{SEP}{segundos}"
        producer.send(TOPIC_REQUESTS, msg.encode(FORMAT)).get(timeout=5)
        return True
    except Exception:
        print("[ERROR] Imposible conectar con WM_Central")
        producer = None
        return False

def pedir_riego(ws, segundos):
    concluido.clear()
    print(f"[PETICIÓN] Solicitando riego en {ws} durante {segundos} s")
    if not enviar_peticion(ws, segundos):
        concluido.set()             # sin conexión: el servicio no puede continuar

########## KAFKA: MENSAJES DE CENTRAL ##########

def hilo_respuestas():
    # Recibe solo los mensajes dirigidos a este operario. Formato: TIPO#ID_OPERARIO#...
    # Al usar un group_id propio, Kafka recuerda lo último leído: si el FO se cierra
    # durante un riego, al volver recibirá lo que se perdió (incluido el resultado final)
    while True:
        try:
            consumer = KafkaConsumer(TOPIC_RESPONSES, bootstrap_servers=KAFKA_ADDR,
                                     group_id=f"fo-{OP_ID}")
            for m in consumer:
                p = m.value.decode(FORMAT).split(SEP)
                if len(p) < 3 or p[1] != OP_ID:
                    continue                                 # mensaje de otro operario
                if p[0] == "INFO":                           # INFO#op#ws#texto
                    print(f"[CENTRAL] {p[3]}")
                elif p[0] == "AUTH":                         # AUTH#op#ws
                    print(f"[AUTORIZADO] Riego autorizado en {p[2]}")
                elif p[0] == "DENIED":                       # DENIED#op#ws#motivo
                    print(f"[DENEGADO] Riego denegado en {p[2]}: {p[3]}")
                    concluido.set()
                elif p[0] == "DATA":                         # DATA#op#ws#caudal#litros
                    print(f"[RIEGO] {p[2]}: {p[3]} L/min | {p[4]} L")
                elif p[0] == "END":                          # END#op#ws#litros#segundos#motivo
                    print(f"[FIN DE RIEGO] {p[2]}: {p[3]} L en {p[4]} s (motivo: {p[5]})")
                    concluido.set()
        except Exception:
            print("[ERROR] Imposible conectar con WM_Central. Reintentando")
            time.sleep(2)

def hilo_estaciones():
    # Mantiene la lista de estaciones. Se lee desde el principio del topic
    # para conocer el último estado de cada una. Formato: WS#ID#ubicación#estado
    while True:
        try:
            consumer = KafkaConsumer(TOPIC_STATIONS, bootstrap_servers=KAFKA_ADDR,
                                     auto_offset_reset="earliest")
            for m in consumer:
                p = m.value.decode(FORMAT).split(SEP)
                if p[0] == "WS" and len(p) >= 4:
                    estaciones[p[1]] = (p[2], p[3])
        except Exception:
            time.sleep(2)

########## FICHERO DE SERVICIOS ##########

def servicios_fichero(ruta):
    # Cada línea del fichero: ID_WS#segundos. Se envía una, se espera a que
    # concluya (éxito o fracaso), se esperan 4 s y se pasa a la siguiente
    try:
        lineas = open(ruta).read().splitlines()
    except OSError:
        print(f"[ERROR] No se puede abrir el fichero {ruta}")
        return
    for linea in lineas:
        if not linea.strip():
            continue
        try:
            ws, segundos = linea.strip().split(SEP)
            segundos = int(segundos)
        except ValueError:
            print(f"[ERROR] Línea no válida: {linea}")
            continue
        pedir_riego(ws, segundos)
        concluido.wait(timeout=segundos + 30)       # evita bloqueo si no hay respuesta
        time.sleep(ESPERA)
    print("[FICHERO] Todos los servicios han sido procesados")

########## MENÚ ##########

def ver_estaciones():
    if not estaciones:
        print("No hay información de estaciones disponible")
    for ws, (ubicacion, estado) in sorted(estaciones.items()):
        print(f"  {ws} | {ubicacion} | {estado}")

def menu():
    while True:
        print("\n1) Ver estaciones  2) Solicitar riego  3) Cargar fichero de servicios  0) Salir")
        op = input("> ").strip()
        if op == "1":
            ver_estaciones()
        elif op == "2":
            ws = input("ID de la estación: ").strip()
            t = input("Duración (s): ").strip()
            if t.isdigit():
                pedir_riego(ws, int(t))
        elif op == "3":
            ruta = input("Ruta del fichero: ").strip()
            threading.Thread(target=servicios_fichero, args=(ruta,), daemon=True).start()
        elif op == "0":
            break

######################### MAIN ##########################

print("****** WM_FO: APLICACIÓN DEL OPERARIO DE CAMPO ******")

if len(sys.argv) == 4:
    KAFKA_ADDR = f"{sys.argv[1]}:{sys.argv[2]}"
    OP_ID = sys.argv[3]
    print(f"Operario: {OP_ID}")

    threading.Thread(target=hilo_respuestas, daemon=True).start()
    threading.Thread(target=hilo_estaciones, daemon=True).start()
    try:
        menu()
    except (KeyboardInterrupt, EOFError):
        pass
    print("[FIN] Aplicación del operario cerrada")
else:
    print("Argumentos necesarios: <KafkaIP> <KafkaPuerto> <ID_Operario>")