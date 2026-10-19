# WM_WS_E: Engine de la Watering Station.
# Uso: python WM_WS_E.py <KafkaIP> <KafkaPuerto> <MonitorIP> <MonitorPuerto>

import socket
import threading
import sys
import time
import os
from kafka import KafkaProducer, KafkaConsumer

HEADER = 64                      # bytes de la cabecera con la longitud del mensaje
FORMAT = 'utf-8'
SEP = "#"                        # separador de campos
CAUDAL = 12.0                    # caudal simulado (L/min)

TOPIC_ORDERS = "wm_ws_orders"    # CENTRAL -> WS (START, STOP, BLOCK, ACTIVATE)
TOPIC_EVENTS = "wm_ws_events"    # WS -> CENTRAL (REQUEST, DATA, END)

# Estado de la estación (compartido entre hilos)
WS_ID = None                     # lo comunica el Monitor tras autenticarse en CENTRAL
id_listo = threading.Event()     # se activa cuando ya se conoce el ID
averia = False                   # True si se simula una fuga (responde KO)
bloqueada = False                # True si CENTRAL la ha puesto fuera de servicio
regando = False
operario = ""
limite = 0                       # duración máxima del riego (s)
segundos = 0                     # tiempo transcurrido del riego
litros = 0.0                     # volumen acumulado del riego
producer = None                  # productor Kafka (se crea al primer envío)

########## MENSAJERÍA CON SOCKETS (idéntica a la del Monitor) ##########

def send(sock, msg):
    # Envía primero la longitud (rellena a HEADER bytes) y después el mensaje
    message = msg.encode(FORMAT)
    send_length = str(len(message)).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    sock.sendall(send_length)
    sock.sendall(message)

def recibir_exacto(sock, n):
    # recv puede devolver menos bytes de los pedidos: se lee hasta completar n
    datos = b''
    while len(datos) < n:
        trozo = sock.recv(n - len(datos))
        if not trozo:
            raise ConnectionError("conexión cerrada por el otro extremo")
        datos += trozo
    return datos

def recibir(sock):
    msg_length = int(recibir_exacto(sock, HEADER).decode(FORMAT))
    return recibir_exacto(sock, msg_length).decode(FORMAT)

########## KAFKA: EVENTOS HACIA CENTRAL ##########

def enviar_evento(msg):
    # Publica un evento en Kafka. Devuelve True si el broker lo ha recibido
    global producer
    try:
        if producer is None:
            producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR, max_block_ms=5000)
        producer.send(TOPIC_EVENTS, msg.encode(FORMAT)).get(timeout=5)
        return True
    except Exception:
        print("[ERROR] Imposible conectar con WM_Central")
        producer = None
        return False

########## RIEGO ##########

def fichero():
    # Fichero donde se guarda el riego en curso para no perder lo ya regado
    return f"riego_{WS_ID}.txt"

def iniciar_riego(op, tiempo):
    global regando, operario, limite, segundos, litros
    if averia or bloqueada or regando:
        print("[RIEGO] No es posible iniciar el riego en el estado actual")
        return
    regando, operario, limite, segundos, litros = True, op, tiempo, 0, 0.0
    print(f"[RIEGO] Electroválvula abierta. Operario: {op}. Duración: {tiempo} s")

def finalizar_riego(motivo):
    global regando
    if not regando:
        return
    regando = False
    print(f"[RIEGO] Electroválvula cerrada ({motivo}). Total: {litros:.2f} L en {segundos} s")
    # Resumen final para CENTRAL; si se entrega, ya no queda nada pendiente en disco
    if enviar_evento(f"END{SEP}{WS_ID}{SEP}{operario}{SEP}{litros:.2f}{SEP}{segundos}{SEP}{motivo}"):
        if os.path.exists(fichero()):
            os.remove(fichero())

def hilo_riego():
    # Cada segundo, mientras se riega: calcula los litros y envía los datos a CENTRAL
    global segundos, litros
    while True:
        time.sleep(1)
        if regando:
            litros += CAUDAL / 60
            segundos += 1
            with open(fichero(), "w") as f:      # copia en disco por si el Engine se detiene
                f.write(f"{operario}{SEP}{litros:.2f}{SEP}{segundos}")
            print(f"[CAUDALÍMETRO] {CAUDAL} L/min | {litros:.2f} L | {segundos} s")
            enviar_evento(f"DATA{SEP}{WS_ID}{SEP}{operario}{SEP}{CAUDAL}{SEP}{litros:.2f}")
            if segundos >= limite:
                finalizar_riego("TIEMPO")

def recuperar_riego():
    # Si el Engine se detuvo durante un riego, informa a CENTRAL del resultado final
    if os.path.exists(fichero()):
        op, l, s = open(fichero()).read().split(SEP)
        print(f"[RECUPERACIÓN] Riego interrumpido: {l} L en {s} s. Se informa a CENTRAL")
        if enviar_evento(f"END{SEP}{WS_ID}{SEP}{op}{SEP}{l}{SEP}{s}{SEP}RECUPERADO"):
            os.remove(fichero())

########## ÓRDENES DE CENTRAL (KAFKA) ##########

def hilo_ordenes():
    # Escucha las órdenes de CENTRAL. Formato: ORDEN#ID_WS#...  (ID_WS puede ser ALL)
    global bloqueada
    id_listo.wait()
    while True:
        try:
            consumer = KafkaConsumer(TOPIC_ORDERS, bootstrap_servers=KAFKA_ADDR)
            for m in consumer:
                p = m.value.decode(FORMAT).split(SEP)
                if len(p) < 2 or p[1] not in (WS_ID, "ALL"):
                    continue                         # la orden es para otra estación
                print(f"[CENTRAL] Orden recibida: {p}")
                if p[0] == "START":                  # START#ID#operario#segundos
                    iniciar_riego(p[2], int(p[3]))
                elif p[0] == "STOP":
                    finalizar_riego("STOP")
                elif p[0] == "BLOCK":                # cierra el riego y deja la WS fuera de servicio
                    bloqueada = True
                    finalizar_riego("BLOQUEADA")
                    print("[ESTADO] Fuera de servicio")
                elif p[0] == "ACTIVATE":
                    bloqueada = False
                    print("[ESTADO] Activada")
        except Exception as e:
            print(f"[ERROR] Sin conexión con el broker Kafka ({e}). Reintentando")
            time.sleep(2)

########## CONEXIÓN CON EL MONITOR ##########

def hilo_monitor():
    # Se conecta al Monitor y responde a sus comprobaciones de salud
    global WS_ID
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(MONITOR_ADDR)
            print(f"[MONITOR] Conectado con el Monitor en {MONITOR_ADDR}")
            while True:
                msg = recibir(sock).split(SEP)       # HEALTH#ID
                if msg[0] == "HEALTH":
                    if WS_ID is None:                # primer mensaje: se conoce el ID
                        WS_ID = msg[1]
                        print(f"[MONITOR] Identificador de la estación: {WS_ID}")
                        recuperar_riego()
                        id_listo.set()
                    send(sock, f"HEALTH_ANS{SEP}{'KO' if averia else 'OK'}")
        except (ConnectionError, OSError, ValueError) as e:
            print(f"[ERROR] Conexión con el Monitor perdida ({e}). Reintentando")
            sock.close()
            time.sleep(2)

########## MENÚ DE LA ESTACIÓN ##########

def menu():
    global averia
    while True:
        print("\n1) Solicitar riego  2) Finalizar riego  3) Simular/resolver avería")
        op = input("> ").strip()
        if WS_ID is None:
            print("Esperando a que el Monitor autentique la estación")
        elif op == "1":                              # el riego manual lo autoriza CENTRAL
            t = input("Duración (s): ")
            if t.isdigit() and enviar_evento(f"REQUEST{SEP}{WS_ID}{SEP}MANUAL{SEP}{t}"):
                print("Petición enviada a CENTRAL. Esperando autorización")
        elif op == "2":
            finalizar_riego("MANUAL")
        elif op == "3":                              # alterna entre fuga y funcionamiento normal
            averia = not averia
            if averia:
                print("[AVERÍA] Fuga simulada. Se responderá KO al Monitor")
                finalizar_riego("FUGA")              # una fuga detiene el riego en curso
            else:
                print("[AVERÍA RESUELTA] Se responderá OK al Monitor")

######################### MAIN ##########################

print("****** WM_WS_E: ENGINE DE LA WATERING STATION ******")

if len(sys.argv) == 5:
    KAFKA_ADDR = f"{sys.argv[1]}:{sys.argv[2]}"
    MONITOR_ADDR = (sys.argv[3], int(sys.argv[4]))

    # Tres hilos en segundo plano; el hilo principal atiende el menú
    threading.Thread(target=hilo_monitor, daemon=True).start()
    threading.Thread(target=hilo_ordenes, daemon=True).start()
    threading.Thread(target=hilo_riego, daemon=True).start()
    try:
        menu()
    except (KeyboardInterrupt, EOFError):
        print("\n[FIN] Engine detenido")
else:
    print("Argumentos necesarios: <KafkaIP> <KafkaPuerto> <MonitorIP> <MonitorPuerto>")