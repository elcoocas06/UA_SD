#=================================================================================================================================
# WM_WS_E: ENGINE DE LA WATERING STATION
# Simula el equipo de riego de la estación: caudalímetro, electroválvula y detector de fugas (que se provoca a mano desde el menú)
# 
# Uso: python WM:WS_E.py <KafkaIP> <KafkaPuerto> <MonitorIP> <MonitorPuerto>
# 
# El programa tiene 4 hilos que trabajan a la vez:
# 1) Hilo principal: atiende el menú de la estación (lee el teclado)
# 2) Hilo de riego: cada segundo simula el caudal, envía DATA y entrega a WM_Central  los datos resumenes finales pendientes.
# 3) Hilo de órdenes: consume de Kafka las órdenes de CENTRAL
# 4) Hilo del monitor: se conecta al Monitor con sockets y responde a sus comprobaciones de salud
#
# Mensajes:
#   Socket Monitor -> Engine :  HEALTH#ID
#   Socket Engine -> Monitor :  HEALTH_ANS#OK/KO
#   Kafka Engine -> Central :   REQUEST#ID#MANUAL#segundos
#                               DATA#ID#operario#caudal#litros
#                               END#ID#operario#litros#segundos#motivo
#                               REJECT#ID#operario#motivo
#   Kafka Central -> Engine :   START#ID#operario#segundos | STOP#ID | BLOCK#ID | ACTIVATE#ID 
#=================================================================================================================================

import socket
import threading
import sys
import time
import os
from kafka import KafkaConsumer, KafkaProducer

# En docker la salida no es una terminal y python acumula los print en un bufer.
# Con esto cada linea se muestra en el momento en que se imprime.
sys.stdout.reconfigure(line_buffering=True)

HEADER = 64                      # bytes de la cabecera con la longitud del mensaje
FORMAT = 'utf-8'
SEP = "#"                        # separador de campos
CAUDAL = 12.0                    # caudal simulado (L/min)
TIMEOUT_MONITOR = 5              # tiempo máximo de espera de respuesta del monitor (s)
DIR_DATOS = os.environ.get("WM_DATOS", ".") # directorio donde se guardan los ficheros de riego (por defecto el actual)
# se rellenan en el main con los argumentos
KAFKA_ADDR = None
MONITOR_ADDR = None

TOPIC_ORDERS = "wm_ws_orders"    # CENTRAL -> WS (START, STOP, BLOCK, ACTIVATE)
TOPIC_EVENTS = "wm_ws_events"   # WS -> CENTRAL (REQUEST, DATA, END, REJECT)

############ Estado de la estacion (compartido entre hilos) ############

WS_ID = None                     # lo comunica el Monitor tras autenticarse en CENTRAL
id_listo = threading.Event()     # se activa cuando ya se conoce el ID

# Cerrojo que protege el acceso al estado del riego. Varios hilos pueden intentar abrir/cerrar la electroválvula a la vez (menú, orden de central, limite de tiempo, caida del monitor). Con el cerrojo se asegura que solo uno de ellos lo haga a la vez.
lock = threading.Lock() 

averia = False                  #True si se simula una fuga (responde KO)
bloqueada = False               #True si CENTRAL la ha puesto fuera de servicio
monitor_ok = False              #True si el monitor responde a las comprobaciones de salud
central_conectada = None        #True si hay conexión con la central
regando = False                 #True si la electroválvula está abierta
operario = ""                   # nombre del operario que ha iniciado el riego
limite = 0                      # duración máxima del riego (s)
inicio = 0                      # instante de tiempo en que se abrió la electroválvula
segundos = 0                    # tiempo transcurrido del riego
litros = 0.0                    # volumen acumulado del riego

producer = None                 # productor Kafka (se crea al primer envío) 
kafka_ok = True                 # para mostrar el error de conexión con el broker solo una vez (no cada vez que se envía un evento)

########### Mensajeria con sockets: <longitud en 64 bytes> + <mensaje> ##########

def send(sock, msg):
    # Envía primero la longitud (rellena a HEADER bytes) y después el mensaje. Así el receptor sabe exactamente cuántos bytes tiene que leer.
    message = msg.encode(FORMAT)
    send_lenght = str(len(message)).encode(FORMAT) 
    send_lenght += b' ' * (HEADER - len(send_lenght)) # rellena con espacios hasta HEADER bytes
    sock.sendall(send_lenght) # envía la longitud del mensaje
    sock.sendall(message)     # envía el mensaje

def recibir_exacto(sock, n):
    # recv() puede devolver menos bytes de los pedidos: se repite hasta completar n bytes. Si el otro extremo cierra la conexión, devuelve un error.
    datos = b''
    while len(datos) < n:
        trozo = sock.recv(n-len(datos)) # lee hasta completar n bytes
        if not trozo: 
            raise ConnectionError("conexión cerrada por el otro extremo")
        datos += trozo
    return datos

def recibir(sock): 
    # Lee primero la longitud del mensaje (HEADER bytes) y después el mensaje completo. Devuelve el mensaje decodificado.
    msg_length = int(recibir_exacto(sock,HEADER).decode(FORMAT)) 
    msg = recibir_exacto(sock,msg_length).decode(FORMAT) 
    return msg

########### KAFKA: eventos hacia central ##########

def enviar_evento(msg):
    # Publica un evento y espera la confirmación del broker. Devuelve True solo si el mensaje ha llegado, así quien llama sabe si puede dar la información por entregada. Si no hay conexión con el broker, devuelve False y muestra un mensaje de error (solo la primera vez).
    global producer, kafka_ok
    try:
        if producer is None:
            producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR, max_block_ms=5000) # crea el productor la primera vez que se envía un evento
        producer.send(TOPIC_EVENTS, msg.encode(FORMAT)).get(timeout=5) # espera la confirmación del broker
        if not kafka_ok:
            print("[INFO] Conexión con WM_Central restablecida")
            kafka_ok = True
        return True
    except Exception:
        if kafka_ok: # solo muestra el error la primera vez que falla la conexión
            print("[ERROR] Imposible conectar con WM_Central")
            kafka_ok = False
        if producer is not None: # si el productor ya estaba creado, lo destruye para que se vuelva a crear la próxima vez que se envíe un evento
            try:
                producer.close(timeout=0)
            except Exception:
                pass
        producer = None
        return False

############ FICHERO DE RIEGO ##########
# Formato: operario#litros#segundos#motivo
#   - Mientras se riega, motivo = "EN_CURSO" y se actualiza cada segundo. Así si se cae la estación, al reiniciarse puede recuperar el riego en curso y no perder lo ya regado.
#   - Al cerrar la electroválvula se guarda el motivo real de cierre(Tiempo, Fuga, Orden de Central,etc).
#   - El fichero solo se borra cuando central ha recibido el evento END.
# Mientras exista el fichero, no se admite un riego nuevo, de modo que ningún resumen se sobreescribe ni se pierde.

def fichero():
    # Fichero donde se guarda el riego en curso para no perder lo ya regado
    return os.path.join(DIR_DATOS, f"riego_{WS_ID}.txt")

def guardar_fichero(motivo):
    # Guarda el riego en curso en un temporal y luego se renombra. Si el Engine muere a mitad de la escritura nunca queda un fichero a medias.
    tmp = fichero() + ".tmp" 
    with open(tmp, "w") as f: 
        f.write(f"{operario}{SEP}{litros:.2f}{SEP}{segundos}{SEP}{motivo}\n")
    os.replace(tmp, fichero()) # renombra el temporal al nombre definitivo (si ya existía, lo sobreescribe)

########### RIEGO ##########

def actualizar_contadores():
    # Actualiza los contadores de tiempo y litros del riego en curso. Se llama cada segundo mientras se riega.
    global segundos, litros
    t = min(time.time() - inicio, limite) # segundos transcurridos desde que se abrió la electroválvula (no puede superar el límite) 
    segundos = int(t)
    litros = CAUDAL * t / 60.0 

def iniciar_riego(op, tiempo):
    # Abre la electroválvula y empieza a regar. 
    # Si no puede, informa del motivo a CENTRAL (evento REJECT).
    global regando, operario, limite, inicio, segundos, litros
    with lock: # asegura que solo un hilo puede abrir/cerrar la electroválvula a la vez
        if averia:
            motivo = "AVERIA"
        elif bloqueada:
            motivo = "BLOQUEADA"
        elif not monitor_ok:
            motivo = "SIN_MONITOR"
        elif regando:
            motivo = "OCUPADA"
        elif os.path.exists(fichero()):
            motivo = "RIEGO_PENDIENTE"  # aun no se ha entregado a CENTRAL el resumen del riego anterior
        else:
            motivo = None
            regando, operario, limite = True, op, tiempo
            inicio, segundos, litros = time.time(), 0, 0.0
            guardar_fichero("EN_CURSO")     # guarda el riego en curso para poder recuperarlo si el Engine se cae
            print(f"[RIEGO] Electroválvula abierta. Operario: {op}. Duración: {tiempo} s")
    # El envio a Kafka se hace fuera del cerrojo, para no bloquear el hilo de riego mientras espera la confirmación del broker 
    if motivo:
        print(f"[RIEGO] Riego rechazado para {op} (motivo: {motivo})")
    enviar_evento(f"REJECT{SEP}{WS_ID}{SEP}{op}{SEP}{motivo}")

def cerrar_valvula(motivo):
    # Cierra el riego y guarda el resumen final en el fichero. No envia el evento END a CENTRAL, eso lo hace el hilo de riego)
    global regando 
    if not regando: 
        return False
    actualizar_contadores()
    regando = False
    guardar_fichero(motivo)
    print(f"[RIEGO] Electroválvula cerrada ({motivo}). Total: {litros:.2f} L en {segundos} s")
    return True

def finalizar_riego(motivo):
    # Coge el cerrojo y cierra la electroválvula.
    with lock: 
        return cerrar_valvula(motivo)

def entregar_end_pendiente():
    # Si hay un resumen final sin entregar, lo envia a central y si llega, borra el fichero
    with lock:
        if WS_ID is None or regando or not os.path.exists(fichero()):
            return # no hay resumen pendiente
        try:
            op, l, s, motivo = open(fichero()).read().strip().split(SEP) 
        except ValueError:
            os.replace(fichero(), fichero() + ".error") # renombra el fichero para que no se pierda y no se intente enviar otra vez
            print(f"[ERROR] Formato incorrecto del fichero de riego: {fichero()}")
            return
    if motivo == "EN_CURSO": # el Engine se detuvo en mitad del riego
        motivo = "RECUPERADO" 
        print(f"[RECUPERACIÓN] Riego interrumpido: {l} L en {s} s. Se informa a CENTRAL")
    if enviar_evento(f"END{SEP}{WS_ID}{SEP}{op}{SEP}{l}{SEP}{s}{SEP}{motivo}"):
        with lock:
            os.remove(fichero())
        print(f"[CENTRAL] Resumen final entregado: {l} L en {s} s ({motivo})")

def hilo_riego():
    # Cada segundo:
    # 1. Si se esta regando, actualiza los contadores, guarda el riego en curso, envia DATA y comprueba si se ha llegado al limite de tiempo.
    # 2. Intenta entregar el END pendiente, si lo hay(se reintenta cada segundo hasta que central/kafka lo recibe)
    while True:
        time.sleep(1)
        data = None
        with lock:
            if regando:
                actualizar_contadores()
                guardar_fichero("EN_CURSO")
                print(f"[CAUDALÍMETRO] {CAUDAL} L/min | {litros:.2f} L | {segundos} s | {operario}")
                data = f"DATA{SEP}{WS_ID}{SEP}{operario}{SEP}{CAUDAL}{SEP}{litros:.2f}"
                if segundos >= limite:
                    cerrar_valvula("TIEMPO")
        if data:
            enviar_evento(data)
        entregar_end_pendiente()

############ ORDENES DE CENTRAL (KAFKA) ############

def procesar_orden(texto):
    global bloqueada
    p = texto.split(SEP)
    if len(p) < 2 or p[1] not in (WS_ID, "ALL"):
        return  # la orden es para otra estacion
    print(f"[CENTRAL] Orden recibida: {texto}")
    try:
        if p[0] == "START": # START#ID#operario#segundos
            iniciar_riego(p[2], int(p[3]))
        elif p[0] == "STOP":
            finalizar_riego("STOP")
        elif p[0] == "BLOCK":   # corta el riego y queda fuera de servicio
            with lock:
                bloqueada = True
                cerrar_valvula("BLOQUEADA")
            print("[ESTADO] FUERA DE SERVICIO por orden de WM_Central")
        elif p[0] == "ACTIVATE":
            with lock:
                bloqueada = False
            print("[ESTADO] ACTIVADA por WM_Central")
        else:
            print(f"[AVISO] Orden desconocida: {texto}")
    except(IndexError, ValueError):     # faltan campos o la duracion no es un numero
        print(f"[ERROR] Orden mal formada: {texto}")

def hilo_ordenes():
    # Sin group_id, cada Engine es un consumidor independiente y recibe todas las ordenes, despues filtra las suyas (su ID o ALL).
    #auto_offset_reset="latest": solo se leen ordenes nuevas, asi una WS qeu arranca no ejecuta ordenes antiguas enviadas mientras estaba apagada.
    id_listo.wait()
    while True:
        try:
            consumer = KafkaConsumer(TOPIC_ORDERS, bootstrap_servers=KAFKA_ADDR, auto_offset_reset="latest")
            print("[KAFKA] Escuchando ordenes de WM_Central")
            for m in consumer:
                procesar_orden(m.value.decode(FORMAT))
        except Exception as e:
            print(f"[ERROR] Sin conexion con el broker Kafka ({e}). Reintentando")
            time.sleep(2)

########## CONEXION CON EL MONITOR (SOCKET) ##########

def hilo_monitor():
    # El Engine es CLIENTE y el Monitor es SERVIDOR. Cada segundo llega un HEALTH y se responde OK o KO(si hay fuga simulada).
    # Si el monitor cae (conexion cerrada o 5 s sin HEALTH), la estacion queda AVERIADA: se corta el riego en curso y no se admiten riegos nuevos hasta que el monitor vuelva.
    global WS_ID, monitor_ok, central_conectada
    avisado = False     # para no reoetir "Esperando al Monitor"
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TIMEOUT_MONITOR)  # evita quedarse bloqueado en recv()
        try:
            sock.connect(MONITOR_ADDR)
            print(f"[MONITOR] Conectado con el Monitor en {MONITOR_ADDR[0]}:{MONITOR_ADDR[1]}")
            avisado = False
            while True:
                msg = recibir(sock).split(SEP)  # HEALTH#ID[#OK|KO]
                if msg[0] != "HEALTH":
                    continue
                if WS_ID is None:   # primer HEALTH: se conoce el ID
                    WS_ID = msg[1]
                    os.makedirs(DIR_DATOS, exist_ok=True)
                    print(f"[MONITOR] Identificador de la estacion: {WS_ID}")
                    id_listo.set()
                if len(msg) > 2:    # el Monitor informa del estado de central
                    nuevo = (msg[2] == "OK")
                    if nuevo != central_conectada:
                        print("[CENTRAL] WM_Central disponible" if nuevo
                              else "[ERROR] No se pudo conectar con WM_Central")
                    central_conectada =  nuevo
                if not monitor_ok: 
                    monitor_ok = True
                    print("[ESTADO] Estacion OPERATIVA (Monitor conectado)")
                send(sock, f"HEALTH_ANS{SEP}{'KO' if averia else 'OK'}")
        except (ConnectionError, OSError, ValueError) as e:
            with lock:
                se_ha_caido = monitor_ok
                monitor_ok = False
                # observaciones: segun la guia, si cae el Monitor el riego en curso finaliza.
                # si continuara hasta acabar, hay que quitar esta linea por seguir_regando
                cerrar_valvula("MONITOR_CAIDO")
            if se_ha_caido:
                print(f"[ESTADO] AVERIADA: conexion con el Monitor perdida ({e}). No se admiten riegos") 
            elif not avisado:
                print(f"[MONITOR] Esperando al Monitor en {MONITOR_ADDR[0]}:{MONITOR_ADDR[1]}")
                avisado = True
            sock.close()
            time.sleep(2)

############# MENÚ DE LA ESTACION #############

def mostrar_estado():
    with lock:
        if WS_ID is None:
            estado = "SIN AUTENTICAR (esperando al Monitor)"
        elif not monitor_ok:
            estado = "AVERIADA (sin Monitor)"
        elif averia:
            estado = "FUGA"
        elif bloqueada:
            estado = "FUERA DE SERVICIO"
        elif regando:
            estado = f"REGANDO | {CAUDAL} L/min | {litros:.2f} L | operario {operario}"
        else:
            estado = "DISPONIBLE"
        pendiente = WS_ID is not None and not regando and os.path.exists(fichero())
    print(f"[ESTADO] WS {WS_ID}: {estado}")
    if pendiente:
        print("[ESTADO] Hay un resumen de riego pendiente de entregar a WM_Central")

def menu():
    global averia
    while True:
        print("\n1) Solicitar riego  2) Finalizar riego  3) Simular/resolver fuga  4) Ver estado")
        op = input("> ").strip()
        if op == "4":
            mostrar_estado()
        elif WS_ID is None:
            print("Esperando a que el Monitor autentique la estación")
        elif op == "1":
            # El riego manual no abre la válvula directamente, se pide a central, que valida y responde con una orden START
            if not monitor_ok:
                print("Estacion AVERIADA: sin conexion con el Monitor. No se admiten riegos")
            elif central_conectada is False:
                print("[ERROR] Imposible conectar con WM_Central")
            else:
                t = input("Duracion (s): ").strip()
                if not t.isdigit() or int(t) == 0:
                    print("Duracion no valida")
                elif enviar_evento(f"REQUEST{SEP}{WS_ID}{SEP}MANUAL{SEP}{t}"):
                    print("Peticion enviada a WM_Central. Esperando autorización")
        elif op == "2":
            if not finalizar_riego("MANUAL"):
                print("No hay ningún riego en curso")
        elif op == "3":
            # Alterna la fuga. Con fuga se responde KO al Monitor(que avisa a Central) y el riego en curso corta inmediatamente.
            with lock:
                averia = not averia
                if averia:
                    cerrar_valvula("FUGA")
            print("[AVERIA] Fuga simulada. Se responderá KO al Monitor" if averia
                  else "[AVERIA RESUELTA] Se responderá OK al Monitor")
        else:
            print("Opción no válida")

##################### MAIN #####################

if __name__ == "__main__":
    print("**** WM_WS_E: ENGINE DE LA WATERING STATION ****")
    if len(sys.argv) != 5:
        print("Argumentos necesarios: <KafkaIP> <KafkaPuerto> <MonitorIP> <MonitorPuerto>")
        sys.exit(1)
    KAFKA_ADDR = f"{sys.argv[1]}:{sys.argv[2]}"
    try:
        MONITOR_ADDR = (sys.argv[3], int(sys.argv[4]))
    except ValueError:
        print("[ERROR] El puerto del Monitor debe ser un número")
        sys.exit(1)

    # Hilos en segundo plano que terminan solos cuando acaba el programa
    threading.Thread(target=hilo_monitor, daemon=True).start()
    threading.Thread(target=hilo_ordenes, daemon=True).start()
    threading.Thread(target=hilo_riego, daemon=True).start()

    try:
        menu()
    except KeyboardInterrupt:
        print("\n[FIN] Engine detenido")
    