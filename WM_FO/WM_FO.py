# ===================================================================================================================
#   Aplicación del operario
#   Permite a un operario solicitar riegos en una estación (ws), 
#   seguir su progreso y consultar las estaciones.
#
# Uso: python WM_FO.py <KafkaIP> <KafkaPuerto> <ID_Operario> [fichero_servicios]
#   - si se indica ficehro_servicios, al arrancar se ejecutan automaticamente todos 
#     sus servicios.
#   -Formato del fichero: una línea por servicio -> ID_WS#segundos (ej. WS-01#30)
#
# Hilos:
#   1. Hilo principal: menú del operario y ejecución de los servicios 
#   (uno detras del otro)
#   2.Hilo de notificaciones: consume de kafka los mensajes que Central envía a 
#   este operario
#
# El FO solo se comunica con Central, y siempre con kafka
#
# Mensajes:
#   kafka FO -> Central (wm_fo_request):        REQUEST#FO_ID#WS_ID#segundos (solicitar un riego)
#                                               LIST#FO_ID                   (consultar estaciones)
#   kafka Central -> FO (wm_fo_notifications):  INFO#FO_ID#WS_ID#texto (pasos intermedios)
#                                               AUTH#FO_ID#WS_ID    (riego autorizado)
#                                               DENIED#FO_ID#WS_ID#motivo (riego denegado)
#                                               STATUS#FO_ID#WS_ID#caudal#litros (cada segundo durante el riego)
#                                               END#FO_ID#WS_ID#litros#segundos#motivo (resumen final)
#                                               STATIONS#FO_ID#WS_01;River Park;AVAILABLE|WS-02;Central Park;LEAK|...
# ===================================================================================================================

import sys
import time
import threading
from kafka import KafkaConsumer, KafkaProducer

sys.stdout.reconfigure(line_buffering=True) # en Docker, cada print se muestra al momento

FORMAT = 'utf-8'
SEP = "#"

TOPIC_REQUESTS = "wm_fo_requests" # FO > CENTRAL
TOPIC_NOTIFY = "wm_fo_notifications" # CENTRAL > FO

ESPERA_ENTRE_SERVICIOS = 4  # s entre servicios del fichero
TIMEOUT_RESPUESTA = 30      # s maximos esperando respuesta de CENTRAL a una peticion
TIMEOUT_SILENCIO = 5        # s sin noticias durante un riego -> CENTRAL no responde
MARGEN_FIN = 60             # s extra sobre la duración antes de dejar esperar el resumen

# Se rellenan en el main con los argumentos de la linea de comandos
KAFKA_ADDR = None   # "IP:puerto" del broker 
FO_ID = None
consumer = None

#################### ESTADO DEL SERVICIO EN CURSO #################### 
lock = threading.Lock()                     # protege las variables de abajo
ws_actual = None                            # WS que esta atendiendo al operario 
fase = None                                 # "ESPERANDO_AUTORIZACION", "REGANDO" o None
ultimo_mensaje = 0.0                        # instante del ultimo mensaje de CENTRAL sobre el servicio
servicio_terminado = threading.Event()      # se activa al llegar END o DENIED del servicio en curso
estaciones_recibidas = threading.Event()    # se activa al llegar la lista de estaciones
escuchando = threading.Event()              # se activa cuando el consumidor de kafka esta listo

producer = None     # productor kafka (sse crea en el primer envío)
kafka_ok = True     # para mostrar el error de conexión solo una vez

#################### KAFKA: PETICIONES HACIA CENTRAL #################### 

def enviar(msg):
    # Publica una petición y espera la confirmación de broker. Devuelve True si ha llegado.
    global producer, kafka_ok
    try:
        if producer is None:
            producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR, max_block_ms=5000)
        producer.send(TOPIC_REQUESTS, msg.encode(FORMAT)).get(timeout=5)
        if not kafka_ok:
            print("[INFO] Conexión con WM_Central restablecida")
            kafka_ok = True
        return True
    except Exception:
        if kafka_ok:    # el error se muestra una sola vez
            print("[ERROR] Imposible conectar con WM_Central")
            kafka_ok = False
        if producer is not None:    # se descarta el productor roto
            try:
                producer.close(timeout=0)
            except Exception:
                pass
        producer = None
        return False

#################### KAFKA: NOTIFICACIONES DE CENTRAL #################### 

def mostrar_estaciones(lista):
    #lista = "WS-01;River Park;AVAILABLE|WS-02;Central Park;LEAK|..."
    print(f"\n{'ID':<8}{'UBICACIÓN':<24}ESTADO")
    print("-" * 46)
    disponibles = 0
    for entrada in filter(None, lista.split("|")):  #filter(None, ..) descarta trozos vacíos
        campos = entrada.split(";")
        if len(campos) != 3:
            continue    #entrada mal formada, se ignora
        ws, ubicacion, estado = campos
        marca = " <- disponible" if estado == "DISPONIBLE" else ""
        disponibles += (estado == "DISPONIBLE")
        print(f"{ws:<8}{ubicacion:<24}{estado}{marca}")
    print(f"Estaciones disponibles: {disponibles}")

def terminar_servicio():
    # Marca el servicio en curso como terminado
    global fase, ws_actual
    fase = None
    ws_actual = None
    servicio_terminado.set()    # despierta al hilo principal

def procesar_notificacion(texto):
    # Interpreta un mensaje de CENTRAL. Solo se procesan los dirigidos a ESTE operario:
    # así el operario solo ve los mensajes de la WS que le está dando servicio.
    global fase, ultimo_mensaje
    p = texto.split(SEP)
    if len(p) < 3 or p[1] != FO_ID:
        return  # es para otro operario: se ignora
    tipo = p[0]
    try:
        if tipo == "STATIONS":      # STATIONS#FO_ID#lista
            mostrar_estaciones(p[2])
            estaciones_recibidas.set()
            return
        ws = p[2]
        with lock:
           es_actual = (ws == ws_actual)    # ¿es del servicio que se está esperando? 
           if es_actual:
               ultimo_mensaje = time.time() # central sigue viva
        if tipo == "INFO":                  # INFO#FO_ID#WS#texto
            print(f"[CENTRAL] {ws}: {p[3]}")
        elif tipo == "AUTH":                # AUTH#FO_ID#WS
            print(f"[AUTORIZADO] Riego autorizado en {ws}. Electroválvula abierta")
            with lock:
                if es_actual:
                    fase = "REGANDO"
        elif tipo == "DENIED":              # DENIED#FO_ID#WS#motivo
            print(f"[DENEGADO] Riego denegado en {ws} (motivo: {p[3]})")
            with lock:
                if es_actual:
                    terminar_servicio()
        elif tipo == "STATUS":              # STATUS#FO_ID#WS#caudal#litros
            print(f"[RIEGO] {ws} | {p[3]} L/min | {p[4]} L acumulados")
        elif tipo == "END":
            # Si no es el servicio en curso, es un resultado que llegó tarde mientras el 
            # FO estaba cerrado: se muestra igualmente para que el operario lo vea.
            titulo = "RESUMEN DEL RIEGO" if es_actual else "RESULTADO DE UN SERVICIO ANTERIOR"
            print(f"\n============ {titulo} ============")        
            print(f" Estación: {ws}")        
            print(f" Volumen total: {p[3]} L")        
            print(f" Duración: {p[4]} s")        
            print(f" Motivo de finalización: {p[5]}")        
            print("-" * (22 + len(titulo)))
            with lock:
                if es_actual:
                    terminar_servicio()
        else:
            print(f"[AVISO] Mensaje desconocido de WM_Central: {texto}")
    except IndexError:
        print(f"[ERROR] Mensaje mal formado de WM_CENTRAL: {texto}")

def hilo_notificaciones():
    # Escucha los mensajes de CENTRAL para los operarios.
    # - group_id propio de cada operario (WM_FO_<ID>): kafka recuerda hasta dónde ha leído 
    #   ESTE operario. Si el FO se cierra durante un riego, los mensajes siguen guardados  
    #   en kafka y, al volver a arrancar con el mismo ID, se reciben los que llegaron 
    #   mientras estaba cerrado  
    # - auto_offset_reset="earliest": solo se usa la primera vez que existe el grupo
    global consumer
    while True:
        try:
            consumer = KafkaConsumer(TOPIC_NOTIFY, bootstrap_servers=KAFKA_ADDR, 
                                     group_id=f"WM_FO_{FO_ID}", auto_offset_reset="earliest")
            while not consumer.assignment():
                for registros in consumer.poll(timeout_ms=200).values():
                    for m in registros:
                        procesar_notificacion(m.value.decode(FORMAT))
            escuchando.set()
            for m in consumer:  # espera mensajes indefinidamente
                procesar_notificacion(m.value.decode(FORMAT))
        except Exception as e:
            print(f"[ERROR] Sin conexión con el broker Kafka ({e}. Reintentando)")
            time.sleep(2)

#################### SERVICIOS #################### 

def solicitar_servicio(ws,segundos):
    global ws_actual, fase, ultimo_mensaje
    with lock:
        ws_actual, fase = ws, "ESPERANDO_AUTORIZACION"
        ultimo_mensaje = time.time()
    servicio_terminado.clear()
    print(f"\n[SERVICIO] Solicitando riego en {ws} durante {segundos} s")
    if not enviar(f"REQUEST{SEP}{FO_ID}{SEP}{ws}{SEP}{segundos}"):
        print("[SERVICIO] No se ha podido enviar la petición")
    else:
        limite = time.time() + segundos + MARGEN_FIN    # tiempo maximo esperando el resumen
        avisado = False
        while not servicio_terminado.wait(1):   # se comprueba cada segundo
            with lock:
                f = fase
                silencio = time.time() - ultimo_mensaje
            if f == "ESPERANDO_AUTORIZACION" and silencio > TIMEOUT_RESPUESTA:
                # CENTRAL no contesta: no se admiten peticiones mientras no esté disponible
                print("[ERROR] Sin respuesta de WM_Central a la peticion. " \
                        "Servicio cancelado, si el riego llega a hacerse, el resumen se muestra")
                break
            if f == "REGANDO":
                if silencio > TIMEOUT_SILENCIO and not avisado:
                    # EL riego sigue en la estacion; el resumen llegará cuando CENTRAL vuelva
                    print("[ERROR] Imposible conectar con WM_Central. El riego continúa en la estación")
                    avisado = True
                elif silencio <= TIMEOUT_SILENCIO and avisado:
                    print("[INFO] WM_Central vuelve a responder")
                    avisado = False
            if time.time() > limite:
                print("[AVISO] No ha llegado el resumen final. Se mostrará en cuanto llegue")
                break
    with lock:  # el operario queda libre
        ws_actual, fase = None, None

def ejecutar_fichero(ruta):
    # Lee el fichero de servicios y los solicita uno a uno. Tras cada uno (con éxito 
    # o no) espera 4 segundos antes del siguiente.

    try:
        with open(ruta) as f:
            lineas = f.read().splitlines()
    except OSError as e:
        print(f"[ERROR] No se puede leer el fichero {ruta} ({e})")
        return
    servicios = []
    for n, linea in enumerate(lineas, start=1): # n = numero de lineas (para los avisos)
        linea = linea.strip()
        if not linea:
            continue    # lineas vacias se saltan
        p = linea.split(SEP)
        if len(p) != 2 or not p[1].strip().isdigit() or int(p[1]) == 0:
            print (f"[AVISO] Línea {n} ignorada (formato ID_WS#segundos): {linea}")
            continue
        servicios.append((p[0].strip(),int(p[1])))
    print(f"[FICHERO] {len(servicios)} servicios a solicitar")
    for i, (ws, segundos) in enumerate(servicios, start=1):
        print(f"\n----------- Servicio {i}/{len(servicios)} -----------")
        solicitar_servicio(ws, segundos)
        if i < len(servicios):
            print(f"[FICHERO] Siguiente servicio en {ESPERA_ENTRE_SERVICIOS} s")
            time.sleep(ESPERA_ENTRE_SERVICIOS)
    print("\n[FICHERO] Todos los servicios del fichero se han procesado")

def ver_estaciones():
    # Pide a CENTRAL la lista de estaciones
    estaciones_recibidas.clear()
    if enviar(f"LIST{SEP}{FO_ID}") and not estaciones_recibidas.wait(TIMEOUT_RESPUESTA):
        print("[ERROR] Imposible conectar con WM_Central: no ha enviado la lista de estaciones")

#################### MENÚ #################### 

def menu():
    while True:
        print("\n1) Solicitar riego  2) Ejecutar fichero de servicios  3) Ver estaciones")
        op = input("> ").strip()
        if op == "1":
            ws = input("ID de la estacion: ").strip()
            t = input("Duración (s): ").strip()
            if not ws or not t.isdigit() or int(t) == 0:
                print("Datos no válidos")
            else:
                solicitar_servicio(ws, int(t))
        elif op == "2":
            ejecutar_fichero(input("Ruta del fichero: ").strip())
        elif op == "3":
            ver_estaciones()
        else:
            print("Opción no válida")

#################### MAIN #################### 

print("**** WM_FO: APLICACIÓN DEL OPERARIO ****")
if len(sys.argv) not in (4,5):
    print("Argumentos necesarios: <KafkaIP> <KafkaPuerto> <ID_Operario> [fichero_servicios]")
    sys.exit(1)
KAFKA_ADDR = f"{sys.argv[1]}:{sys.argv[2]}"
FO_ID = sys.argv[3]
print(f"Operario: {FO_ID}")

threading.Thread(target=hilo_notificaciones, daemon=True).start()
print("Conectando con el broker Kafka")
if not escuchando.wait(15): # se espera a estar escuchando antes de pedir nada
    print("[ERROR] Imposible conectar con WM_Central. Se seguirá intentando")
try:
    if len(sys.argv) == 5:   # con servicios del fichero
        ejecutar_fichero(sys.argv[4])
    menu()
except KeyboardInterrupt:
    if consumer is not None:
        try:
            consumer.close()
        except Exception:
            pass
    print("\n[FIN] Aplicación del operario cerrada")