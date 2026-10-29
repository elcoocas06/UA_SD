import socket 
import threading
import sys
import time
import sqlite3                  # base de datos SQLite
import os                       # variables de entorno (Railway)
import uuid                     # identificador único de consumidor Kafka
import html                     # para escapar texto en la página web
from collections import deque   # lista con tamaño máximo (para los mensajes del panel)
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer   # servidor web del panel
from urllib.parse import parse_qs                                      # leer formularios web
 
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
 
########## CONSTANTES ##########
 
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
SEP = "#"
TIMEOUT_MONITOR = 5             # si un Monitor no envía nada en 5 s => lo damos por desconectado
TIMEOUT_AUTORIZACION = 5        # segundos que esperamos a que la WS empiece a regar tras un START
TIMEOUT_SIN_DATOS = 10          # segundos sin DATA de una WS que riega => damos el riego por interrumpido
ANTIGUEDAD_MAXIMA = 10          # Una petición más antigua (enviada con CENTRAL caída) no se atiende
 
# Topics de Kafka
TOPIC_ORDERS = "wm_ws_orders"       # CENTRAL -> Engines (texto)
TOPIC_EVENTS = "wm_ws_events"       # Engines -> CENTRAL (texto)
TOPIC_REQUESTS = "wm_fo_requests"       # FO -> CENTRAL (texto)   
TOPIC_OPERATORS = "wm_fo_notifications" # CENTRAL -> FO (texto)   
TOPICS = [TOPIC_ORDERS, TOPIC_EVENTS, TOPIC_REQUESTS, TOPIC_OPERATORS]
 
# Estados que se muestran en el panel
DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA DE SERVICIO"
DESCONECTADA = "DESCONECTADA"
 
DURACION_POR_DEFECTO = 20
DB_FILE = os.environ.get("WM_DB", "wm_central.db")     # en Railway: ruta dentro de un volumen
OPERARIOS_INICIALES = [("FO-01", "Operario 1"), ("FO-02", "Operario 2"), ("FO-03", "Operario 3"),
                       ("FO-04", "Operario 4"), ("FO-05", "Operario 5")]

# Se pueden dar de alta más desde el panel web ("Dar de alta una estación").
ESTACIONES_INICIALES = [("WS-01", "River Park"), ("WS-02", "Central Park"),
                        ("WS-03", "Riverside Gardens"), ("WS-04", "North Roundabout")]
OPERADORES_ESPECIALES = ("MANUAL", "CENTRAL")           # riego desde la propia WS o desde el panel
 
# Motivos con los que el Engine cierra un riego (mensaje END)
MOTIVOS_END = {"TIEMPO": "tiempo límite alcanzado",
               "STOP": "parada ordenada por CENTRAL",
               "MANUAL": "finalizado desde la propia estación",
               "BLOQUEADA": "bloqueo ordenado por CENTRAL",
               "FUGA": "fuga detectada",
               "MONITOR_CAIDO": "el Monitor de la estación se ha caído",
               "RECUPERADO": "riego recuperado tras reiniciarse la estación",
               "SIN_DATOS": "la estación dejó de enviar datos",
               "AVERIA": "avería detectada por el Monitor (fuga o Engine caído)"}   # CAMBIO
 
# Motivos con los que el Engine rechaza un START (mensaje REJECT)
MOTIVOS_REJECT = {"AVERIA": "la estación tiene una avería",
                  "BLOQUEADA": "la estación está bloqueada",
                  "SIN_MONITOR": "la estación no tiene Monitor",
                  "OCUPADA": "la estación ya está regando",
                  "RIEGO_PENDIENTE": "la estación aún no ha entregado el resumen del riego anterior"}
 
 
########## VARIABLES COMPARTIDAS ##########
 
estaciones = {}                 # id de WS -> diccionario con su información (ver nueva_estacion)
lock_est = threading.Lock()     # Protege el acceso al estado de las estaciones
lock_db = threading.Lock()      # Protege el acceso a SQLite
mensajes = deque(maxlen=60)     # últimos mensajes (se muestran en el panel)
producer = None
listo_peticiones = threading.Event()    # un consumidor para peticiones de los FO...
listo_eventos = threading.Event()       # un consumidor para los eventos de los Engines
 
 
########## LOG ##########
 
def log(texto):
    linea = f"{time.strftime('%H:%M:%S')} {texto}"
    print(linea, flush=True)
    mensajes.append(linea)
 
########## FUNCIONES DE MENSAJERIA ##########
 
def send(sock, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    sock.sendall(send_length)
    sock.sendall(message)
 
 
def recibir_exacto(sock, n):
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
                "fecha TEXT, duracion REAL, volumen REAL, motivo TEXT)")
    for id_op, nombre in OPERARIOS_INICIALES:
        db_ejecutar("INSERT OR IGNORE INTO operarios VALUES (?, ?)", (id_op, nombre))
    for ws_id, ubicacion in ESTACIONES_INICIALES:                  
        db_ejecutar("INSERT OR IGNORE INTO estaciones VALUES (?, ?, 'ACTIVA')", (ws_id, ubicacion))
 
 
# Comprobar si un operario está registrado
def operario_existe(id_op):
    return len(db_ejecutar("SELECT id FROM operarios WHERE id = ?", (id_op,), leer=True)) > 0
 
 
########## ESTACIONES (estado en memoria) ##########
 
def nueva_estacion(ws_id, ubicacion):
    return {"id": ws_id, "ubicacion": ubicacion,
            "conectado": False,      # ¿hay un Monitor conectado por socket?
            "conn": None,            # socket de ese Monitor
            "monitor": "WAIT",       # último estado del Monitor: WAIT (sin Engine) / OK / KO (avería)
            "bloqueada": False,      # CENTRAL la ha puesto fuera de servicio
            "caudal": 0.0, "volumen": 0.0, "operario": "-",
            "riego_op": None,        # operario del riego en curso (None = no hay riego)
            "riego_inicio": None,
            "ultimo_data": None,     # hora del último DATA recibido
            "pendiente": None}       # START enviado y sin confirmar: (operario, hora)
 
 
def cargar_estaciones():
    # Al arrancar: cargamos de la BD las estaciones ya dadas de alta. Todas DESCONECTADAS
    # hasta que su Monitor conecte (no conocemos su estado real).
    for ws_id, ubicacion, estado in db_ejecutar("SELECT id, ubicacion, estado FROM estaciones", leer=True):
        estaciones[ws_id] = nueva_estacion(ws_id, ubicacion)
        estaciones[ws_id]["bloqueada"] = (estado == "BLOQUEADA")    # el bloqueo sobrevive a un reinicio
        log(f"[BD] Estación {ws_id} ({ubicacion}) cargada: DESCONECTADA hasta que conecte"
            f"{' (bloqueada)' if estado == 'BLOQUEADA' else ''}")
 
 
def estado_visible(ws):
    # Calcula el estado que se muestra en el panel. Llamar SIEMPRE con lock_est cogido.
    if not ws["conectado"] or ws["monitor"] == "WAIT":
        return DESCONECTADA            # sin Monitor, o el Engine todavía no ha conectado
    if ws["monitor"] == "KO":
        return FUGA
    if ws["bloqueada"]:
        return FUERA_SERVICIO
    if ws["riego_op"] is not None:
        return REGANDO
    return DISPONIBLE
 
 
########## KAFKA ##########
 
def conectar_kafka():
    global producer
    while producer is None:
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR)
            log(f"[KAFKA] Conectado al broker {KAFKA_ADDR}")
        except NoBrokersAvailable:
            log(f"[KAFKA] Broker {KAFKA_ADDR} no disponible. Reintento en 3s")
            time.sleep(3)
 
 
def crear_topics():
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_ADDR)
        for topic in TOPICS:
            try:
                admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
            except TopicAlreadyExistsError:
                pass
        admin.close()
    except Exception as e:
        log(f"[KAFKA] No se han podido crear los topics ({e}). Se confía en la creación automática")
 
 
def publicar(topic, texto):
    # envío con errores controlados. Si Kafka falla, se registra y CENTRAL sigue funcionando.
    try:
        producer.send(topic, texto.encode(FORMAT))
        producer.flush(timeout=5)
    except Exception as e:
        log(f"[KAFKA] No se ha podido enviar '{texto}' ({e})")
 
 
def enviar_orden(texto):
    # Orden de texto para los Engines
    publicar(TOPIC_ORDERS, texto)
 
 
def avisar(operador, mensaje, texto_log=None):
    # mensaje de TEXTO para un operario, con el formato que entiende WM_FO:
    #   TIPO#FO_ID#WS_ID#campos...  (INFO, AUTH, DENIED, STATUS, END, STATIONS)
    # Si se indica texto_log, se muestra también en la pantalla de CENTRAL.
    if texto_log:
        log(f"[CENTRAL -> {operador}] {texto_log}")
    publicar(TOPIC_OPERATORS, mensaje)
 
 
def info(operador, ws_id, texto):
    # paso intermedio de una petición -> INFO#FO_ID#WS_ID#texto
    avisar(operador, f"INFO{SEP}{operador}{SEP}{ws_id}{SEP}{texto}", f"{ws_id}: {texto}")
 
 
def enviar_lista(operador):
    # respuesta a LIST -> STATIONS#FO_ID#WS-01;ubicacion;ESTADO|WS-02;...
    with lock_est:
        filas = [(ws["id"], ws["ubicacion"], estado_visible(ws))
                 for ws in sorted(estaciones.values(), key=lambda w: w["id"])]
    # ';' y '|' separan los datos de la lista: se quitan de la ubicación por si acaso
    lista = "|".join(f"{i};{u.replace(';', ',').replace('|', '/')};{e}" for i, u, e in filas)
    avisar(operador, f"STATIONS{SEP}{operador}{SEP}{lista}", f"Lista de {len(filas)} estaciones enviada")
 
 
########## LOGICA: PETICIONES DE RIEGO ##########
 
def denegar(operador, ws_id, motivo):
    # DENIED#FO_ID#WS_ID#motivo
    avisar(operador, f"DENIED{SEP}{operador}{SEP}{ws_id}{SEP}{motivo}", f"Riego DENEGADO en {ws_id}: {motivo}")
    if operador == "MANUAL":
        # la petición se hizo desde el menú de la propia WS. Nadie escucha al
        # "operario" MANUAL, así que la respuesta se envía a la estación como orden DENIED.
        enviar_orden(f"DENIED{SEP}{ws_id}{SEP}{motivo}")
 
 
def tratar_peticion(operador, ws_id, duracion):
    # Pasos de la mecánica de la solución: validar -> pedir autorización a la WS -> riego
    info(operador, ws_id, f"Petición recibida: riego durante {duracion} s")      
 
    # 1) ¿Operario válido?
    if operador not in OPERADORES_ESPECIALES and not operario_existe(operador):
        denegar(operador, ws_id, "operario no registrado en CENTRAL")
        return
    if duracion <= 0:
        denegar(operador, ws_id, "duración no válida")
        return
 
    # 2) ¿Existe la WS y está disponible?
    motivo = None                       # si queda None, la petición es válida
    with lock_est:
        ws = estaciones.get(ws_id)
        if ws is None:
            motivo = "la estación no existe"
        else:
            estado = estado_visible(ws)
            if estado != DISPONIBLE:
                motivo = f"la estación no está disponible (estado: {estado})"
            elif ws["pendiente"] is not None or ws["riego_op"] is not None:
                motivo = "la estación está atendiendo otra petición"
            else:
                ws["pendiente"] = (operador, time.time())      # reservamos la WS
    if motivo is not None:
        denegar(operador, ws_id, motivo)
        return
 
    # 3) Pedimos autorización a la WS: le enviamos START. Si no puede, contestará REJECT;
    #    si puede, empezará a enviar DATA cada segundo.
    info(operador, ws_id, "Estación disponible. Solicitando autorización a la estación...")   # CAMBIO
    enviar_orden(f"START{SEP}{ws_id}{SEP}{operador}{SEP}{duracion}")
 
 
def cerrar_riego(ws_id, motivo, volumen=None, duracion=None, operador=None):
    # Da por terminado un riego: lo guarda en la BD y avisa al operario con el resumen.
    with lock_est:
        ws = estaciones.get(ws_id)
        if ws is None:
            return
        if operador is None:
            operador = ws["riego_op"]
            if operador is None:
                return                  # no había riego que cerrar
        if volumen is None:
            volumen = ws["volumen"]
        if duracion is None:
            duracion = time.time() - ws["riego_inicio"] if ws["riego_inicio"] else 0
        ws["riego_op"] = None
        ws["riego_inicio"] = None
        ws["operario"] = "-"
        ws["caudal"] = 0.0
        ws["volumen"] = 0.0
    db_ejecutar("INSERT INTO riegos (ws, operario, fecha, duracion, volumen, motivo) VALUES (?,?,?,?,?,?)",
                (ws_id, operador, time.strftime("%Y-%m-%d %H:%M:%S"), duracion, volumen, motivo))
    enviar_resumen(operador, ws_id, volumen, duracion, motivo)      
 
 
def enviar_resumen(operador, ws_id, volumen, duracion, motivo):
    # END#FO_ID#WS_ID#litros#segundos#motivo
    texto_motivo = MOTIVOS_END.get(motivo, motivo)
    avisar(operador, f"END{SEP}{operador}{SEP}{ws_id}{SEP}{volumen:.2f}{SEP}{duracion:.0f}{SEP}{texto_motivo}",
           f"RESUMEN del riego en {ws_id}: volumen {volumen:.2f} L, duración {duracion:.0f} s, motivo: {texto_motivo}")
 
 
########## LOGICA: EVENTOS QUE LLEGAN DE LOS ENGINES ##########
 
def tratar_data(p, reciente=True):
    # DATA#id#operario#caudal#litros
    # reciente=False: DATA antiguo releído de Kafka tras un reinicio de CENTRAL (se actualiza el
    # estado, pero no se reenvía al operario un progreso que ya no es actual)
    ws_id, operador, caudal, litros = p[1], p[2], float(p[3]), float(p[4])
    with lock_est:
        ws = estaciones.get(ws_id)
        if ws is None:
            return                              # estación no dada de alta: la ignoramos
        ws["caudal"], ws["volumen"], ws["operario"] = caudal, litros, operador
        ws["ultimo_data"] = time.time()
        es_el_primero = ws["riego_op"] is None
        if es_el_primero:                       # primer DATA: la WS ha aceptado el START
            ws["riego_op"] = operador
            ws["riego_inicio"] = time.time()
            ws["pendiente"] = None
    if not reciente:                                                
        return
    if es_el_primero:                                               # AUTH#FO_ID#WS_ID
        avisar(operador, f"AUTH{SEP}{operador}{SEP}{ws_id}", f"{ws_id} ha AUTORIZADO el riego. Electroválvula abierta")
    # Progreso del riego en la terminal de CENTRAL cada segundo
    print(f"{time.strftime('%H:%M:%S')} [RIEGO] {ws_id} | {caudal} L/min"
          f"{litros:.2f} L acumulados | operario {operador}", flush=True)
    # El operario solo recibe los mensajes de la WS que le está prestando servicio
    avisar(operador, f"STATUS{SEP}{operador}{SEP}{ws_id}{SEP}{caudal}{SEP}{litros:.2f}")   # CAMBIO
 
 
def tratar_end(p):
    # END#id#operario#litros#segundos#motivo
    ws_id, operador, litros, segundos, motivo = p[1], p[2], float(p[3]), float(p[4]), p[5]
    with lock_est:
        ws = estaciones.get(ws_id)
        abierto = ws is not None and ws["riego_op"] is not None
    if not abierto:
        # CAMBIO: CENTRAL ya dio este riego por terminado antes de recibir el END (avería informada
        # por el Monitor, o la estación dejó de enviar datos) y lo guardó con los últimos datos que
        # tenía. Ahora llega el resultado REAL de la estación: se CORRIGE ese registro en vez de crear
        # otro (el mismo riego no aparece dos veces) y se envía al operario el resultado definitivo.
        filas = db_ejecutar("SELECT id, operario, motivo FROM riegos WHERE ws = ? ORDER BY id DESC LIMIT 1",
                            (ws_id,), leer=True)
        if filas and filas[0][1] == operador and filas[0][2] in ("AVERIA", "SIN_DATOS"):
            db_ejecutar("UPDATE riegos SET duracion = ?, volumen = ?, motivo = ? WHERE id = ?",
                        (segundos, litros, motivo, filas[0][0]))
            enviar_resumen(operador, ws_id, litros, segundos, motivo)
            return
    cerrar_riego(ws_id, motivo, volumen=litros, duracion=segundos, operador=operador)
 
 
def tratar_reject(p):
    # REJECT#id#operario#motivo
    ws_id, operador, motivo = p[1], p[2], p[3]
    if motivo == "None":
        # Protección: un REJECT con motivo "None" no es un rechazo real (lo enviaban versiones
        # antiguas del Engine al empezar un riego). Se ignora.
        return
    with lock_est:
        ws = estaciones.get(ws_id)
        if ws is not None:
            ws["pendiente"] = None
    denegar(operador, ws_id, f"la estación rechaza el riego ({MOTIVOS_REJECT.get(motivo, motivo)})")
 
 
def tratar_evento(texto, reciente=True):
    p = texto.split(SEP)
    tipo = p[0]
    if tipo == "REQUEST":                       # REQUEST#id#MANUAL#segundos
        if not reciente:                        # CAMBIO: enviada mientras CENTRAL estaba caída
            log(f"[AVISO] Petición manual antigua de {p[1]} ignorada (CENTRAL no estaba disponible)")
            return
        tratar_peticion(p[2], p[1], int(p[3]))
    elif tipo == "DATA":
        tratar_data(p, reciente)
    elif tipo == "END":
        tratar_end(p)
    elif tipo == "REJECT":
        tratar_reject(p)
    else:
        log(f"[AVISO] Evento desconocido de un Engine: {texto}")
 
 
########## HILOS ##########
 
def tratar_peticion_fo(texto, reciente=True):
    # CAMBIO: peticiones de texto de los operarios (WM_FO)
    #   REQUEST#FO_ID#WS_ID#segundos   |   LIST#FO_ID
    p = texto.split(SEP)
    if p[0] == "REQUEST":
        tratar_peticion(p[1], p[2], int(p[3]))
    elif p[0] == "LIST":
        enviar_lista(p[1])
    else:
        log(f"[AVISO] Petición desconocida de un operario: {texto}")
 
 
def hilo_consumidor(topic, grupo, desde, procesar, listo, descripcion):
    # CAMBIO: un consumidor por topic, porque cada uno necesita una política distinta:
    #  - Peticiones de los FO: grupo nuevo en cada arranque + "latest" -> solo se atienden
    #    peticiones nuevas. Las enviadas mientras CENTRAL estaba caída NO se ejecutan.
    #  - Eventos de los Engines: grupo FIJO -> Kafka recuerda hasta dónde se leyó. Si CENTRAL
    #    cae, al volver lee los DATA/END que llegaron mientras tanto: no se pierde ningún riego.
    consumer = KafkaConsumer(topic, bootstrap_servers=KAFKA_ADDR, group_id=grupo, auto_offset_reset=desde)
 
    def procesar_seguro(mensaje):
        # Un mensaje mal formado no debe tumbar el hilo: lo registramos y seguimos
        try:
            reciente = time.time() - mensaje.timestamp / 1000 <= ANTIGUEDAD_MAXIMA
            procesar(mensaje.value.decode(FORMAT), reciente)
        except Exception as e:
            log(f"[ERROR] Mensaje ignorado ({e!r}): {mensaje.value!r}")
 
    # Hasta que Kafka asigna el topic no se puede recibir nada. Los mensajes que devuelva
    # poll() mientras tanto también se procesan (antes se descartaban).
    while not consumer.assignment():
        for registros in consumer.poll(timeout_ms=200).values():
            for mensaje in registros:
                procesar_seguro(mensaje)
    listo.set()
    log(f"[KAFKA] CENTRAL escuchando {descripcion}")
    for mensaje in consumer:
        procesar_seguro(mensaje)
 
 
def hilo_estado():
    # Cada segundo: muestra en la pantalla de CENTRAL los cambios de estado de las estaciones.
    # CAMBIO: los operarios ya no reciben la tabla cada segundo (la piden con LIST), y la BD
    # guarda en "estado" si la WS está BLOQUEADA o ACTIVA (lo que hay que recordar tras un reinicio).
    ultimo = {}
    while True:
        with lock_est:
            tabla = [(ws["id"], estado_visible(ws)) for ws in estaciones.values()]
        for ws_id, estado in tabla:
            if ultimo.get(ws_id) != estado:
                if ws_id in ultimo:
                    log(f"[WS {ws_id}] {ultimo[ws_id]} -> {estado}")
                ultimo[ws_id] = estado
        time.sleep(1)
 
 
def hilo_vigilar():
    # Cada segundo comprueba dos cosas:
    #  - Si una WS no ha respondido a un START a tiempo, se deniega el riego.
    #  - Si una WS que riega deja de enviar DATA, se da el riego por interrumpido.
    while True:
        caducadas = []
        sin_datos = []
        with lock_est:
            ahora = time.time()
            for ws in estaciones.values():
                if ws["pendiente"] is not None and ahora - ws["pendiente"][1] > TIMEOUT_AUTORIZACION:
                    caducadas.append((ws["pendiente"][0], ws["id"]))
                    ws["pendiente"] = None
                if ws["riego_op"] is not None and ws["ultimo_data"] and ahora - ws["ultimo_data"] > TIMEOUT_SIN_DATOS:
                    sin_datos.append(ws["id"])
        for operador, ws_id in caducadas:
            denegar(operador, ws_id, "la estación no ha respondido a la solicitud de autorización")
        for ws_id in sin_datos:
            cerrar_riego(ws_id, "SIN_DATOS")
        time.sleep(1)
 
 
########## LADO SERVIDOR: MONITORES (sockets) ##########
 
def handle_monitor(conn, addr):
    print(f"[NUEVA CONEXION] Monitor {addr} connected.")
    conn.settimeout(TIMEOUT_MONITOR)
    ws_id = None
    try:
        # ---- 1) Registro / autenticación ----
        peticion = recibir(conn).split(SEP)
        if peticion[0] != "REGISTER" or len(peticion) < 3 or not peticion[1].strip():
            send(conn, f"KO{SEP}registro incorrecto")
            return
        nuevo_id, ubicacion = peticion[1].strip(), peticion[2]
 
        # CAMBIO (#3): autenticación con aceptación o denegación
        #  - KO#NO_REGISTRADA#texto: el ID no está dado de alta en CENTRAL (el Monitor termina)
        #  - KO#DUPLICADA#texto: ya hay un Monitor conectado con ese ID (el Monitor reintenta)
        with lock_est:
            ws = estaciones.get(nuevo_id)
            if ws is None:
                denegacion = f"NO_REGISTRADA{SEP}la estación {nuevo_id} no está dada de alta en CENTRAL"
            elif ws["conectado"]:
                denegacion = f"DUPLICADA{SEP}ya hay un Monitor conectado con el ID {nuevo_id}"
            else:
                denegacion = None
                if ubicacion != "UNKNOWN":
                    ws["ubicacion"] = ubicacion
                ubicacion = ws["ubicacion"]
                ws["conn"] = conn
                ws["conectado"] = True
                ws["monitor"] = "WAIT"
        if denegacion:
            log(f"[REGISTRO DENEGADO] {nuevo_id}: {denegacion.split(SEP)[1]}")
            send(conn, f"KO{SEP}{denegacion}")
            return
        ws_id = nuevo_id
        db_ejecutar("UPDATE estaciones SET ubicacion = ? WHERE id = ?", (ubicacion, ws_id))
        log(f"[WS {ws_id}] Registro ACEPTADO ({ubicacion}). Monitor autenticado")
        send(conn, "OK")
 
        # ---- 2) Estado periódico del Monitor ----
        while True:
            msg = recibir(conn).split(SEP)
            if msg[0] == FIN:
                print(f"[MONITOR] {ws_id} se despide (FIN)")
                break
            if msg[0] == "STATUS" and len(msg) >= 3 and msg[1] == ws_id:
                with lock_est:
                    anterior_estado = ws["monitor"]
                    ws["monitor"] = msg[2]
                if msg[2] == "KO" and anterior_estado != "KO":
                    log(f"[!!! AVERIA !!!] El Monitor de {ws_id} informa de una FUGA")
                    # CAMBIO: si la WS estaba regando, el riego termina y se avisa en ese momento al
                    # operario que recibía el servicio (lo pide la guía). Si después llega el END de la
                    # estación (FUGA, o RECUPERADO si cayó el Engine), tratar_end corrige el registro.
                    cerrar_riego(ws_id, "AVERIA")
                elif msg[2] == "OK" and anterior_estado == "KO":
                    log(f"[AVERIA RESUELTA] El Monitor de {ws_id} informa de que la fuga está resuelta")
                if msg[2] == "OK" and anterior_estado != "OK":
                    # El Engine acaba de (re)conectar. Si esta WS estaba bloqueada, el Engine
                    # habrá perdido esa orden al reiniciarse: se la repetimos (con un pequeño
                    # retraso para dar tiempo a que empiece a escuchar Kafka).
                    with lock_est:
                        reenviar = ws["bloqueada"]
                    if reenviar:
                        threading.Timer(3, enviar_orden, args=(f"BLOCK{SEP}{ws_id}",)).start()
                send(conn, "OK")
            else:
                send(conn, f"KO{SEP}mensaje desconocido")
 
    except (socket.timeout, ConnectionError, OSError, ValueError) as e:
        print(f"[MONITOR] Conexión con {ws_id or addr} terminada: {e}")
    finally:
        conn.close()
        if ws_id is not None:
            with lock_est:
                ws = estaciones.get(ws_id)
                era_actual = ws is not None and ws["conn"] is conn   # ¿no nos ha sustituido otro Monitor?
                if era_actual:
                    ws["conectado"] = False
                    ws["conn"] = None
                    ws["monitor"] = "WAIT"
            if era_actual:
                log(f"[WS {ws_id}] DESCONECTADA")
 
 
def start_servidor():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", PUERTO))
    server.listen()
    log(f"[LISTENING] CENTRAL a la escucha de Monitores en el puerto {PUERTO}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_monitor, args=(conn, addr), daemon=True).start()
 
 
########## PANEL WEB ##########
 
CSS = """
body{font-family:Segoe UI,Arial,sans-serif;background:#10202b;color:#e8eef2;margin:20px}
h1{font-size:22px;margin:0 0 4px} .sub{color:#8fa6b5;margin-bottom:14px}
table{border-collapse:collapse;width:100%;margin-bottom:14px}
th,td{padding:8px 10px;border-bottom:1px solid #2c4252;text-align:left}
th{color:#8fa6b5;font-weight:600}
.disp{background:#2e7d32} .riego{background:#2e7d32;animation:parpadeo 1s infinite}
.fuga{background:#c62828} .fuera{background:#ef6c00} .desc{background:#757575}
@keyframes parpadeo{50%{opacity:.3}}
.resumen span{display:inline-block;padding:4px 10px;border-radius:12px;margin-right:6px;font-size:13px}
button{padding:4px 9px;margin-right:3px;border:0;border-radius:4px;cursor:pointer}
input{width:50px}
pre{background:#0a141b;padding:10px;border-radius:6px;font-size:12px;line-height:1.5;overflow:auto}
"""
 
CLASES = {DISPONIBLE: "disp", REGANDO: "riego", FUGA: "fuga",
          FUERA_SERVICIO: "fuera", DESCONECTADA: "desc"}
 
 
def generar_pagina():
    with lock_est:
        filas = [(ws["id"], ws["ubicacion"], estado_visible(ws), ws["caudal"], ws["volumen"], ws["operario"])
                 for ws in sorted(estaciones.values(), key=lambda w: w["id"])]
    conteo = {estado: 0 for estado in CLASES}
    for fila in filas:
        conteo[fila[2]] += 1
 
    resumen = "".join(f"<span class='{CLASES[e]}'>{n} {e.capitalize()}</span>" for e, n in conteo.items())
 
    cuerpo = ""
    for ws_id, ubicacion, estado, caudal, volumen, operario in filas:
        regando = estado == REGANDO
        texto_estado = "Fuera de Servicio" if estado == FUERA_SERVICIO else estado
        cuerpo += (f"<tr><td>{html.escape(ws_id)}</td><td>{html.escape(ubicacion)}</td>"
                   f"<td class='{CLASES[estado]}'>{texto_estado}</td>"
                   f"<td>{caudal if regando else '-'}</td><td>{volumen if regando else '-'}</td>"
                   f"<td>{html.escape(operario) if regando else '-'}</td>"
                   f"<td><form method='post'><input type='hidden' name='ws' value='{html.escape(ws_id)}'>"
                   f"<input type='number' name='duracion' value='{DURACION_POR_DEFECTO}' min='1'> s "
                   f"<button name='accion' value='start'>Iniciar riego</button>"
                   f"<button name='accion' value='block'>Bloquear</button>"
                   f"<button name='accion' value='activate'>Activar</button></form></td></tr>")
 
    return ("<!DOCTYPE html><html><head><meta charset='utf-8'><meta http-equiv='refresh' content='2'>"
            "<title>WM_Central</title><style>" + CSS + "</style></head><body>"
            "<h1>WaterManagement · Panel de CENTRAL</h1>"
            f"<div class='sub'>{len(filas)} estaciones registradas · Kafka: {html.escape(KAFKA_ADDR)}</div>"
            f"<div class='resumen'>{resumen}</div><br>"
            "<table><tr><th>ID</th><th>Ubicación</th><th>Estado</th><th>Caudal (L/min)</th>"
            "<th>Volumen (L)</th><th>Operario</th><th>Acciones</th></tr>" + cuerpo + "</table>"
            "<b>Órdenes a TODAS las estaciones:</b> "
            "<form method='post' style='display:inline'><input type='hidden' name='ws' value='*'>"
            f"<input type='number' name='duracion' value='{DURACION_POR_DEFECTO}' min='1'> s "
            "<button name='accion' value='start'>Iniciar riego</button>"
            "<button name='accion' value='block'>Bloquear</button>"
            "<button name='accion' value='activate'>Activar</button></form>"
            "<br><br><b>Dar de alta una estación:</b> "                                  # CAMBIO (#3)
            "<form method='post' style='display:inline'>"
            "<input name='ws' placeholder='ID (WS-05)' style='width:110px'> "
            "<input name='ubicacion' placeholder='Ubicación' style='width:160px'> "
            "<button name='accion' value='alta'>Dar de alta</button></form>"
            "<h3>Mensajes de la aplicación</h3><pre>" + html.escape("\n".join(list(mensajes)[-18:])) +
            "</pre></body></html>")
 
 
def dar_de_alta(ws_id, ubicacion):
    # CAMBIO (#3): alta de una nueva estación desde el panel. A partir de ese momento su
    # Monitor puede registrarse. '#', ';' y '|' se usan como separadores: no se admiten.
    ws_id, ubicacion = ws_id.strip(), ubicacion.strip() or "UNKNOWN"
    if not ws_id or ws_id == "*" or any(c in ws_id + ubicacion for c in "#;|"):
        log(f"[ALTA] Datos no válidos: '{ws_id}' / '{ubicacion}'")
        return
    with lock_est:
        if ws_id in estaciones:
            log(f"[ALTA] La estación {ws_id} ya estaba dada de alta")
            return
        estaciones[ws_id] = nueva_estacion(ws_id, ubicacion)
    db_ejecutar("INSERT OR IGNORE INTO estaciones VALUES (?, ?, 'ACTIVA')", (ws_id, ubicacion))
    log(f"[ALTA] Estación {ws_id} ({ubicacion}) dada de alta. DESCONECTADA hasta que conecte")
 
 
def ejecutar_accion(accion, ws_id, duracion, ubicacion=""):
    # Órdenes manuales de CENTRAL sobre una o todas las WS (punto 11 de la mecánica)
    todas = ws_id == "*"
    if accion == "alta":                                            # CAMBIO (#3)
        dar_de_alta(ws_id, ubicacion)
    elif accion == "start":
        with lock_est:
            objetivos = list(estaciones.keys()) if todas else [ws_id]
        for objetivo in objetivos:
            tratar_peticion("CENTRAL", objetivo, duracion)
    elif accion in ("block", "activate"):
        bloquear = accion == "block"
        with lock_est:
            for ws in estaciones.values():
                if todas or ws["id"] == ws_id:
                    ws["bloqueada"] = bloquear
            ids = [ws["id"] for ws in estaciones.values() if todas or ws["id"] == ws_id]
        for i in ids:                                               # CAMBIO: el bloqueo se guarda en la BD
            db_ejecutar("UPDATE estaciones SET estado = ? WHERE id = ?", ("BLOQUEADA" if bloquear else "ACTIVA", i))
        log(f"[CENTRAL] Orden {'BLOQUEAR' if bloquear else 'ACTIVAR'} a {'todas las WS' if todas else ws_id}")
        enviar_orden(f"{'BLOCK' if bloquear else 'ACTIVATE'}{SEP}{'ALL' if todas else ws_id}")
 
 
class PanelHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        pagina = generar_pagina().encode(FORMAT)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(pagina)))
        self.end_headers()
        self.wfile.write(pagina)
 
    def do_POST(self):
        longitud = int(self.headers.get("Content-Length", 0))
        datos = parse_qs(self.rfile.read(longitud).decode(FORMAT))
        accion = datos.get("accion", [""])[0]
        ws_id = datos.get("ws", ["*"])[0]
        try:
            duracion = int(datos.get("duracion", [DURACION_POR_DEFECTO])[0])
        except ValueError:
            duracion = DURACION_POR_DEFECTO
        ejecutar_accion(accion, ws_id, duracion, datos.get("ubicacion", [""])[0])   # CAMBIO (#3)
        self.send_response(303)                  # redirigimos al panel
        self.send_header("Location", "/")
        self.end_headers()
 
    def log_message(self, *args):
        pass                                     # no llenamos la consola con cada petición HTTP
 
 
########## MAIN  ##########
 
print("****** WELCOME TO WM_CENTRAL: SISTEMA CENTRAL DE WATERMANAGEMENT ****")
 
# Parametros: 
# - Puerto para monitores
# - Dirección del broker de Kafka (ip y puerto)
if len(sys.argv) in (4, 5):
    PUERTO = int(sys.argv[1])
    KAFKA_ADDR = f"{sys.argv[2]}:{sys.argv[3]}"
    # Puerto web: 4º argumento, o la variable PORT (la que asigna Railway), o 8080
    # CAMBIO (#21): el 4.º argumento tiene prioridad; si no se indica, PORT (Railway) o 8080
    PUERTO_WEB = int(sys.argv[4]) if len(sys.argv) == 5 else int(os.environ.get("PORT", 8080))
 
    # Inicialización de la base de datos
    iniciar_db()
    cargar_estaciones()
    conectar_kafka()
    crear_topics()
 
    # CAMBIO: dos consumidores (ver hilo_consumidor)
    threading.Thread(target=hilo_consumidor, daemon=True,
                     args=(TOPIC_REQUESTS, f"wm-central-{uuid.uuid4()}", "latest", tratar_peticion_fo,
                           listo_peticiones, "peticiones de los operarios")).start()
    threading.Thread(target=hilo_consumidor, daemon=True,
                     args=(TOPIC_EVENTS, "wm-central-eventos", "earliest", tratar_evento,
                           listo_eventos, "eventos de las estaciones")).start()
    listo_peticiones.wait()     # no seguimos hasta que Kafka esté listo para recibir
    listo_eventos.wait()
    threading.Thread(target=hilo_estado, daemon=True).start()
    threading.Thread(target=hilo_vigilar, daemon=True).start()
    threading.Thread(target=start_servidor, daemon=True).start()
 
    log(f"[WEB] Panel de monitorización en http://0.0.0.0:{PUERTO_WEB}")
    try:
        ThreadingHTTPServer(("0.0.0.0", PUERTO_WEB), PanelHandler).serve_forever()   # el hilo principal sirve el panel
    except KeyboardInterrupt:
        print("CENTRAL detenida")
    
# Parametros mal introducidos
else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: <PuertoSockets> <KafkaIP> <KafkaPuerto> [PuertoWeb]")