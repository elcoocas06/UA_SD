import socket 
import threading
import sys
import time

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
SEP = "#"
PERIODO = 1             # Tiempo de comprobacion de estado de salud de WS
TIMEOUT_ENGINE = 2
TIMEOUT_CENTRAL = 5 
ESPERA_ENGINE = 8       # 8 s tras autenticarse sin que el Engine conecte -> se dea por caído (KO)

# Posibles estados del Engine (los guardamos como texto)
WAIT = "WAIT"
OK = "OK"
KO = "KO"

# Estado compartido entre hilos
estado_engine = WAIT
lock_estado = threading.Lock()
engine_conectado = False
lock_engine = threading.Lock()


# CENTRAL ya ha validado el ID de esta WS
autenticado = threading.Event()
central_ok = False          # True mientras hay conexion con CENTRAL


########## FUNCIONES DE MENSAJERIA ##########

def send(sock, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    sock.sendall(send_length)
    sock.sendall(message)

def recibir_exacto(sock, n):
    # recv puede devolver menos bytes de los pedidos: leemos hasta tener n
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


########## ESTADO DEL ENGINE ##########

def obtener_estado():
    with lock_estado:
        return estado_engine


def cambiar_estado(nuevo):
    global estado_engine
    with lock_estado:
        anterior = estado_engine
        estado_engine = nuevo

    # Casos posibles al cambio de estado
    if anterior != nuevo:
        if nuevo == KO and anterior == WAIT:
            print(f"[!!! AVERIA !!!] El Engine de {WS_ID} no se ha conectado (WAIT -> KO). Se notifica a CENTRAL")
        elif nuevo == KO:
            print(f"[!!! AVERIA !!!] Fuga detectada en {WS_ID} ({anterior} -> KO). Se notifica a CENTRAL")
        elif nuevo == OK and anterior == KO:
            print(f"[AVERIA RESUELTA] {WS_ID} vuelve a estar operativa (KO -> OK). Se notifica a CENTRAL")
        else:
            print(f"[ESTADO] Engine de {WS_ID}: {anterior} -> {nuevo}")

########## LADO SERVIDOR: ENGINE ##########

def handle_engine(conn, addr):
    global engine_conectado
    print(f"[NUEVA CONEXION] Engine {addr} connected.")
    conn.settimeout(TIMEOUT_ENGINE)

    try:
        # No empezamos a monitorizar hasta que CENTRAL haya validado el ID
        autenticado.wait()

        connected = True
        while connected:
            # Enviamos el ID de la estacion a Engine y comprobamos su estado
            send(conn, f"HEALTH{SEP}{WS_ID}{SEP}{'OK' if central_ok else 'KO'}")
            respuesta = recibir(conn).split(SEP)

            # Si es OK -> Todo bien / Si es KO -> averia
            if respuesta[0] == "HEALTH_ANS" and len(respuesta) > 1 and respuesta[1] == OK:
                cambiar_estado(OK)
            else:
                cambiar_estado(KO)
            time.sleep(PERIODO)

    except (socket.timeout, ConnectionError, OSError, ValueError) as e:
        print(f"[ENGINE] Sin respuesta o conexión interrumpida: {e}")
    finally:
        conn.close()
        with lock_engine:
            engine_conectado = False
        # Si el Engine ya habia estado conectado y se cae -> averia
        if obtener_estado() != WAIT:
            cambiar_estado(KO)
        print("[ENGINE] Conexión cerrada. Esperando a que el Engine vuelva a conectar")


# Gestionamos la conexion de un unico Engine 
def start_servidor():
    global engine_conectado
    # No se abre el puerto del Engine hasta que CENTRAL valida la WS. Así el Engine solo 
    # ve "Esperando al Monitor" y no se conecta a un Monitor que todavía 
    # no le puede atender 
    autenticado.wait()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1) # reutilizar el puerto al reiniciar
    server.bind(("0.0.0.0", PUERTO))
    server.listen()
    print(f"[LISTENING] Monitor a la escucha del Engine en el puerto {PUERTO}")

    while True:
        conn, addr = server.accept()

        # Cada monitor solo puede tener un Engine
        with lock_engine:
            ocupado = engine_conectado
            if not ocupado:
                engine_conectado = True
        if not ocupado:
            thread = threading.Thread(target=handle_engine, args=(conn, addr), daemon=True)
            thread.start()
        else:
            print("OOppsss... Ya hay un Engine conectado a este Monitor. Conexión rechazada")
            conn.close()


########## LADO CLIENTE: CENTRAL ##########

def central_conectada():
    global central_ok
    espera = 1
    while True:   
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(TIMEOUT_CENTRAL) 
        try: 
            print(f"Conectando con CENTRAL en [{ADDR_CENTRAL}] ...")
            client.connect(ADDR_CENTRAL)
            print(f"Establecida conexión en [{ADDR_CENTRAL}]")

            # 1) Autenticación / registro
            send(client, f"REGISTER{SEP}{WS_ID}{SEP}{UBICACION}")
            respuesta = recibir(client).split(SEP)
            if respuesta[0] != "OK":
                motivo = respuesta[1] if len(respuesta) > 1 else ""
                texto = respuesta[2] if len(respuesta) > 2 else motivo
                client.close()
                if motivo == "DUPLICADA":
                    # Puede ser nuestra conexión anterior, que CENTRAL aún no ha dado por cerrada
                    print(f"[REGISTRO] CENTRAL rechaza {WS_ID} por ahora ({texto}). Reintento en {espera}s")
                    time.sleep(espera)
                    espera = min(espera * 2, 10)
                    continue
                print(f"[REGISTRO DENEGADO] CENTRAL rechaza el registro de {WS_ID}: {texto}")
                return False
            print(f"WS {WS_ID} autenticada y registrada en CENTRAL")

            # Cuando la central valida que el WS está operativo, 
            # podemos empezar a monitorizar el Engine
            autenticado.set()
            central_ok = True
            espera = 1
            inicio_espera = time.time()     # desde cuando se espera al Engine

            # 2) Enviar el estado de la WS cada segundo (a central) 
            while True:
                estado = obtener_estado()
                if estado == WAIT and time.time() - inicio_espera > ESPERA_ENGINE:
                    cambiar_estado(KO) # Monitor OK + Engine caído -> averiada
                    estado = KO
                
                send(client, f"STATUS{SEP}{WS_ID}{SEP}{estado}")
                recibir(client)     # respuesta de CENTRAL (si no llega en 5s -> timeout)
                time.sleep(PERIODO)

        # Ctrl+C en el teclado: nos despedimos de CENTRAL
        # Sirve para apagar el monitor de forma manual
        except KeyboardInterrupt:
            print("CENTRAL caída")
            try:
                print("Envio a CENTRAL: ", FIN)
                send(client, FIN)
            except OSError:
                pass
            client.close()
            return True

        # Cualquier fallo de red: cerramos, esperamos y volvemos a intentarlo
        except (socket.timeout, ConnectionError, OSError, ValueError) as e:
            central_ok = False
            print(f"Conexión con CENTRAL perdida ({e}). Reintento en {espera}s")
            client.close()
            time.sleep(espera)
            espera = min(espera * 2, 10)
        

######################### MAIN ##########################

print("****** WELCOME TO WM_WS_M: MONITOR DE LA WATERING STATION ****")

# Parametros: 
# - Puerto para el engine
# - Ip y puerto de la central
# - ID de la WS
# - Ubicación de la estación (Opcional)?
if len(sys.argv) in (5, 6):
    PUERTO = int(sys.argv[1])
    ADDR_CENTRAL = (sys.argv[2], int(sys.argv[3]))
    WS_ID = sys.argv[4]
    UBICACION = sys.argv[5] if len(sys.argv) == 6 else "UNKNOWN"

    # Hilo servidor (Engine) en segundo plano; el hilo principal hace de cliente de CENTRAL
    thread = threading.Thread(target=start_servidor, daemon=True)
    thread.start()

    # Si CENTRAL RECHAZA LA CONEXION acaba el programa
    try:
        if not central_conectada():
            sys.exit(2)
    except KeyboardInterrupt:
        print("\n[FIN] Monitor detenido")

# Parametros mal introducidos
else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: <Puerto> <CentralIP> <CentralPuerto> <ID_WS> [Ubicacion]")