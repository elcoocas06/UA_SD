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
        if nuevo == KO:
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
            send(conn, f"HEALTH{SEP}{WS_ID}")
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
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
                print(f"CENTRAL rechaza el registro de {WS_ID}: {respuesta[1:]}")
                client.close()
                return False
            print(f"WS {WS_ID} autenticada y registrada en CENTRAL")

            # Cuando la central valida que el WS está operativo, 
            # podemos empezar a monitorizar el Engine
            autenticado.set()
            espera = 1

            # 2) Enviar el estado de la WS cada segundo (a central) 
            while True:
                estado = obtener_estado()
                print("Envio a CENTRAL: ", f"STATUS{SEP}{WS_ID}{SEP}{estado}")
                send(client, f"STATUS{SEP}{WS_ID}{SEP}{estado}")
                print("Recibo de CENTRAL: ", recibir(client))
                time.sleep(PERIODO)

        # Ctrl+C en el teclado: nos despedimos de CENTRAL
        # Sirve para apagar el monitor de forma manual
        except KeyboardInterrupt:
            print("SE ACABO LO QUE SE DABA")
            try:
                print("Envio a CENTRAL: ", FIN)
                send(client, FIN)
            except OSError:
                pass
            client.close()
            return True

        # Cualquier fallo de red: cerramos, esperamos y volvemos a intentarlo
        except (socket.timeout, ConnectionError, OSError, ValueError) as e:
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
    if not central_conectada():
        sys.exit(2)

# Parametros mal introducidos
else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: <Puerto> <CentralIP> <CentralPuerto> <ID_WS> [Ubicacion]")