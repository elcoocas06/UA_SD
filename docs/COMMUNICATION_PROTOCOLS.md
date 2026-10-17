# Protocolo de Comunicación — WaterManagement (Release 1)

Define las tramas, el cálculo del LRC, el catálogo de mensajes, los flujos
entre módulos, los estados de las estaciones y la integración con Kafka.

Regla de oro: ninguna transacción puede perderse. Toda orden debe poder
confirmarse (ACK) o rechazarse (NACK) y, si el receptor cae, el emisor debe
poder reenviar o reconciliar el estado al recuperarse.

---

## 1. Formato de trama

Todas las comunicaciones por socket usan tramas con la forma:

<STX> <DATA> <ETX> <LRC>

| Campo | Tamaño  | Descripción                    |
|-------|---------|--------------------------------|
| STX   | 1 byte  | Start of Text. Valor 0x02.     |
| DATA  | N bytes | Carga útil en ASCII.           |
| ETX   | 1 byte  | End of Text. Valor 0x03.       |
| LRC   | 1 byte  | Longitudinal Redundancy Check. |

- DATA no puede contener 0x02 ni 0x03. Si el payload los necesitara, se
  codifica en Base64 o se escapa.
- La trama se envía como bytes, no como cadena con "\x02" literal.
- Tamaño máximo recomendado de DATA: 1024 bytes. Si se supera, se fragmenta
  en varias tramas con el campo SEQ.

### 1.1 Ejemplo

Orden de inicio de riego al Engine WS-01:

DATA = "CMD|IRRIGATE_START|WS-01|FO-03|25.0|120"
Trama = 0x02 + "CMD|IRRIGATE_START|WS-01|FO-03|25.0|120" + 0x03 + LRC

---

## 2. Cálculo del LRC

### 2.1 Algoritmo

El LRC se calcula como XOR de todos los bytes de DATA (sin incluir STX, ETX
ni el propio LRC):

def compute_lrc(data: bytes) -> int:
    lrc = 0
    for b in data:
        lrc ^= b
    return lrc & 0xFF

### 2.2 Verificación

def verify_frame(frame: bytes) -> bool:
    if len(frame) < 4:
        return False
    if frame[0] != 0x02 or frame[-2] != 0x03:
        return False
    data = frame[1:-2]
    return compute_lrc(data) == frame[-1]

- Si el LRC no coincide: descartar la trama, registrar el error y, si aplica,
  responder NACK|LRC_ERROR.
- No se debe reintentar indefinidamente una trama corrupta: máximo 3
  reintentos y luego notificar al módulo superior.

### 2.3 Escape de bytes reservados

Si DATA contiene 0x02 o 0x03, se aplica byte stuffing:

| Byte original | Secuencia escapada |
|---------------|--------------------|
| 0x02          | 0x10 0x02          |
| 0x03          | 0x10 0x03          |
| 0x10          | 0x10 0x10          |

El receptor desescapa antes de validar el LRC. El LRC se calcula sobre los
bytes ya escapados (es decir, sobre lo que realmente viaja en DATA).

---

## 3. Catálogo de mensajes

Formato general de DATA:

<TIPO>|<ACCION>|<CAMPO1>|<CAMPO2>|...|<CAMPO_N>

- <TIPO>: REG, AUTH, CMD, ACK, NACK, EVT, HEALTH, DATA.
- Los campos son str; los numéricos usan . como separador decimal.
- Los campos vacíos se representan como cadena vacía entre |.

### 3.1 Campos comunes

| Campo | Descripción                                         |
|-------|-----------------------------------------------------|
| SEQ   | Secuencia monotónica por emisor.                    |
| TS    | Timestamp ISO-8601 UTC, p. ej. 2026-10-17T12:35Z.   |
| SRC   | ID del emisor: CENTRAL, WS-01, FO-03, MON-01, etc.  |
| DST   | ID del destinatario o BROADCAST.                    |
| ID    | Identificador de correlación de la transacción.     |

### 3.2 Registro y autenticación

| Mensaje      | Dirección    | DATA                                   |
|--------------|--------------|----------------------------------------|
| REG|REQUEST  | WS → Central | REG|REQUEST|SEQ|TS|WS-01|PARK_A|secret |
| REG|ACCEPT   | Central → WS | REG|ACCEPT|SEQ|TS|WS-01|token          |
| REG|DENY     | Central → WS | REG|DENY|SEQ|TS|WS-01|NOT_REGISTERED   |
| AUTH|REQUEST | WS → Central | AUTH|REQUEST|SEQ|TS|WS-01|token        |
| AUTH|OK      | Central → WS | AUTH|OK|SEQ|TS|WS-01                   |
| AUTH|KO      | Central → WS | AUTH|KO|SEQ|TS|WS-01|INVALID_TOKEN     |

- PARK_A / GARDEN_B: ubicación de la estación.
- secret: credencial de registro (puede ser un hash).
- token: token de sesión emitido por Central tras el registro.

### 3.3 Órdenes de Central a estaciones

| Mensaje            | Dirección    | DATA                                           |
|--------------------|--------------|------------------------------------------------|
| CMD|IRRIGATE_START | Central → WS | CMD|IRRIGATE_START|SEQ|TS|WS-01|FO-03|25.0|120 |
| CMD|IRRIGATE_STOP  | Central → WS | CMD|IRRIGATE_STOP|SEQ|TS|WS-01|FO-03           |
| CMD|BLOCK          | Central → WS | CMD|BLOCK|SEQ|TS|WS-01                         |
| CMD|ACTIVATE       | Central → WS | CMD|ACTIVATE|SEQ|TS|WS-01                      |
| CMD|STATUS_REQUEST | Central → WS | CMD|STATUS_REQUEST|SEQ|TS|WS-01                |

- 25.0: caudal objetivo en L/min.
- 120: volumen objetivo en litros.
- BLOCK finaliza cualquier riego en curso y pasa a Out of Service.
- ACTIVATE devuelve la estación a Available si no hay avería.

### 3.4 Solicitud de riego desde Field Operator

| Mensaje              | Dirección         | DATA                                             |
|----------------------|-------------------|--------------------------------------------------|
| CMD|IRRIGATE_REQUEST | FO → Central      | CMD|IRRIGATE_REQUEST|SEQ|TS|FO-03|WS-01|25.0|120 |
| ACK|IRRIGATE_ACCEPT  | Central → FO      | ACK|IRRIGATE_ACCEPT|SEQ|TS|FO-03|WS-01|TX-123    |
| NACK|IRRIGATE_DENY   | Central → FO      | NACK|IRRIGATE_DENY|SEQ|TS|FO-03|WS-01|WS_BUSY    |
| EVT|IRRIGATE_DONE    | WS → Central → FO | EVT|IRRIGATE_DONE|SEQ|TS|WS-01|FO-03|120.0|25.0  |

- TX-123: ID de transacción generado por Central.
- Motivos de denegación: WS_BUSY, WS_LEAK, WS_OUT_OF_SERVICE,
  WS_DISCONNECTED, UNAUTHORIZED.

### 3.5 Health check Monitor ↔ Engine

| Mensaje     | Dirección        | DATA                                    |
|-------------|------------------|-----------------------------------------|
| HEALTH|PING | Monitor → Engine | HEALTH|PING|SEQ|TS|MON-01               |
| HEALTH|PONG | Engine → Monitor | HEALTH|PONG|SEQ|TS|ENG-01|OK            |
| HEALTH|KO   | Engine → Monitor | HEALTH|PONG|SEQ|TS|ENG-01|KO            |

- El Monitor envía PING cada 1 segundo.
- Si no hay PONG en 3 s, el Monitor marca el Engine como caído.
- Si el PONG llega con KO, el Monitor notifica fuga a Central.

### 3.6 Notificaciones de estado y avería

| Mensaje          | Dirección         | DATA                                    |
|------------------|-------------------|-----------------------------------------|
| EVT|STATE        | WS → Central      | EVT|STATE|SEQ|TS|WS-01|Available        |
| EVT|LEAK         | Monitor → Central | EVT|LEAK|SEQ|TS|WS-01|MON-01            |
| EVT|LEAK_FIXED   | Monitor → Central | EVT|LEAK_FIXED|SEQ|TS|WS-01             |
| EVT|ENGINE_DOWN  | Monitor → Central | EVT|ENGINE_DOWN|SEQ|TS|WS-01|ENG-01     |
| EVT|DISCONNECTED | Central (interno) | EVT|DISCONNECTED|SEQ|TS|WS-01           |

- Available, Watering, Leak, Out of Service, Disconnected.
- Central actualiza su panel al recibir estos eventos.

### 3.7 ACK / NACK genéricos

| Mensaje | DATA                                               |
|---------|----------------------------------------------------|
| ACK     | ACK|<ACCION>|SEQ|TS|<SRC>|<DST>|<TX_ID>            |
| NACK    | NACK|<ACCION>|SEQ|TS|<SRC>|<DST>|<TX_ID>|<MOTIVO>  |

- Toda orden CMD debe responderse con ACK o NACK.
- Si el emisor no recibe respuesta en 5 s, reintenta hasta 3 veces;
  si sigue sin respuesta, marca el destino como no disponible y notifica.

---

## 4. Flujos de comunicación

### 4.1 Arranque de una estación

WS_Monitor → Central   : REG|REQUEST
Central    → WS_Monitor : REG|ACCEPT (token)
WS_Monitor → Central   : AUTH|REQUEST
Central    → WS_Monitor : AUTH|OK
WS_Monitor → Central   : EVT|STATE Available

Si REG|DENY o AUTH|KO, el Monitor muestra error y no permite riegos.

### 4.2 Solicitud de riego desde FO

FO      → Central : CMD|IRRIGATE_REQUEST
Central → WS      : CMD|IRRIGATE_START
WS      → Central : ACK|IRRIGATE_START
Central → FO      : ACK|IRRIGATE_ACCEPT
WS      → Central : EVT|STATE Watering
WS      → Central : EVT|IRRIGATE_DONE
Central → FO      : EVT|IRRIGATE_DONE

Si la WS no está Available, Central responde NACK|IRRIGATE_DENY.

### 4.3 Fuga detectada

Engine  → Monitor : HEALTH|PONG ... KO
Monitor → Central : EVT|LEAK
Central → WS      : CMD|IRRIGATE_STOP (si había riego)
Central → FO      : EVT|LEAK (si había servicio)
Monitor → Central : EVT|LEAK_FIXED (al resolverse)
Central → WS      : CMD|ACTIVATE

### 4.4 Caída del Monitor

- Central deja de recibir EVT|STATE / heartbeats del Monitor.
- Tras 3 s sin mensajes de vida, Central marca la WS como Disconnected.
- Si había riego, finaliza salvo que el Engine también haya caído.
- La WS no admite nuevos riegos y muestra Averiada.

### 4.5 Caída del Engine

- El Monitor detecta ausencia de PONG en 3 s.
- Monitor → Central: EVT|ENGINE_DOWN.
- Central marca la WS como averiada (Leak/rojo según guía) y avisa al FO.
- Al recuperarse el Engine, el Monitor reanuda PING y envía el estado
  final del riego que quedó pendiente.

### 4.6 Caída de Central

- WS y FO muestran "Imposible conectar con WM_Central".
- Las WS siguen prestando el servicio en curso hasta finalizarlo.
- No se admiten nuevas peticiones de riego.
- Al restaurar Central, WS y FO se reconectan y reenvían estado.

### 4.7 Caída de un Field Operator

- El riego continúa.
- Al recuperarse el FO, consulta el resultado de su servicio
  (EVT|IRRIGATE_DONE o estado actual de la WS).

---

## 5. Estados de la estación

| Estado         | Color             | Condición                               |
|----------------|-------------------|-----------------------------------------|
| Available      | VERDE             | Monitor_OK y Engine_OK, sin riego.      |
| Watering       | VERDE parpadeante | Riego en curso; caudal, volumen y FO.   |
| Leak           | ROJO              | Monitor_OK y Engine_KO, o fuga.         |
| Out of Service | NARANJA           | Bloqueada por Central.                  |
| Disconnected   | GRIS              | Central no recibe mensajes de vida.     |

Tabla de decisión Monitor/Engine:

| Monitor | Engine | Estado       |
|---------|--------|--------------|
| OK      | OK     | Available    |
| OK      | KO     | Leak         |
| KO      | OK     | Disconnected |
| KO      | KO     | Disconnected |

---

## 6. Integración con Kafka

Kafka se usa para eventos de negocio y desacoplar Central del panel y
de los consumidores externos. Los sockets siguen siendo el canal de control
entre módulos.

### 6.1 Topics

| Topic               | Productor    | Consumidor     | Contenido                       |
|---------------------|--------------|----------------|---------------------------------|
| wm.station.events   | WS / Monitor | Central, panel | Estados, fugas, fin de riego.   |
| wm.operator.events  | FO           | Central        | Solicitudes y cancelaciones.    |
| wm.central.commands | Central      | WS             | Órdenes (respaldo del socket).  |
| wm.audit            | Todos        | Auditoría      | Log inmutable de transacciones. |

### 6.2 Formato del mensaje Kafka

{
  "tx_id": "TX-123",
  "seq": 42,
  "ts": "2026-10-17T12:35:00Z",
  "src": "WS-01",
  "dst": "CENTRAL",
  "type": "EVT",
  "action": "IRRIGATE_DONE",
  "payload": {
    "operator": "FO-03",
    "volume_l": 120.0,
    "flow_l_min": 25.0
  }
}

- tx_id es la clave de partición para preservar el orden por transacción.
- Central consume wm.station.events y actualiza SQLite y el panel.
- Si Kafka no está disponible, Central sigue operando por socket y
  encola los eventos para publicarlos al recuperarse.

### 6.3 Garantías

- At-least-once en publicación; los consumidores deduplican por tx_id.
- wm.audit se considera la fuente de verdad para reconciliación.
- No se usa Kafka para órdenes críticas si el socket está disponible;
  Kafka actúa como respaldo y bus de eventos.

---

## 7. Persistencia y transacciones

- Central guarda en SQLite:
  - estaciones registradas (ID, ubicación, token),
  - estado actual,
  - agua suministrada acumulada,
  - historial de riegos (tx_id, FO, WS, volumen, caudal, inicio, fin),
  - eventos de avería y recuperación.
- Toda orden se escribe primero en SQLite (o en cola persistente) antes de
  confirmarse al emisor, para no perder transacciones.
- Al reiniciar Central, se reconcilian estados con las WS conectadas.

---

## 8. Pruebas del protocolo

| Prueba | Descripción            | Criterio de éxito              |
|--------|------------------------|--------------------------------|
| P1     | Trama válida           | verify_frame devuelve True.    |
| P2     | LRC incorrecto         | Se descarta y se registra.     |
| P3     | Byte reservado en DATA | El escape/desescape funciona.  |
| P4     | Registro aceptado      | WS recibe token y pasa a Avail.|
| P5     | Registro denegado      | WS muestra error.              |
| P6     | Riego aceptado         | FO recibe ACK y WS a Watering. |
| P7     | Riego denegado         | FO recibe NACK con motivo.     |
| P8     | Fuga                   | Central marca Leak y avisa FO. |
| P9     | Caída de Monitor       | Central marca Disconnected.    |
| P10    | Caída de Engine        | Monitor notifica, Central avisa|
| P11    | Caída de Central       | WS y FO muestran error.        |
| P12    | Recuperación           | Vuelve al estado sin perder Tx.|
| P13    | Múltiples instancias   | Varias WS y FO a la vez.       |
| P14    | Duplicados             | El receptor deduplica por SEQ. |

---

## 9. Buenas prácticas de implementación

- Usar struct.pack/unpack o bytes explícitos para STX/ETX/LRC.
- No concatenar tramas sin delimitador: el receptor debe leer hasta ETX.
- Timeouts explícitos en todas las lecturas de socket.
- Hilos o asyncio para servidores concurrentes; nunca bloquear el accept.
- Loguear SEQ, TS, SRC, DST y tx_id en cada mensaje.
- Validar siempre el LRC antes de procesar DATA.
- Mantener un dict de transacciones pendientes por tx_id para ACK/NACK.
- En caso de duda, no confirmar una transacción hasta persistirla.
