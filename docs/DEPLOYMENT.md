# Guía de Despliegue — WaterManagement (Release 1)

Este documento detalla lo indispensable para desplegar el sistema en local
y en la nube (Railway/AWS) para la corrección.

---

## 1. Estructura del repositorio

| Ruta         | Contenido                                          |
|--------------|----------------------------------------------------|
| WM_Central/  | Código de la Central y base de datos SQLite.       |
| WM_WS/       | Código del Monitor y del Engine de la estación.    |
| WM_FO/       | Código de la aplicación del Field Operator.        |
| prompts/     | Registro de los prompts de IA utilizados.          |
| docs/        | Documentación (este archivo, protocolos, memoria). |
| README.md    | Instrucciones básicas de arranque.                 |
| Nombres.txt  | Nombre y apellidos de los alumnos.                 |

---

## 2. Configuración de variables de entorno

Ningún módulo debe tener IPs o puertos fijos en el código.

| Variable       | Módulo | Descripción                       | Ejemplo                     |
|----------------|--------|-----------------------------------|-----------------------------|
| CENTRAL_HOST   | Todos  | IP o dominio de la Central.       | wm-central.up.railway.app   |
| CENTRAL_PORT   | Todos  | Puerto de la Central.             | 8080                        |
| WS_ID          | WS     | Identificador de la estación.     | WS-01                       |
| WS_LOCATION    | WS     | Ubicación (parque o jardín).      | PARK_A                      |
| WS_PORT        | WS     | Puerto del servidor de la WS.     | 9090                        |
| ENGINE_PORT    | WS     | Puerto del Engine local.          | 9091                        |
| FO_ID          | FO     | Identificador del operario.       | FO-03                       |
| KAFKA_BROKER   | Todos  | URL del broker Kafka.             | kafka:9092                  |
| SQLITE_PATH    | Central| Ruta del archivo de base de datos.| /data/wm.db                 |

Ejemplo de archivo .env:

CENTRAL_HOST=wm-central.up.railway.app
CENTRAL_PORT=8080
WS_ID=WS-01
WS_LOCATION=PARK_A
WS_PORT=9090
ENGINE_PORT=9091

---

## 3. Ejecución en local (desarrollo)

Abre cuatro terminales distintas (una por módulo).

### 3.1 Central
cd WM_Central
pip install -r requirements.txt
python main.py

### 3.2 Watering Station (WS)
cd WM_WS
pip install -r requirements.txt
python main.py

### 3.3 Field Operator (FO)
cd WM_FO
pip install -r requirements.txt
python main.py

### 3.4 Kafka (opcional en local)
docker run -d --name kafka -p 9092:9092 apache/kafka:latest

---

## 4. Despliegue en Cloud (Railway / AWS)

Obligatorio para la corrección: WM_Central, Kafka y SQLite en la nube.

1. En Railway, crea un proyecto y conecta tu GitHub.
2. Selecciona la carpeta WM_Central/ como Root Directory.
3. Añade un volumen o servicio para persistir SQLite.
4. Configura las variables (CENTRAL_PORT, KAFKA_BROKER, SQLITE_PATH).
5. Despliega y anota la URL pública (ej. wm-central.up.railway.app).
6. Añade un servicio Kafka y anota su URL interna.

Verificación rápida en el navegador o terminal:
curl https://tu-url-de-railway/health

---

## 5. Ejecución en el laboratorio (día de la corrección)

La Central, Kafka y SQLite ya están en Railway.
En el laboratorio solo se arrancan WM_FO y WM_WS.

1. Clona el repositorio y entra en la carpeta:
   git clone https://github.com/tu-usuario/UA_SD.git
   cd UA_SD

2. Crea el archivo .env en WM_WS y WM_FO apuntando a Railway.
3. Arranca la WS:
   cd WM_WS
   python main.py

4. Arranca el FO (en otra terminal):
   cd WM_FO
   python main.py

5. Para varias instancias, cambia las variables antes de arrancar:
   WS_ID=WS-02 WS_PORT=9092 python main.py

---

## 6. Uso de Docker (opcional pero recomendado)

Desde la raíz del repositorio:

docker build -t wm-central -f WM_Central/Dockerfile .
docker build -t wm-ws -f WM_WS/Dockerfile .
docker build -t wm-fo -f WM_FO/Dockerfile .

docker run --env-file .env wm-central
docker run --env-file .env wm-ws
docker run --env-file .env wm-fo

---

## 7. Pruebas de resiliencia antes de la corrección

| Escenario        | Acción                  | Resultado esperado                                    |
|------------------|-------------------------|-------------------------------------------------------|
| Caída de Monitor | Cerrar terminal de WS   | Central marca la WS como Disconnected.                |
| Caída de Engine  | Cerrar proceso Engine   | Monitor notifica avería; Central marca Leak.          |
| Caída de Central | Parar servicio Railway  | WS y FO muestran error; riego en curso termina.       |
| Caída de FO      | Cerrar terminal del FO  | El riego continúa; al recuperar, ve el resultado.     |
| Múltiples WS     | Arrancar dos WS         | Ambas aparecen en el panel de Central.                |
| Múltiples FO     | Arrancar dos FO         | Ambos pueden solicitar riego simultáneamente.         |
| Recuperación     | Restaurar un módulo     | El sistema vuelve a la normalidad sin perder datos.   |

---

## 8. Solución de problemas comunes

| Problema                          | Causa probable               | Solución                                  |
|-----------------------------------|------------------------------|-------------------------------------------|
| "Imposible conectar con Central"  | URL o puerto incorrecto.     | Revisa CENTRAL_HOST y CENTRAL_PORT.       |
| La WS no aparece en Central       | Registro o auth fallidos.    | Revisa logs de WS y token de Central.     |
| LRC incorrecto                    | Codificación o caracteres.   | Envía bytes y calcula XOR correctamente.  |
| SQLite no guarda datos            | Ruta no escribible.          | Revisa SQLITE_PATH y permisos.            |
| Kafka no conecta                  | Broker mal configurado.      | Verifica KAFKA_BROKER y servicio activo.  |

---

## 9. Checklist antes de la corrección

[ ] Central desplegada y funcionando en Railway.
[ ] Kafka desplegado y accesible.
[ ] SQLite persiste los datos correctamente.
[ ] Repositorio GitHub con estructura exigida y acceso al profesor.
[ ] Carpeta prompts/ con el registro real de prompts.
[ ] README.md y Nombres.txt actualizados.
[ ] Caídas de Monitor, Engine, FO y Central probadas.
[ ] Varias instancias de WS y FO sin fallos.
[ ] Todo se ejecuta desde terminal (sin IDE).
