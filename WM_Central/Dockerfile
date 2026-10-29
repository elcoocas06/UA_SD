# ==================================================================
# WM_Central: imagen de la central (se despliega en Railway)
# Parámetros por variables de entorno (sin tocar el código):
#   PUERTO_SOCKETS  puerto donde escucha a los Monitores (por defecto 7000)
#   KAFKA_HOST      dirección del broker Kafka
#   KAFKA_PORT      puerto del broker Kafka
#   PORT            puerto del panel web (Railway lo usa para el dominio público)
#   WM_DB           ruta de la base de datos SQLite (dentro de un volumen)
# ==================================================================

# Imagen base: Linux mínimo con Python 3.12
FROM python:3.12-slim

WORKDIR /app

# 1) Librerías (se copian antes que el código para aprovechar la caché de Docker:
#    si solo cambia el código, no se vuelve a ejecutar pip install)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Código
COPY WM_Central.py .

# Valores por defecto (se pueden cambiar con -e en docker run o con variables en Railway)
ENV PYTHONUNBUFFERED=1 \
    PUERTO_SOCKETS=7000 \
    KAFKA_HOST=localhost \
    KAFKA_PORT=9092 \
    PORT=8080 \
    WM_DB=/data/wm_central.db

# Carpeta de la base de datos. En Railway se monta un volumen en /data para que la
# BD sobreviva a los reinicios y redespliegues
RUN mkdir -p /data

# Puertos que usa el contenedor: sockets de los Monitores y panel web
EXPOSE 7000 8080

# "sh -c" para que se sustituyan las variables; "exec" para que Python reciba
# directamente las señales de parada (Ctrl+C / docker stop)
CMD ["sh", "-c", "exec python WM_Central.py \"$PUERTO_SOCKETS\" \"$KAFKA_HOST\" \"$KAFKA_PORT\""]
