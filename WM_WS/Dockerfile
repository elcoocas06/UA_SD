# ==================================================================
# WM_WS: imagen de la estación de riego (se ejecuta en el laboratorio)
# Una sola imagen con los dos programas; se elige cuál lanzar al ejecutarla:
#   docker run ... wm_ws WM_WS_M.py <PuertoEngine> <CentralIP> <CentralPuerto> <ID_WS> [Ubicacion]
#   docker run ... wm_ws WM_WS_E.py <KafkaIP> <KafkaPuerto> <MonitorIP> <MonitorPuerto>
# ==================================================================

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY WM_WS_M.py WM_WS_E.py ./

# WM_DATOS: carpeta del fichero de riego del Engine (riego_<ID>.txt). Se monta un
# volumen en ella para no perder lo regado si el contenedor se borra
ENV PYTHONUNBUFFERED=1 \
    WM_DATOS=/app/datos
RUN mkdir -p /app/datos

# El contenedor ejecuta "python <lo que se indique en docker run>".
# Sin argumentos lanza el Monitor, que muestra la ayuda de uso
ENTRYPOINT ["python"]
CMD ["WM_WS_M.py"]
