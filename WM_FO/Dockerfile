# ==================================================================
# WM_FO: imagen de la aplicación del operario (se ejecuta en el laboratorio)
#   docker run ... wm_fo <KafkaIP> <KafkaPuerto> <ID_Operario> [fichero_servicios]
# ==================================================================

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código y fichero de servicios de ejemplo (para el modo automático)
COPY WM_FO.py servicios.txt ./

ENV PYTHONUNBUFFERED=1

# Los argumentos de docker run se añaden detrás de "python WM_FO.py"
ENTRYPOINT ["python", "WM_FO.py"]
