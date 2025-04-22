#!/bin/bash

# Activar el entorno virtual
. /home/mluque/Proyecto/gcp/venv/bin/activate

# Instalar las librerías requeridas (si aún no están instaladas)
#pip install -r /home/mluque/Proyecto/gcp/requirements.txt

# Ejecutar el script Python
python3 /home/mluque/Proyecto/gcp/procesar_costos_gcp.py

# Desactivar el entorno virtual
deactivate
