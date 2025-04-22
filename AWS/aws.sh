#!/bin/bash

# Activar el entorno virtual
. /home/mluque/Proyecto/AWS/venv/bin/activate

# Instalar las librerías requeridas (si aún no están instaladas)
#pip install -r /home/mluque/Proyecto/AWS/requirements.txt

# Ejecutar el script Python
python3 /home/mluque/Proyecto/AWS/main.py

# Desactivar el entorno virtual
deactivate