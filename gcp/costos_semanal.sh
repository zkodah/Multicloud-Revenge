#!/bin/bash

# Activar el entorno virtual
. /home/mluque/myenv/bin/activate

# Instalar las librerías requeridas (si aún no están instaladas)
#pip install -r /home/mluque/Proyecto/gcp/requirements.txt

# Ejecutar el script Python
python3 /home/mluque/Proyecto/gcp/demo.py

# Desactivar el entorno virtual
deactivate
