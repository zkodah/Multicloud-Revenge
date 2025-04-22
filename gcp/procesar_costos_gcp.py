import subprocess

# Ejecutar el script que inserta datos en la base de datos
def ejecutar_insercion():
    try:
        #subprocess.run(['/home/mluque/Proyecto/myenv/bin/python', 'cost_gcp_diario.py'], check=True)
        #subprocess.run(['/home/mluque/myenv/bin/python', '/home/mluque/Proyecto/gcp/cost_gcp_diario.py'], check=True)
        subprocess.run(['python3', '/home/mluque/Proyecto/gcp/cost_gcp_diario.py'], check=True)


        print("✅ Datos insertados correctamente.")
    except subprocess.CalledProcessError:
        print("❌ Error al insertar los datos en la base de datos.")

# Ejecutar el script que envía el correo
def ejecutar_envio_correo():
    try:
        subprocess.run(['python3', '/home/mluque/Proyecto/gcp/cost_gcp_notify_daily.py'], check=True)
        #subprocess.run(['python3', 'cost_gcp_diario.py'], check=True)
        print("✅ Correo enviado correctamente.")
    except subprocess.CalledProcessError:
        print("❌ Error al enviar el correo.")

# Llamar a las funciones
if __name__ == "__main__":
    ejecutar_insercion()
    ejecutar_envio_correo()
