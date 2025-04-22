from datetime import datetime  # Importación necesaria para trabajar con fechas
from azure.storage.blob import BlobServiceClient  # Importación necesaria para trabajar con Azure Blob Storage
import os  # Importación necesaria para trabajar con archivos
import psycopg2  # Importación necesaria para trabajar con PostgreSQL
import pandas as pd  # Importación necesaria para trabajar con DataFrames
import Key  # Key.py

# Función para conectar al contenedor de Azure Blob Storage
def conexionCH():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(Key.CONNECTION_STRINGCH)
        container_client = blob_service_client.get_container_client(Key.CONTAINER_NAMECH)

        if not container_client.exists():
            raise Exception("El contenedor no existe")
        print("\n\nContenedor conectado:", Key.CONTAINER_NAMECH)
        return container_client
    except Exception as e:
        print(f"\n🚨 Error al conectar: {str(e)}")
        return None

def conexionPE():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(Key.CONNECTION_STRINGPE)
        container_client = blob_service_client.get_container_client(Key.CONTAINER_NAMEPE)

        if not container_client.exists():
            raise Exception("El contenedor no existe")
        print("\n\nContenedor conectado:", Key.CONTAINER_NAMEPE)
        return container_client
    except Exception as e:
        print(f"\n🚨 Error al conectar: {str(e)}")
        return None

# Función para obtener el último archivo en el contenedor
def obtener_ultimo_archivoCH(container_client):
    try:
        blobs = list(container_client.list_blobs())
        if not blobs:
            raise Exception("No hay archivos en el contenedor")
        
        # Filtrar solo archivos CSV que contengan 'part_1'
        blobs_csv = [blob for blob in blobs if blob.name.endswith(".csv") and "part_1" in blob.name]
        
        if not blobs_csv:
            raise Exception("No hay archivos CSV con 'part_1' en el contenedor")
        
        ultimo_blob = sorted(blobs_csv, key=lambda x: x.name, reverse=True)[0]
        
        print(f"\n Archivo seleccionado: {ultimo_blob.name}")
        return ultimo_blob.name
    except Exception as e:
        print(f"\n🚨 Error al obtener el archivo correcto: {str(e)}")
        return None

def obtener_ultimo_archivoPE(container_client):
    try:
        blobs = list(container_client.list_blobs())
        if not blobs:
            raise Exception("No hay archivos en el contenedor")
        
        # Filtrar solo archivos CSV que contengan 'part_1'
        blobs_csv = [blob for blob in blobs if blob.name.endswith(".csv") and "part_1" in blob.name]
        
        if not blobs_csv:
            raise Exception("No hay archivos CSV con 'part_1' en el contenedor")
        
        ultimo_blob = sorted(blobs_csv, key=lambda x: x.name, reverse=True)[0]
        
        print(f"\n Archivo seleccionado: {ultimo_blob.name}")
        return ultimo_blob.name
    except Exception as e:
        print(f"\n🚨 Error al obtener el archivo correcto: {str(e)}")
        return None

# Función para descargar el archivo seleccionado
def descargar_archivoCH(container_client, blob_name):
    try:
        nombre_archivo = os.path.basename(blob_name)
        ruta_local = os.path.join(Key.LOCAL_DOWNLOAD_PATH, nombre_archivo)

        if not os.path.exists(Key.LOCAL_DOWNLOAD_PATH):
            os.makedirs(Key.LOCAL_DOWNLOAD_PATH)

        blob_client = container_client.get_blob_client(blob_name)
        
        print(f"\n Descargando archivo: {blob_name}...")

        with open(ruta_local, "wb") as file:
            file.write(blob_client.download_blob().readall())

        print(f"\n Archivo descargado exitosamente: {ruta_local}")
        return ruta_local
    except Exception as e:
        print(f"\n🚨 Error al descargar el archivo: {str(e)}")
        return None

def descargar_archivoPE(container_client, blob_name):
    try:
        nombre_archivo = os.path.basename(blob_name)
        ruta_local = os.path.join(Key.LOCAL_DOWNLOAD_PATH, nombre_archivo)

        if not os.path.exists(Key.LOCAL_DOWNLOAD_PATH):
            os.makedirs(Key.LOCAL_DOWNLOAD_PATH)

        blob_client = container_client.get_blob_client(blob_name)
        
        print(f"\n Descargando archivo: {blob_name}...")

        with open(ruta_local, "wb") as file:
            file.write(blob_client.download_blob().readall())

        print(f"\n Archivo descargado exitosamente: {ruta_local}")
        return ruta_local
    except Exception as e:
        print(f"\n🚨 Error al descargar el archivo: {str(e)}")
        return None

# Función para insertar los datos del archivo CSV en PostgreSQL
def insertar_en_postgresql(ruta_csv):
    try:
        conn = psycopg2.connect(**Key.DB_CONFIG)  # Conectar a la base de datos
        cursor = conn.cursor()
        
        # Leer el CSV
        df = pd.read_csv(ruta_csv)

        # Identificar columnas de tipo fecha automáticamente (las que contienen 'date' en el nombre)
        columnas_fecha = [col for col in df.columns if 'date' in col.lower()]
        
        # Convertir todas las columnas de fecha a tipo datetime
        for col in columnas_fecha:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Reemplazar valores no válidos con None
        df = df.replace({'nan': None, 'NaN': None, 'NaT': None, pd.NaT: None})
        df = df.where(pd.notnull(df), None)
        
        print("\n📅 Cargando todas las fechas sin filtrarlas...")

        # Eliminar los datos antiguos antes de insertar nuevos registros
        cursor.execute("DELETE FROM azure_billing_data")
        conn.commit()
        print("\n🔄 Todos los datos antiguos eliminados para actualización.")

        # Insertar los datos actualizados
        columnas = ', '.join(df.columns)
        valores = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO azure_billing_data ({columnas}) VALUES ({valores})"

        for fila in df.itertuples(index=False, name=None):
            cursor.execute(insert_query, fila)

        conn.commit()
        print("\n✅ Todos los datos insertados correctamente.")

        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n🚨 Error al insertar/actualizar en PostgreSQL: {str(e)}")

# Función principal
def main():
    container_client = conexionCH()

    if container_client:
        archivo = obtener_ultimo_archivoCH(container_client)

        if archivo:
            ruta_csv = descargar_archivoCH(container_client, archivo)
            insertar_en_postgresql(ruta_csv)

    container_client = conexionPE()

    if container_client:
        archivo = obtener_ultimo_archivoPE(container_client)

        if archivo:
            ruta_csv = descargar_archivoPE(container_client, archivo)
            insertar_en_postgresql(ruta_csv)

# Ejecutar la función principal                   
if __name__ == "__main__":
    main()
