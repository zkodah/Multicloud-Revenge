from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

CONNECTION_STRING = os.getenv("CONNECTION_STRING")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
LOCAL_DOWNLOAD_PATHCH = "descargas_Part" 

def conexion_blob():
    try:
        blob_service_client =BlobServiceClient.from_connection_string(CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        if not container_client.exists():
            raise Exception(f"El contenedor '{CONTAINER_NAME}' no existe.")
        print(f"Conexión exitosa al contenedor: {CONTAINER_NAME}")
        return container_client
    except Exception as e:
        print("Error al conectar al contenedor:", e)
        return None

def listar_blobs(container_client):
    try:
        blobs = container_client.list_blobs()
        for blob in blobs:
            print(f"Blob: {blob.name}")
    except Exception as e:
        print("Error al listar blobs:", e)

def obtener_archivos01(container_client):
    try:
        blobs = list(container_client.list_blobs())
        if not blobs:
            raise Exception("No se encontraron blobs en el contenedor.")
        blobs_csv = [blob for blob in blobs if blob.name.endswith(".csv") and "part_1" in blob.name]
        if not blobs_csv:
            raise Exception("No se encontraron archivos CSV en el contenedor.")
        ultimos_blob = sorted(blobs_csv, key=lambda x: x.name,reverse=True)[0]
        return ultimos_blob.name
    except Exception as e:
        print("Error al obtener archivos:", e)
        return None
def obtener_archivos00(container_client):
    try:
        blobs = list(container_client.list_blobs())
        if not blobs:
            raise Exception("No se encontraron blobs en el contenedor.")
        blobs_csv = [blob for blob in blobs if blob.name.endswith(".csv") and "part_0" in blob.name]
        if not blobs_csv:
            raise Exception("No se encontraron archivos CSV en el contenedor.")
        ultimos_blob = sorted(blobs_csv, key=lambda x: x.name,reverse=True)[0]
        return ultimos_blob.name
    except Exception as e:
        print("Error al obtener archivos:", e)
        return None
def descargar_blob01(container_client, blob_name):
    try:
        nombre_archivo01 = os.path.basename(blob_name)
        ruta_local = os.path.join(LOCAL_DOWNLOAD_PATHCH, nombre_archivo01)

        if not os.path.exists(LOCAL_DOWNLOAD_PATHCH):
            os.makedirs(LOCAL_DOWNLOAD_PATHCH)
        blob_client = container_client.get_blob_client(blob_name)
        with open(ruta_local, "wb") as archivo:
            archivo.write(blob_client.download_blob().readall())
        print(f"Archivo {nombre_archivo01} descargado en {ruta_local}")
        return ruta_local
    except Exception as e:
        print("Error al descargar el blob:", e)
        return None
def informacion_csv(ruta_local):
    try:
        df = pd.read_csv(ruta_local)

        # Verifica que las columnas existen
        columnas_necesarias = [
            "invoiceId", "date",
            "costInBillingCurrency", "costInPricingCurrency",
            "costInUsd", "paygCostInBillingCurrency", "paygCostInUsd"
        ]
        if not all(col in df.columns for col in columnas_necesarias):
            raise ValueError("El archivo CSV no contiene todas las columnas requeridas.")

        # Agrupar por invoiceId y date, sumando los campos numéricos
        tabla_resumen = df.groupby(["invoiceId", "date"], as_index=False)[
            ["costInBillingCurrency", "costInPricingCurrency", "costInUsd", "paygCostInBillingCurrency", "paygCostInUsd"]
        ].sum()

        print("\nResumen agrupado por invoiceId y date:\n")
        print(tabla_resumen)

        # (Opcional) guardar a CSV
        tabla_resumen.to_csv("resumen_costos.csv", index=False)
        print("\nArchivo 'resumen_costos.csv' generado.")
    except Exception as e:
        print("Error procesando el archivo CSV:", e)

def main():
    container_client = conexion_blob()
    if container_client:
        listar_blobs(container_client)
        archivo01 = obtener_archivos01(container_client)
        if archivo01:
            print(f"Último archivo CSV: {archivo01}")
        archivo00 = obtener_archivos00(container_client)
        if archivo00:
            print(f"Último archivo CSV: {archivo00}")
            archivo00 = obtener_archivos00(container_client)
        ruta_local = descargar_blob01(container_client, archivo01)
        if ruta_local:
            informacion_csv(ruta_local)

if __name__ == "__main__":
    main()