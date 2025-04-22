from datetime import datetime, timedelta  # Importación necesaria para trabajar con fechas
from azure.storage.blob import BlobServiceClient  # Importación necesaria para trabajar con Azure Blob Storage
import os  # Importación necesaria para trabajar con archivos
import psycopg2  # Importación necesaria para trabajar con PostgreSQL
import pandas as pd  # Importación necesaria para trabajar con DataFrames
import matplotlib.pyplot as plt  # Importación necesaria para graficar
import Graficos2  # Graficos2.py
import Graficos1  # Graficos1.py
import Envio  # Envio.py
import Key  # Key.py

 
# Función para conectar al contenedor de Azure Blob Storage
def conexionCH():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(Key.CONNECTION_STRINGCH) # conexion mediante la cadena de conexion
        container_clientCH = blob_service_client.get_container_client(Key.CONTAINER_NAMECH) # obtiene el cliente del contenedor
 
        if not container_clientCH.exists():
            raise Exception("El contenedor no existe")
        print("\n\nContenedor conectado:", Key.CONTAINER_NAMECH)
        return container_clientCH
    except Exception as e:
        print(f"\n Error al conectar: {str(e)}")
        return None
def conexionPE():
    try:
        blob_service_clientPE = BlobServiceClient.from_connection_string(Key.CONNECTION_STRINGPE)
        container_clientPE = blob_service_clientPE.get_container_client(Key.CONTAINER_NAMEPE)
 
        if not container_clientPE.exists():
            raise Exception("El contenedor no existe")
        print("\n\nContenedor conectado:", Key.CONTAINER_NAMEPE)
        return container_clientPE
    except Exception as e:
        print(f"\n Error al conectar: {str(e)}")
        return None
 
# Función para listar los archivos en el contenedor
def listar_archivosCH(container_clientCH):
    try:
        print("\n\n Listando archivos...")
        blobs = container_clientCH.list_blobs()
        for blob in blobs:
            print(f" - {blob.name} ({blob.size} bytes)")
    except Exception as e:
        print(f"\n Error al listar archivos: {str(e)}")
def listar_archivosPE(container_clientPE):
    try:
        print("\n\n Listando archivos...")
        blobs = container_clientPE.list_blobs()
        for blob in blobs:
            print(f" - {blob.name} ({blob.size} bytes)")
    except Exception as e:
        print(f"\n Error al listar archivos: {str(e)}")
   
# Función para obtener el último archivo en el contenedor
def obtener_ultimo_archivoCH(container_clientCH):
    try:
        blobs = list(container_clientCH.list_blobs())
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
        print(f"\n Error al obtener el archivo correcto: {str(e)}")
        return None
def obtener_ultimo_archivoPE(container_clientPE):
    try:
        blobs = list(container_clientPE.list_blobs())
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
        print(f"\n Error al obtener el archivo correcto: {str(e)}")
        return None
 
# Función para descargar el archivo seleccionado
def descargar_archivoCH(container_clientCH, blob_nameCH):
    try:
        nombre_archivoCH = os.path.basename(blob_nameCH)
        ruta_local = os.path.join(Key.LOCAL_DOWNLOAD_PATHCH, nombre_archivoCH)
 
        if not os.path.exists(Key.LOCAL_DOWNLOAD_PATHCH):
            os.makedirs(Key.LOCAL_DOWNLOAD_PATHCH)
 
        blob_client = container_clientCH.get_blob_client(blob_nameCH)
       
        print(f"\n Descargando archivo: {blob_nameCH}...")
 
        with open(ruta_local, "wb") as file:
            file.write(blob_client.download_blob().readall())
 
        print(f"\n Archivo descargado exitosamente: {ruta_local}")
        return ruta_local
    except Exception as e:
        print(f"\n Error al descargar el archivo: {str(e)}")
        return None
def descargar_archivoPE(container_clientPE, blob_namePE):
    try:
        nombre_archivoPE = os.path.basename(blob_namePE)
        ruta_local = os.path.join(Key.LOCAL_DOWNLOAD_PATHPE, nombre_archivoPE)
 
        if not os.path.exists(Key.LOCAL_DOWNLOAD_PATHPE):
            os.makedirs(Key.LOCAL_DOWNLOAD_PATHPE)
 
        blob_client = container_clientPE.get_blob_client(blob_namePE)
       
        print(f"\n Descargando archivo: {blob_namePE}...")
 
        with open(ruta_local, "wb") as file:
            file.write(blob_client.download_blob().readall())
 
        print(f"\n Archivo descargado exitosamente: {ruta_local}")
        return ruta_local
    except Exception as e:
        print(f"\n🚨 Error al descargar el archivo: {str(e)}")
        return None
 
def insertar_y_actualizar_datos(ruta_csvCH, ruta_csvPE):
    try:
        conn = psycopg2.connect(**Key.DB_CONFIG)
        cursor = conn.cursor()
        
        # Leer ambos archivos CSV
        df_chile = pd.read_csv(ruta_csvCH)
        df_peru = pd.read_csv(ruta_csvPE)
        
        # Unificar los datos en un solo DataFrame
        df = pd.concat([df_chile, df_peru], ignore_index=True)
        
        # Convertir las columnas de fecha a datetime
        columnas_fecha = [
            'billingPeriodEndDate', 'billingPeriodStartDate',
            'servicePeriodEndDate', 'servicePeriodStartDate', 'exchangeRateDate'
        ]
        for col in columnas_fecha:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        
        df = df.replace({'nan': None, 'NaN': None, 'NaT': None, pd.NaT: None})
        df = df.where(pd.notnull(df), None)

        # Obtener fechas existentes en la base de datos
        cursor.execute("SELECT DISTINCT date FROM azure_billing_data")
        fechas_existentes = {fila[0] for fila in cursor.fetchall()}
        
        # Filtrar datos nuevos (que no estén en la base de datos)
        df_nuevas_fechas = df[~df['date'].isin(fechas_existentes)]

        # Definir consulta de inserción antes de cualquier uso
        columnas = ', '.join(df.columns)
        valores = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO azure_billing_data ({columnas}) VALUES ({valores})"

        if not df_nuevas_fechas.empty:
            print("\n Insertando datos históricos en la base de datos...")
            for fila in df_nuevas_fechas.itertuples(index=False, name=None):
                cursor.execute(insert_query, fila)
            conn.commit()
            print("\n Datos históricos insertados correctamente.")
        else:
            print("\n No hay datos históricos nuevos para insertar.")
        
        # Actualizar datos de ayer y hoy
        hoy = datetime.today().date()
        ayer = hoy - timedelta(days=1)
        fechas_actualizar = [ayer, hoy]
        
        print(f"\n Actualizando datos de ayer ({ayer}) y hoy ({hoy})...")
        
        cursor.execute("DELETE FROM azure_billing_data WHERE date = ANY(%s)", (fechas_actualizar,))
        conn.commit()
        print("\n Datos de ayer y hoy eliminados para actualización.")
        
        df_actualizar = df[df['date'].isin(fechas_actualizar)]
        if not df_actualizar.empty:
            for fila in df_actualizar.itertuples(index=False, name=None):
                cursor.execute(insert_query, fila)
            conn.commit()
            print("\n Datos de ayer y hoy actualizados correctamente.")
        else:
            print("\n No hay datos recientes para actualizar.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"\n Error al insertar/actualizar en PostgreSQL: {str(e)}")
# Función principal
#def mainCH():
 
 #   container_clientCH = conexionCH()

  #  if container_clientCH: 
   #     listar_archivosCH(container_clientCH)
    #    archivoCH = obtener_ultimo_archivoCH(container_clientCH)
     #   if archivoCH :
      #      ruta_csvCH = descargar_archivoCH(container_clientCH, archivoCH)
       #     insertar_en_postgresqlCH(ruta_csvCH)  # Insertar datos de CH
        #   df2 = Graficos2.obtener_datos()  # Obtener datos después de la inserción de CH
         #  if df2 is not None:
          #  Graficos2.graficar_por_cliente(df2, show=False)
       #print("\n\n Proceso finalizado.")
    
 
#f mainPE():
 #  container_clientPE = conexionPE()

  # if container_clientPE:
   #    listar_archivosPE(container_clientPE)
    #   archivoPE = obtener_ultimo_archivoPE(container_clientPE)

     #  if archivoPE:
      #     ruta_csvPE = descargar_archivoPE(container_clientPE, archivoPE)
       #    insertar_en_postgresqlPE(ruta_csvPE)  # Insertar datos de PE
        #   df = Graficos1.obtener_datos()  # Obtener datos después de la inserción de PE
         #  if df is not None:
          #  Graficos1.graficar_por_cliente(df, show=False)

       #print("\n\n Proceso finalizado.")
    #container_clientPE = conexionPE()
 
    #if container_clientPE:
        #listar_archivosPE(container_clientPE)
        #archivoPE = obtener_ultimo_archivoPE(container_clientPE)
 
        #if archivoPE:
            #ruta_csvPE = descargar_archivoPE(container_clientPE, archivoPE)
            #insertar_en_postgresql()
            #df = Graficos1.obtener_datos()
            #if df is not None:
             #   Graficos1.graficar_por_cliente(df, show=False)
              #  print("\n\n Proceso finalizado.")
            #else:
             #   print("\n Error al cargar datos para graficar.")
    
   
           
# Ejecutar la función principal                  
#f __name__ == "__main__":
   #ainCH()
   #mainPE()   
   #Envio.enviar_correo() 

def main():

    ruta_csvCH = None
    container_clientCH = conexionCH()
    if container_clientCH: 
        listar_archivosCH(container_clientCH)
        archivoCH = obtener_ultimo_archivoCH(container_clientCH)
        if archivoCH:
            ruta_csvCH = descargar_archivoCH(container_clientCH, archivoCH)
        else:
            print("No se encontró archivo en el contenedor de Chile.")
    else:
        print("No se pudo conectar al contenedor de Chile.")
    

    ruta_csvPE = None
    container_clientPE = conexionPE()
    if container_clientPE:
        listar_archivosPE(container_clientPE)
        archivoPE = obtener_ultimo_archivoPE(container_clientPE)
        if archivoPE:
            ruta_csvPE = descargar_archivoPE(container_clientPE, archivoPE)
        else:
            print("No se encontró archivo en el contenedor de Perú.")
    else:
        print("No se pudo conectar al contenedor de Perú.")
# valido la ruta
    if ruta_csvCH is None and ruta_csvPE is None:
        print("No se encontraron archivos CSV para procesar. Proceso finalizado.")
        return

    insertar_y_actualizar_datos(ruta_csvCH, ruta_csvPE)

    df_ch = Graficos2.obtener_datos()  # Datos actualizados de Chile
    if df_ch is not None:
        Graficos2.graficar_por_cliente(df_ch, show=False)

    df_pe = Graficos1.obtener_datos()  # Datos actualizados de Perú
    if df_pe is not None:
        Graficos1.graficar_por_cliente(df_pe, show=False)

    print("\n\nProceso finalizado.")
    Envio.enviar_correo()


if __name__ == "__main__":
    main()