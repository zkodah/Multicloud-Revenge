import pandas as pd
from token_mana import obtener_token
from descarg import descargar_reporte, insertar_en_postgres
import envioAWS

def main():
    print("\nObteniendo token...")
    access_token = obtener_token()
    
    if not access_token:
        print("\nNo se pudo obtener un token válido.")
        return
    
    print("\nToken obtenido con éxito.")
    print("\nEnviando correo...")

    csv_path = descargar_reporte(access_token)
    if not csv_path:
        print("\nError al descargar o limpiar el CSV.")
        return
    
    print(f"\nArchivo CSV limpio guardado en: {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"\nError al leer el CSV: {e}")
        return
    
    if df.empty:
        print("\nEl CSV está vacío. No hay datos nuevos para insertar.")
    else:
        insertar_en_postgres(df)
        print("\nProceso de inserción finalizado.")
        #envioAWS.enviar_correo()

if __name__ == "__main__":
    main()
