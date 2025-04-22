import http.client
import json
import os
import pandas as pd
from io import StringIO
from token_mana import obtener_token

ACCOUNT_ID = "2853"
REPORT_ID = "20005"
DOWNLOAD_PATH = "./descargas"

def descargar_reporte(access_token):
    payload = json.dumps({
        "report_id": REPORT_ID,
        "date_range_option": {
            "selected_range": {
                "relative_date_range": "TODAY"
            }
        }
    })

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    ruta = f"/api/v3/accounts/{ACCOUNT_ID}/reports/{REPORT_ID}/reportDataCsv"
    conn = http.client.HTTPSConnection("ion.tdsynnex.com")
    conn.request("POST", ruta, payload, headers)
    res = conn.getresponse()
    data = res.read()

    if res.status == 200:
        try:
            json_data = json.loads(data.decode('utf-8'))
            csv_text = json_data.get("results", "")
            if not csv_text:
                csv_text = data.decode('utf-8')
        except json.JSONDecodeError:
            csv_text = data.decode('utf-8')

        os.makedirs(DOWNLOAD_PATH, exist_ok=True)
        raw_file_path = os.path.join(DOWNLOAD_PATH, f"raw_costos_diarios_test_{ACCOUNT_ID}.csv")
        with open(raw_file_path, "w", encoding="utf-8") as f:
            f.write(csv_text)
        print(f"Archivo raw guardado en: {raw_file_path}")

        try:
            df = pd.read_csv(StringIO(csv_text), sep=None, engine="python")
        except Exception as e:
            print(f"Error al procesar el CSV: {e}")
            return None

        if "Customer Name" in df.columns:
            df = df[df["Customer Name"] != "Auna (Jaime Pradenas)"]

        cleaned_file_path = os.path.join(DOWNLOAD_PATH, f"costos_diarios_test_{ACCOUNT_ID}_limpio.csv")
        df.to_csv(cleaned_file_path, index=False, sep=",", encoding="utf-8")
        print(f"Reporte CSV limpio guardado en: {cleaned_file_path}")
        return cleaned_file_path
    else:
        print(f"Error al descargar CSV: {res.status} - {data.decode('utf-8')}")
        return None

def verificar_informacion(csv_path):
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error al leer el CSV: {e}")
        return None

    if "Customer Name" not in df.columns or "Seller Cost (USD)" not in df.columns:
        print("El archivo no contiene las columnas necesarias.")
        return None

    # Agrupar sin fechas, solo por cliente
    resumen = df.groupby("Customer Name")["Seller Cost (USD)"].sum().reset_index()
    resumen["Seller Cost (USD)"] = resumen["Seller Cost (USD)"].round(2)
    return resumen

def resumen_por_cuenta(df):
    if df.empty:
        print(" El DataFrame está vacío.")
        return None
    
    # Agrupar por las columnas claves y sumar costos
    resumen = df.groupby(
        ["Customer Name", "Cloud Account Number", "Product Name"]
    ).agg({
        "Seller Cost (USD)": "sum",
        "Customer Cost (USD)": "sum",
        "Margin (USD)": "sum"
    }).reset_index()

    # Redondear para presentación
    resumen[["Seller Cost (USD)", "Customer Cost (USD)", "Margin (USD)"]] = resumen[[
        "Seller Cost (USD)", "Customer Cost (USD)", "Margin (USD)"
    ]].round(4)

    print("\n Resumen de costos por cuenta y producto:")
    print(resumen)
    return resumen


def main():
    access_token = obtener_token()
    if access_token:
        csv_path = descargar_reporte(access_token)
        if csv_path:
            print(f"\n Archivo CSV limpio guardado en: {csv_path}")
            tabla = verificar_informacion(csv_path)
            df = pd.read_csv(csv_path)
            resumen_por_cuenta(df)
            if tabla is not None:
                print("\n Costos por cliente del día:")
                print(tabla)
        else:
            print(" No se pudo descargar o limpiar el CSV.")
    else:
        print(" No se pudo obtener un token de acceso.")

if __name__ == "__main__":
    main()
