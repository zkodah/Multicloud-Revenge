import http.client # Importar el módulo http.client
import json # Importar el módulo json
import os # Importar el módulo os
import pandas as pd # Importar el módulo pandas
import psycopg2 # Importar el módulo psycopg2
from io import StringIO # Importar StringIO del módulo io
from datetime import datetime, timezone, timedelta # Importar datetime, timezone y timedelta del módulo datetime
import key # Importar el módulo key

ACCOUNT_ID = "2853"
REPORT_ID = "20005"

hoy = datetime.now(timezone.utc)
ayer = hoy - timedelta(days=1)
fecha_hoy_str = hoy.strftime("%Y-%m-%d")

DOWNLOAD_PATH = "./descargas"

def descargar_reporte(access_token):
    conn = http.client.HTTPSConnection("ion.tdsynnex.com")
    payload = json.dumps({
        "report_id": REPORT_ID,
        "date_range_option": {
            "selected_range": {
                "available_range": {
                    "relative_actual_date_range": {
                        "start_date": ayer.strftime("%Y-%m-%d"),
                        "end_date": hoy.strftime("%Y-%m-%d")
                    }
                }
            }
        }
    })

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + access_token
    }
    ruta = f"/api/v3/accounts/{ACCOUNT_ID}/reports/{REPORT_ID}/reportDataCsv"
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

        # Guardar CSV crudo para depuración
        raw_file_path = os.path.join(DOWNLOAD_PATH, f"raw_costos_diarios_{ACCOUNT_ID}.csv")
        os.makedirs(DOWNLOAD_PATH, exist_ok=True)
        with open(raw_file_path, "w", encoding="utf-8") as f:
            f.write(csv_text)
        
        try:
            df = pd.read_csv(StringIO(csv_text), sep=None, engine="python")
        except Exception as e:
            print(f"Error al procesar el CSV: {e}")
            return None
        
        df = df[df["Customer Name"] != "Auna (Jaime Pradenas)"]
        if "Fecha" in df.columns:
            df["Fecha"].fillna(fecha_hoy_str, inplace=True)
        else:
            df["Fecha"] = fecha_hoy_str     
        cleaned_file_path = os.path.join(DOWNLOAD_PATH, f"costos_diarios_{ACCOUNT_ID}_limpio.csv")
        df.to_csv(cleaned_file_path, index=False, sep=",")
        print(f"Reporte CSV limpio guardado en: {cleaned_file_path}")
        
        return cleaned_file_path
    else:
        print(f"Error al descargar CSV: {res.status} - {data.decode('utf-8')}")
        return None
    
def insertar_en_postgres(df):
    try:
        conn = psycopg2.connect(**key.DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT customer_name, fecha, seller_cost_usd FROM aws_billing_data")
        registros_existentes = set()

        for fila in cursor.fetchall():
            try:
                customer_name = fila[0]
                fecha = fila[1]
                seller_cost_usd = float(fila[2]) if fila[2] is not None else 0.0
                seller_cost_usd = round(seller_cost_usd, 3) 
                registros_existentes.add((customer_name, fecha, seller_cost_usd))
            except Exception as e:
                print(f"⚠ Error al convertir valores existentes: {e}")
        df["seller_cost_usd"] = df["Seller Cost (USD)"].astype(float).round(3)  
        df["Customer Cost (USD)"] = df["Customer Cost (USD)"].astype(float).round(5)
        df["Margin (USD)"] = df["Margin (USD)"].astype(float).round(4)
        df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date  

        df_nuevos = df[~df.apply(lambda row: (row["Customer Name"], row["Fecha"], row["seller_cost_usd"]) in registros_existentes, axis=1)]

        if df_nuevos.empty:
            cursor.close()
            conn.close()
            return
        for _, row in df_nuevos.iterrows():
            cursor.execute(
                """
                INSERT INTO aws_billing_data 
                (customer_name, cloud_account_number, product_name, usage_type, price_book, 
                 seller_cost_usd, customer_cost_usd, margin_usd, usage_quantity, fecha)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row.get("Customer Name", "Unknown"),
                    row.get("Cloud Account Number", "Unknown"),
                    row.get("Product Name", "Unknown"),
                    row.get("Usage Type", "Unknown"),
                    row.get("Price Book", "Unknown"),
                    row.get("seller_cost_usd", 0),
                    row.get("Customer Cost (USD)", 0),
                    row.get("Margin (USD)", 0),
                    row.get("Usage Quantity", 0),
                    row.get("Fecha")
                )
            )
        conn.commit()
        cursor.close()
        conn.close()
        print(f" Insertados {len(df_nuevos)} registros nuevos en PostgreSQL.")

    except Exception as e:
        print(f" Error al insertar en PostgreSQL: {e}")

def compararC():
    try:
        conn = psycopg2.connect(**key.DB_CONFIG)
        query = """
        SELECT customer name,cloud_account_number,product_name,seller_cost_usd,fecha
        FROM aws_billing_data
        WHERE fecha >= %s AND fecha <= %s
        """
        hoy = datetime.today().date()
        ayer = hoy - timedelta(days=1)

        df = pd.read_sql_query(query, conn, params=(ayer, hoy))
        conn.close()

        if df.empty:
            print("No hay datos para comparar.")
            return None
        
        df["fecha"] = pd.to_datetime(df["fecha"]).dt.strftime("%Y-%m-%d")

        pivot = df.pivot_table(
            index=["customer_name", "cloud_account_number", "product_name"],
            values=["seller_cost_usd"],
            aggfunc="sum"
        ).fillna(0)

        pivot ["Diferencia USD"] = (
            pivot.get(hoy.strftime("%Y-%m-%d"), 0) -
            pivot.get(ayer.strftime("%Y-%m-%d"), 0)
        )

        pivot = pivot.sort_values(by=["Diferencia USD"], ascending=False)

        return pivot.reset_index()
    except Exception as e:
        print(f"Error al comparar datos: {e}")
        return None