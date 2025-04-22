import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import key

# Cargar el CSV
df = pd.read_csv("azure_export_to_aws.csv")

# Convertir columnas a tipos correctos
df["seller_cost_usd"] = df["seller_cost_usd"].astype(float).round(2)
df["customer_cost_usd"] = df["customer_cost_usd"].astype(float).round(3)
df["margin_usd"] = df["margin_usd"].astype(float).round(3)
df["usage_quantity"] = df["usage_quantity"].astype(float).round(2)
df["fecha"] = pd.to_datetime(df["fecha"]).dt.date

# Conexión a PostgreSQL
conn = psycopg2.connect(**key.DB_CONFIG)
cursor = conn.cursor()

# Obtener registros existentes para evitar duplicados
cursor.execute("SELECT customer_name, fecha, seller_cost_usd FROM aws_billing_data")
existentes = set((fila[0], fila[1], round(float(fila[2]), 2)) for fila in cursor.fetchall())

# Filtrar nuevos datos
df_nuevos = df[~df.apply(lambda row: (row["customer_name"], row["fecha"], row["seller_cost_usd"]) in existentes, axis=1)]

print(f" Registros nuevos para insertar: {len(df_nuevos)}")

# Insertar en la tabla aws_billing_data
for _, row in df_nuevos.iterrows():
    cursor.execute("""
        INSERT INTO aws_billing_data (
            customer_name, cloud_account_number, product_name, usage_type, price_book,
            seller_cost_usd, customer_cost_usd, margin_usd, usage_quantity, fecha
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row.get("customer_name", "Unknown"),
        row.get("cloud_account_number", "Unknown"),
        row.get("product_name", "Unknown"),
        row.get("usage_type", "Unknown"),
        row.get("price_book", "Unknown"),
        row.get("seller_cost_usd", 0),
        row.get("customer_cost_usd", 0),
        row.get("margin_usd", 0),
        row.get("usage_quantity", 0),
        row.get("fecha")
    ))

conn.commit()
cursor.close()
conn.close()
print(" Datos insertados en aws_billing_data correctamente.")
