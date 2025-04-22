import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import Key
import os
from sqlalchemy import create_engine

# Función para obtener los datos de la base de datos
def obtener_datos():
    try:
        engine = create_engine(
            f"postgresql://{Key.DB_CONFIG['user']}:{Key.DB_CONFIG['password']}@{Key.DB_CONFIG['host']}:{Key.DB_CONFIG['port']}/{Key.DB_CONFIG['dbname']}"
        )
        query = "SELECT date, customername, costinbillingcurrency, partnername FROM azure_billing_data"
        df = pd.read_sql(query, engine)
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        print(f"\ Error al obtener datos: {str(e)}")
        return None

# Función para generar la tabla pivote
def generar_tabla(df):
    try:
        if df is None or not {'date', 'costinbillingcurrency', 'customername', 'partnername'}.issubset(df.columns):
            print("\ Error: DataFrame inválido o faltan columnas necesarias.")
            return None
        
        df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')

        hoy = datetime.today().date()
        dias_precisos = pd.date_range(end=hoy - timedelta(days=1), periods=7).strftime('%Y-%m-%d')
        df = df[df["date"].isin(dias_precisos)]

        tabla_pivot = df.pivot_table(
            index=["partnername", "customername"],
            columns="date",
            values="costinbillingcurrency",
            aggfunc="sum"
        ).fillna(0)
        return tabla_pivot.reset_index()
    except Exception as e:
        print(f"\ Error al generar la tabla: {str(e)}")
        return None