import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import key  

def obtener_datos_aws():
    
    try:
        engine = create_engine(
            f"postgresql://{key.DB_CONFIG['user']}:{key.DB_CONFIG['password']}@{key.DB_CONFIG['host']}:{key.DB_CONFIG['port']}/{key.DB_CONFIG['dbname']}"
        )
        query = """
        SELECT customer_name, fecha, margin_usd, seller_cost_usd, customer_cost_usd
        FROM aws_billing_data
        """
        df = pd.read_sql(query, engine)
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        print(f"Error al obtener datos de AWS: {e}")
        return None

def generar_tabla_aws(df):
    try:
        if df is None or not {'fecha', 'seller_cost_usd', 'customer_name'}.issubset(df.columns):
            print("Error: DataFrame inválido o faltan columnas necesarias.")
            return None
        
        df["fecha"] = pd.to_datetime(df["fecha"]).dt.strftime('%Y-%m-%d')

        hoy = datetime.today().date()
        dias_precisos = pd.date_range(end=hoy, periods=7).strftime('%Y-%m-%d')
        df = df[df["fecha"].isin(dias_precisos)]
        tabla_pivot = df.pivot_table(
        index=["customer_name"],
        columns="fecha",
        values="seller_cost_usd",
        aggfunc="sum"
        ).fillna(0)
        return tabla_pivot.reset_index()

    except Exception as e:
        print(f"Error al generar la tabla: {e}")
        return None


