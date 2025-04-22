import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import Key

# Carpeta donde se guardarán los gráficos
GRAPH_FOLDER = "graficos_clientes"

# Función para obtener los datos de la base de datos
def obtener_datos():
    try:
        engine = create_engine(f"postgresql://{Key.DB_CONFIG['user']}:{Key.DB_CONFIG['password']}@{Key.DB_CONFIG['host']}:{Key.DB_CONFIG['port']}/{Key.DB_CONFIG['dbname']}")
        query = """
        SELECT date, customername, costinbillingcurrency, partnername 
        FROM azure_billing_data 
        WHERE partnername = 'GTD PERÚ S.A'
        """
        df1 = pd.read_sql(query, engine)
        df1.columns = df1.columns.str.lower()
        return df1
    except Exception as e:
        print(f"\n🚨 Error al obtener datos: {str(e)}")
        return None

# Función para generar los gráficos por cliente, separados por 'partnername'
def graficar_por_cliente(df2, show=False):
    try:
        if df2 is None or not {'date', 'customername', 'costinbillingcurrency', 'partnername'}.issubset(df2.columns):
            print("\n🚨 Error: DataFrame inválido o faltan columnas necesarias para graficar.")
            return

        df2["date"] = pd.to_datetime(df2["date"])
        df2 = df2.groupby(["date", "customername", "partnername"]).sum().reset_index()
        df2["diferencia"] = df2.groupby("customername")["costinbillingcurrency"].diff().fillna(0)
        df2["date"] = df2["date"].dt.strftime('%d-%m-%Y')

        
        hoy = datetime.today().date()
        dias_precisos = pd.date_range(end=hoy - timedelta(days=1), periods = 7).strftime('%d-%m-%Y')
        df2 = df2[df2["date"].isin(dias_precisos)]

        archivos_generados = set()
        
        for idx, (partner, customer) in enumerate(df2.groupby(["partnername", "customername"]).groups.keys(), start=1):
            partner_folder = os.path.join(GRAPH_FOLDER, partner)
            os.makedirs(partner_folder, exist_ok=True)
            
            filename = f"Cliente_{idx}.jpg"
            save_path = os.path.join(partner_folder, filename)
            
            if save_path in archivos_generados:
                continue  # Evita generar el mismo gráfico más de una vez
            archivos_generados.add(save_path)
            
            df_customer = df2[(df2["partnername"] == partner) & (df2["customername"] == customer)]
            
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.set_title(f"{partner}\nCliente: {customer}", fontsize=14, fontweight="bold")

            sns.barplot(x="date", y="costinbillingcurrency", data=df_customer, color="skyblue", label="Costo", ax=ax)
            ax.bar(df_customer["date"], df_customer["diferencia"], color="red", label="Diferencia")

            ax.set_xlabel("Fecha")
            ax.set_ylabel("Costo (USD)")
            ax.tick_params(axis='x', rotation=45)
            ax.legend(loc="lower left")

            for container in ax.containers:
                ax.bar_label(container, fmt="$%.2f", fontsize=8, padding=1)

            fig.tight_layout()
            fig.savefig(save_path, dpi=300, bbox_inches="tight")
            plt.close(fig)
            
            print(f"✅ Gráfico guardado: {save_path}")

            if show:
                plt.show()
    except Exception as e:
        print(f"\n🚨 Error al graficar: {str(e)}")
