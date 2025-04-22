import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import os
import key


def compararC():
    try:
        # Conexión a PostgreSQL
        conn = psycopg2.connect(**key.DB_CONFIG)
        query = """
            SELECT 
                customer_name,
                cloud_account_number,
                product_name,
                seller_cost_usd,
                fecha
            FROM aws_billing_data
            WHERE fecha >= %s AND fecha <= %s
        """
        hoy = datetime.today().date()
        ayer = hoy - timedelta(days=1)
        hoy_str = hoy.strftime("%Y-%m-%d")

        df = pd.read_sql_query(query, conn, params=(ayer, hoy))
        conn.close()

        if df.empty:
            print("No hay datos para comparar.")
            return None

        df["fecha"] = pd.to_datetime(df["fecha"]).dt.date

        pivot = df.pivot_table(
            index=["customer_name", "cloud_account_number", "product_name"],
            columns="fecha",
            values="seller_cost_usd",
            aggfunc="sum"
        ).fillna(0)

        pivot["Costo Ayer"] = pivot.get(ayer, 0)
        pivot["Costo Hoy"] = pivot.get(hoy, 0)
        pivot["diff_raw"] = pivot["Costo Hoy"] - pivot["Costo Ayer"]

        pivot = pivot[pivot["diff_raw"] != 0]
        if pivot.empty:
            print("No hay cambios en los costos entre ayer y hoy.")
            return None

        resultado = pivot.reset_index()[[
            "customer_name",
            "cloud_account_number",
            "product_name",
            "Costo Ayer",
            "Costo Hoy",
            "diff_raw"
        ]]
        resultado = resultado.rename(columns={"diff_raw": "Diferencia USD"})

        for col in ["Costo Ayer", "Costo Hoy", "Diferencia USD"]:
            resultado[col] = resultado[col].round(4)

        resultado = resultado.sort_values(by="Diferencia USD", ascending=False).reset_index(drop=True)

        os.makedirs("./descargas", exist_ok=True)

        # Guardar detalle con fecha
        detalle_path = f"./descargas/diferencias_detalle_{hoy_str}.csv"
        resultado.to_csv(detalle_path, index=False)
        print(f"CSV guardado: {detalle_path}")

        # Guardar resumen con fecha
        resumen = resultado.groupby("customer_name", as_index=False)["Diferencia USD"].sum()
        resumen_path = f"./descargas/diferencias_totales_por_cliente_{hoy_str}.csv"
        resumen.to_csv(resumen_path, index=False)
        print(f"Resumen guardado: {resumen_path}")

        return resultado

    except Exception as e:
        print(f"Error al comparar datos: {e}")
        return None


if __name__ == '__main__':
    tabla_comparacion = compararC()
    if tabla_comparacion is not None:
        print(tabla_comparacion)
