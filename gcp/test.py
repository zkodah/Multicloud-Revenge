import pandas as pd
import psycopg2
import key_gcp
from sqlalchemy import create_engine
from datetime import datetime, timedelta


# Configurar conexión con PostgreSQL
DB_URL = f"postgresql://{key_gcp.DB_CONFIG['DB_USER']}:{key_gcp.DB_CONFIG['DB_PASSWORD']}@{key_gcp.DB_CONFIG['DB_HOST']}:{key_gcp.DB_CONFIG['DB_PORT']}/{key_gcp.DB_CONFIG['DB_NAME']}"
engine = create_engine(DB_URL)

# Establecer conexión con PostgreSQL
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# Obtener la fecha de hoy
today = datetime.today()

# Definir el rango de las últimas dos semanas
start_of_two_weeks_ago = today - timedelta(days=14)

# Convertir a string para PostgreSQL
start_of_two_weeks_ago_str = start_of_two_weeks_ago.strftime('%Y-%m-%d')

'''
# Consulta SQL corregida para coincidir con la consulta manual
query = """
    SELECT 
        TO_CHAR(DATE_TRUNC('week', c.fecha), 'YYYY-"Semana"W') AS semana,
        ROUND(SUM(c.subtotal)::NUMERIC, 2) AS total_subtotal
    FROM gcp_costos_diarios c
    WHERE c.fecha >= %s
    GROUP BY DATE_TRUNC('week', c.fecha)
    ORDER BY semana DESC;
"""

# Ejecutar la consulta
cursor.execute(query, (start_of_two_weeks_ago_str,))
data = cursor.fetchall()

# Cerrar conexión
cursor.close()
conn.close()

# Validar datos obtenidos
if len(data) >= 2:
    semana_anterior, total_anterior = data[0]
    semana_dos_semanas, total_dos_semanas = data[1]

    diferencia = total_anterior - total_dos_semanas
    diferencia_porcentual = (diferencia / total_dos_semanas) * 100 if total_dos_semanas > 0 else 0

    print(f"Semana: {semana_anterior}")
    print(f"Total Subtotal Anterior: {total_anterior}")
    print(f"Semana Anterior: {semana_dos_semanas}")
    print(f"Total Subtotal 2 Semanas Atrás: {total_dos_semanas}")
    print(f"Diferencia porcentual: {diferencia_porcentual:.2f}%")

    if abs(diferencia_porcentual) > 50:
        print("¡Diferencia superior al 50%! 🚨")
    else:
        print("Diferencia dentro de lo esperado. ✅")

else:
    print("No hay datos suficientes para comparar.")
'''


# Obtener los últimos 7 días (excluyendo hoy)
query = """
WITH fechas AS (
    SELECT GENERATE_SERIES(
        CURRENT_DATE - INTERVAL '7 days',
        CURRENT_DATE - INTERVAL '1 day',
        INTERVAL '1 day'
    )::DATE AS fecha
)
SELECT 
    f.fecha,
    ROUND(COALESCE(SUM(c.subtotal), 0)::NUMERIC, 2) AS total_subtotal
FROM fechas f
LEFT JOIN gcp_costos_diarios c ON f.fecha = c.fecha
LEFT JOIN gcp_responsables p ON c.project_id = p.project_id
GROUP BY f.fecha
ORDER BY f.fecha DESC
LIMIT 2;
"""

# Ejecutar la consulta y cargar los datos en un DataFrame
df = pd.read_sql(query, conn)

# Cerrar la conexión
conn.close()

# Verificar si hay al menos 2 registros para calcular la diferencia
if len(df) < 2:
    print("No hay suficientes datos para calcular la diferencia porcentual.")
else:
    # Extraer valores
    fecha_actual, total_actual = df.iloc[0]
    fecha_anterior, total_anterior = df.iloc[1]

    # Calcular la diferencia porcentual
    if total_anterior == 0:
        diferencia_porcentual = None
        alerta = "N/A"
    else:
        diferencia_porcentual = round(((total_actual - total_anterior) / total_anterior) * 100, 2)
        alerta = "⚠️ ¡ALERTA!" if abs(diferencia_porcentual) > 50 else "✅ Normal"

    # Crear tabla de resultados
    resultado = pd.DataFrame({
        "Fecha Actual": [fecha_actual],
        "Total Actual": [total_actual],
        "Fecha Anterior": [fecha_anterior],
        "Total Anterior": [total_anterior],
        "Diferencia %": [diferencia_porcentual],
        "Alerta": [alerta]
    })

    # Mostrar la tabla en consola
    print(resultado.to_string(index=False))