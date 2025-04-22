import pandas as pd
import smtplib
from sqlalchemy import create_engine
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import key_gcp
from key_gcp import MAIL_CONFIG

# Configurar conexión con PostgreSQL
DB_URL = f"postgresql://{key_gcp.DB_CONFIG['DB_USER']}:{key_gcp.DB_CONFIG['DB_PASSWORD']}@{key_gcp.DB_CONFIG['DB_HOST']}:{key_gcp.DB_CONFIG['DB_PORT']}/{key_gcp.DB_CONFIG['DB_NAME']}"
engine = create_engine(DB_URL)

# Establecer conexión con PostgreSQL
#conn = psycopg2.connect(DB_URL)
#cursor = conn.cursor()

# Obtener la fecha de hoy
today = datetime.today()

# Definir el rango de las últimas dos semanas
start_of_two_weeks_ago = today - timedelta(days=14)
start_of_two_weeks_ago_str = start_of_two_weeks_ago.strftime('%Y-%m-%d')

# Consultar la diferencia porcentual entre los últimos dos días
query_diferencia = """
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
df_diferencia = pd.read_sql(query_diferencia, engine)

# Verificar si hay al menos 2 registros para calcular la diferencia porcentual
if len(df_diferencia) < 2:
    print("No hay suficientes datos para calcular la diferencia porcentual.")
else:
    # Extraer valores
    fecha_actual, total_actual = df_diferencia.iloc[0]
    fecha_anterior, total_anterior = df_diferencia.iloc[1]

    # Calcular la diferencia porcentual
    if total_anterior == 0:
        diferencia_porcentual = None
        alerta = "N/A"
    else:
        diferencia_porcentual = round(((total_actual - total_anterior) / total_anterior) * 100, 2)
        alerta = "⚠️ ¡ALERTA!" if abs(diferencia_porcentual) > 50 else "✅ Normal"

    # Crear tabla de resultados
    resultado_diferencia = pd.DataFrame({
        "Fecha Actual": [fecha_actual],
        "Total Actual": [total_actual],
        "Fecha Anterior": [fecha_anterior],
        "Total Anterior": [total_anterior],
        "Diferencia %": [diferencia_porcentual],
        "Alerta": [alerta]
    })

# Consultar la tabla con proyectos y fechas
query_proyectos = """
WITH fechas AS (
    SELECT GENERATE_SERIES(
        CURRENT_DATE - INTERVAL '7 days',
        CURRENT_DATE - INTERVAL '1 day',
        INTERVAL '1 day'
    )::DATE AS fecha
)
SELECT 
    f.fecha,
    COALESCE(c.project_name, 'Otros Costos') AS project_name,
    ROUND(COALESCE(SUM(c.subtotal), 0)::NUMERIC, 2) AS total_subtotal
FROM fechas f
LEFT JOIN gcp_costos_diarios c ON f.fecha = c.fecha
GROUP BY f.fecha, COALESCE(c.project_name, 'Otros Costos')
ORDER BY f.fecha DESC, total_subtotal DESC;
"""

# Ejecutar la consulta para obtener los detalles por proyecto
#conn = psycopg2.connect(DB_URL)
#df_proyectos = pd.read_sql(query_proyectos, conn)
df_proyectos = pd.read_sql(query_proyectos, engine)

# Convertir la columna 'fecha' a datetime
df_proyectos['fecha'] = pd.to_datetime(df_proyectos['fecha'], errors='coerce')

# Convertir la tabla a formato ancho (pivotada)
df_proyectos_pivot = df_proyectos.pivot_table(index="project_name", columns="fecha", values="total_subtotal", fill_value=0)

# Ordenar las columnas por fecha (de más antigua a más reciente)
df_proyectos_pivot = df_proyectos_pivot.sort_index(axis=1, ascending=True)

# Convertir las fechas nuevamente a formato string para que se muestren correctamente
df_proyectos_pivot.columns = df_proyectos_pivot.columns.strftime('%Y-%m-%d')

# Si no quieres la palabra 'fecha' en el nombre de las columnas, puedes renombrarlas así:
df_proyectos_pivot.columns = [col.replace('fecha', '') for col in df_proyectos_pivot.columns]

# Restablecer índice para mejor visualización
df_proyectos_pivot.reset_index(inplace=True)


# Crear el cuerpo del correo con ambas tablas
html_diferencia = resultado_diferencia.to_html(index=False, border=1)
html_proyectos = df_proyectos_pivot.to_html(index=False, border=1)

email_body = f"""
<html>
<head>
    <style>
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid black; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <h2>Costos GCP</h2>
    <h3>Diferencia por Fecha</h3>
    {html_diferencia}
    <h3>Detalle por Proyecto</h3>
    {html_proyectos}
</body>
</html>
"""

# Usar los datos del archivo de configuración para el servidor de correo
server = smtplib.SMTP(MAIL_CONFIG['MAIL_SERVER'], MAIL_CONFIG['MAIL_PORT'])
server.starttls()
server.login(MAIL_CONFIG['MAIL_USERNAME'], MAIL_CONFIG['MAIL_PASSWORD'])

# Función para enviar el correo
def send_email():
    msg = MIMEMultipart()
    msg["From"] = MAIL_CONFIG['MAIL_USERNAME']
    msg["To"] = 'multicloud.gtd@grupogtd.com'
    msg["Subject"] = "Costos Diario GCP"

    msg.attach(MIMEText(email_body, "html"))

    # Enviar el correo
    server.sendmail(MAIL_CONFIG['MAIL_USERNAME'], 'multicloud.gtd@grupogtd.com', msg.as_string())

    #print("✅ Correo enviado correctamente.")
    server.quit()

# Enviar el correo
send_email()
