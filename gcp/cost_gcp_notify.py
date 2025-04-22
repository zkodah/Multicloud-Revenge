import psycopg2
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# 🔹 Configuración de la conexión a PostgreSQL
DB_CONFIG = {
    "dbname": "gcp",
    "user": "postgres",
    "password": "santy741",
    "host": "localhost",
    "port": "5432"
}

# 🔹 Consulta 1: Total general por mes
QUERY_TOTAL_GENERAL = """
SELECT 
    TO_CHAR(MAX(c.fecha), 'TMMonth') AS mes,
    ROUND(SUM(c.costo)::NUMERIC, 2) AS total_costo,
    ROUND(SUM(c.descuentos)::NUMERIC, 2) AS total_descuento,
    ROUND(SUM(c.promociones_y_otros)::NUMERIC, 2) AS total_promocion,
    ROUND(SUM(c.subtotal)::NUMERIC, 2) AS total_subtotal
FROM gcp_costos_mensuales c
JOIN gcp_responsables p ON c.project_id = p.project_id
WHERE p.responsable = 'Jorge Polanco';
"""

# 🔹 Consulta 2: Detalle por proyecto
QUERY_DETALLE_PROYECTO = """
SELECT 
    CASE 
        WHEN c.project_name IS NULL THEN 'TOTAL GENERAL' 
        ELSE c.project_name 
    END AS project_name,
    p.responsable,
    p.email,
    TO_CHAR(c.fecha, 'TMMonth') AS mes,
    ROUND(SUM(c.costo)::NUMERIC, 2) AS total_costo,
    ROUND(SUM(c.descuentos)::NUMERIC, 2) AS total_descuento,
    ROUND(SUM(c.promociones_y_otros)::NUMERIC, 2) AS total_promocion,
    ROUND(SUM(c.subtotal)::NUMERIC, 2) AS total_subtotal
FROM gcp_costos_mensuales c
JOIN gcp_responsables p ON c.project_id = p.project_id
WHERE p.responsable = 'Jorge Polanco'
GROUP BY ROLLUP ((c.project_name, p.responsable, p.email, TO_CHAR(c.fecha, 'TMMonth')))
ORDER BY 
    CASE WHEN c.project_name IS NULL THEN 1 ELSE 0 END, 
    mes DESC, 
    project_name NULLS LAST, 
    responsable NULLS LAST;
"""

# 🔹 Función para ejecutar las consultas y devolver DataFrames
def fetch_data(query):
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# 🔹 Obtener los datos
df_total_general = fetch_data(QUERY_TOTAL_GENERAL)
df_detalle_proyecto = fetch_data(QUERY_DETALLE_PROYECTO)

# 🔹 Convertir los DataFrames a tablas HTML
html_total_general = df_total_general.to_html(index=False, border=1)
html_detalle_proyecto = df_detalle_proyecto.to_html(index=False, border=1)

# 🔹 Configuración del correo
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587
EMAIL_SENDER = "elasticsearch@grupogtd.com"
EMAIL_PASSWORD = "H9vE]cLg(m$RhyhJvz6T9[W,m"
EMAIL_RECIPIENT = "fabian.ramirez@grupogtd.com"

# 🔹 Construir el cuerpo del mensaje
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
    <h2>Estado de Cuenta Mensual</h2>
    <h3>Total General</h3>
    {html_total_general}
    <h3>Detalle por Proyecto</h3>
    {html_detalle_proyecto}
</body>
</html>
"""

# 🔹 Enviar el correo
def send_email():
    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECIPIENT
    msg["Subject"] = "Estado de Cuenta Mensual"

    msg.attach(MIMEText(email_body, "html"))

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(EMAIL_SENDER, EMAIL_PASSWORD)
    server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())
    server.quit()

# 🚀 Ejecutar el envío del correo
send_email()

print("✅ Correo enviado correctamente.")
