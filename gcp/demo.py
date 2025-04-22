import pandas as pd
from sqlalchemy import create_engine
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
import key_gcp
import smtplib
import math
import os

# Database connection configuration
DB_URL = f"postgresql://{key_gcp.DB_CONFIG['DB_USER']}:{key_gcp.DB_CONFIG['DB_PASSWORD']}@{key_gcp.DB_CONFIG['DB_HOST']}:{key_gcp.DB_CONFIG['DB_PORT']}/{key_gcp.DB_CONFIG['DB_NAME']}"
engine = create_engine(DB_URL)

def obtener_fechas_semanas():
    hoy = datetime.now()
    
    # Encontrar el inicio de la semana actual (lunes)
    inicio_semana_actual = hoy - timedelta(days=hoy.weekday())
    
    # Retroceder al final de la semana anterior completa (domingo)
    fin_semana_anterior = inicio_semana_actual - timedelta(days=1)
    
    # Inicio de la semana anterior completa (lunes)
    inicio_semana_anterior = fin_semana_anterior - timedelta(days=6)
    
    # Final de la semana anterior a la anterior (domingo)
    fin_semana_anterior2 = inicio_semana_anterior - timedelta(days=1)
    
    # Inicio de la semana anterior a la anterior (lunes)
    inicio_semana_anterior2 = fin_semana_anterior2 - timedelta(days=6)

    semana_anterior = f"{inicio_semana_anterior.day} al {fin_semana_anterior.day} {fin_semana_anterior.strftime('%B')}"
    semana_anterior2 = f"{inicio_semana_anterior2.day} al {fin_semana_anterior2.day} {fin_semana_anterior2.strftime('%B')}"

    return (
        semana_anterior,  # Esto era anteriormente "semana_actual" 
        semana_anterior2, # Esto era anteriormente "semana_anterior"
        inicio_semana_anterior.strftime('%Y-%m-%d'), 
        fin_semana_anterior.strftime('%Y-%m-%d'),
        inicio_semana_anterior2.strftime('%Y-%m-%d'), 
        fin_semana_anterior2.strftime('%Y-%m-%d')
    )


'''
def obtener_fechas_semanas():
    hoy = datetime.now()
    fin_semana_actual = hoy - timedelta(days=hoy.weekday() + 1)
    inicio_semana_actual = fin_semana_actual - timedelta(days=6)
    fin_semana_anterior = inicio_semana_actual - timedelta(days=1)
    inicio_semana_anterior = fin_semana_anterior - timedelta(days=6)

    semana_actual = f"{inicio_semana_actual.day} al {fin_semana_actual.day} {fin_semana_actual.strftime('%B')}"
    semana_anterior = f"{inicio_semana_anterior.day} al {fin_semana_anterior.day} {fin_semana_anterior.strftime('%B')}"

    return (
        semana_actual, 
        semana_anterior, 
        inicio_semana_actual.strftime('%Y-%m-%d'), 
        fin_semana_actual.strftime('%Y-%m-%d'),
        inicio_semana_anterior.strftime('%Y-%m-%d'), 
        fin_semana_anterior.strftime('%Y-%m-%d')
    )
'''
'''
def obtener_datos():
    semana_actual_texto, semana_anterior_texto, inicio_actual, fin_actual, inicio_anterior, fin_anterior = obtener_fechas_semanas()

    # Get the first day of the current month
    primer_dia_mes = datetime.now().replace(day=1).strftime('%Y-%m-%d')
    dia_anterior = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    query = f"""
    WITH periodos AS (
        SELECT 
            COALESCE(c.project_name, 'Otros Costos') AS project_name,
            r.responsable,
            r.email,
            ROUND(SUM(CASE 
                WHEN c.fecha BETWEEN '{primer_dia_mes}' AND '{dia_anterior}' THEN c.subtotal 
                ELSE 0 
            END)::NUMERIC, 2) AS total_acumulado_mes,
            ROUND(SUM(CASE 
                WHEN c.fecha BETWEEN '{inicio_actual}' AND '{fin_actual}' THEN c.subtotal 
                ELSE 0 
            END)::NUMERIC, 2) AS total_costo_actual,
            ROUND(SUM(CASE 
                WHEN c.fecha BETWEEN '{inicio_anterior}' AND '{fin_anterior}' THEN c.subtotal 
                ELSE 0 
            END)::NUMERIC, 2) AS total_costo_anterior,
            ROUND(
                CASE 
                    WHEN SUM(CASE WHEN c.fecha BETWEEN '{inicio_anterior}' AND '{fin_anterior}' THEN c.subtotal ELSE 0 END) = 0 THEN NULL
                    ELSE ((SUM(CASE WHEN c.fecha BETWEEN '{inicio_actual}' AND '{fin_actual}' THEN c.subtotal ELSE 0 END) - 
                           SUM(CASE WHEN c.fecha BETWEEN '{inicio_anterior}' AND '{fin_anterior}' THEN c.subtotal ELSE 0 END)) / 
                          SUM(CASE WHEN c.fecha BETWEEN '{inicio_anterior}' AND '{fin_anterior}' THEN c.subtotal ELSE 0 END)) * 100 
                END, 2
            ) AS porcentaje_diferencia
        FROM gcp_costos_diarios c
        JOIN gcp_responsables r ON c.project_id = r.project_id
        WHERE c.fecha BETWEEN '{primer_dia_mes}' AND '{fin_actual}'
        GROUP BY c.project_name, r.responsable, r.email
    ),
    responsables_totales AS (
        SELECT 
            responsable,
            email,
            ROUND(SUM(total_acumulado_mes)::NUMERIC, 2) AS total_acumulado_mes
        FROM periodos
        GROUP BY responsable, email
    )
    SELECT 
        p.*,
        rt.total_acumulado_mes AS total_acumulado_responsable
    FROM periodos p
    JOIN responsables_totales rt ON p.responsable = rt.responsable AND p.email = rt.email
    ORDER BY p.responsable, p.project_name;
    """

    try:
        # Use SQLAlchemy engine to execute the query
        df = pd.read_sql(query, engine)
        
        return df, semana_actual_texto, semana_anterior_texto
    except Exception as e:
        print(f"Error al obtener datos: {e}")
        return None, None, None
'''

def obtener_datos():
    semana_actual_texto, semana_anterior_texto, inicio_actual, fin_actual, inicio_anterior, fin_anterior = obtener_fechas_semanas()

    # Calculamos el primer día del mes correspondiente a la fecha de inicio_anterior
    # (para asegurar que tengamos datos de ambas semanas)
    primer_dia_mes = datetime.strptime(inicio_anterior, '%Y-%m-%d').replace(day=1).strftime('%Y-%m-%d')
    dia_anterior = fin_actual  # Usamos fin_actual como último día

    print(f"Buscando datos desde {primer_dia_mes} hasta {dia_anterior}")
    print(f"Semana 1: {inicio_actual} al {fin_actual}")
    print(f"Semana 2: {inicio_anterior} al {fin_anterior}")

    query = f"""
    WITH periodos AS (
        SELECT 
            COALESCE(c.project_name, 'Otros Costos') AS project_name,
            r.responsable,
            r.email,
            ROUND(SUM(CASE 
                WHEN c.fecha BETWEEN '{primer_dia_mes}' AND '{dia_anterior}' THEN c.subtotal 
                ELSE 0 
            END)::NUMERIC, 2) AS total_acumulado_mes,
            ROUND(SUM(CASE 
                WHEN c.fecha BETWEEN '{inicio_actual}' AND '{fin_actual}' THEN c.subtotal 
                ELSE 0 
            END)::NUMERIC, 2) AS total_costo_actual,
            ROUND(SUM(CASE 
                WHEN c.fecha BETWEEN '{inicio_anterior}' AND '{fin_anterior}' THEN c.subtotal 
                ELSE 0 
            END)::NUMERIC, 2) AS total_costo_anterior,
            ROUND(
                CASE 
                    WHEN SUM(CASE WHEN c.fecha BETWEEN '{inicio_anterior}' AND '{fin_anterior}' THEN c.subtotal ELSE 0 END) = 0 THEN NULL
                    ELSE ((SUM(CASE WHEN c.fecha BETWEEN '{inicio_actual}' AND '{fin_actual}' THEN c.subtotal ELSE 0 END) - 
                           SUM(CASE WHEN c.fecha BETWEEN '{inicio_anterior}' AND '{fin_anterior}' THEN c.subtotal ELSE 0 END)) / 
                          SUM(CASE WHEN c.fecha BETWEEN '{inicio_anterior}' AND '{fin_anterior}' THEN c.subtotal ELSE 0 END)) * 100 
                END, 2
            ) AS porcentaje_diferencia
        FROM gcp_costos_diarios c
        JOIN gcp_responsables r ON c.project_id = r.project_id
        WHERE c.fecha BETWEEN '{inicio_anterior}' AND '{fin_actual}'
        GROUP BY c.project_name, r.responsable, r.email
    ),
    responsables_totales AS (
        SELECT 
            responsable,
            email,
            ROUND(SUM(total_acumulado_mes)::NUMERIC, 2) AS total_acumulado_mes
        FROM periodos
        GROUP BY responsable, email
    )
    SELECT 
        p.*,
        rt.total_acumulado_mes AS total_acumulado_responsable
    FROM periodos p
    JOIN responsables_totales rt ON p.responsable = rt.responsable AND p.email = rt.email
    ORDER BY p.responsable, p.project_name;
    """

    try:
        # Use SQLAlchemy engine to execute the query
        df = pd.read_sql(query, engine)
        
        if df.empty:
            print("La consulta no devolvió resultados. Verifica si hay datos para el periodo seleccionado.")
        else:
            print(f"Se encontraron {len(df)} registros.")
            
        return df, semana_actual_texto, semana_anterior_texto
    except Exception as e:
        print(f"Error al obtener datos: {e}")
        return None, None, None

def format_porcentaje(valor):
    """
    Formats percentage, converting NaN to 0%
    """
    import math
    if pd.isna(valor) or math.isnan(valor):
        return "0%"
    return f"{valor}%"

def generar_html_tabla(df, semana_actual, semana_anterior):
    # Calcular totales
    total_costo_acumulado = round(df['total_costo_actual'].sum(), 2)
    total_costo_anterior = round(df['total_costo_anterior'].sum(), 2)
    diferencia_porcentual = round(((total_costo_acumulado - total_costo_anterior) / total_costo_anterior) * 100, 2) if total_costo_anterior != 0 else 0

    # Estilo CSS para la tabla
    font_family = "Arial, sans-serif"
    html = f"""
        <html>
        <head>
            <style>
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    font-family: {font_family};
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: center;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
            </style>
        </head>
        <body>
            <h2>Reporte de Costos GCP</h2>
            <br>
            <table>
                <tr>
                    <td><strong>Total Costo Acumulado del Mes</strong></td>
                    <td>{df['total_acumulado_responsable'].iloc[0]}</td>
                </tr>
            </table>
            <br>
            <table>
                <tr>
                    <th>Semana</th>
                    <th>Total x Semana</th>
                    <th>Diferencia</th>
                </tr>
                <tr>
                    <td>{semana_actual}</td>
                    <td>{total_costo_acumulado}</td>
                    <td rowspan="2">{diferencia_porcentual}%</td>
                </tr>
                <tr>
                    <td>{semana_anterior}</td>
                    <td>{total_costo_anterior}</td>
                </tr>
            </table>
            <br>
            <table>
            <tr>
                <th>Proyecto</th>
                <th>Semana {semana_actual}</th>
                <th>Semana {semana_anterior}</th>
                <th>% Diferencia</th>
            </tr>
            {''.join([f"""
            <tr>
                <td>{row['project_name']}</td>
                <td>{row['total_costo_actual']}</td>
                <td>{row['total_costo_anterior']}</td>
                <td>{format_porcentaje(row['porcentaje_diferencia'])}</td>
                </tr>""" for _, row in df.iterrows()])}
            </table>
            
        </body>
        </html>
        """
    return html

def generar_csv_detalle_costos(responsable):
    """
    Generates a CSV file with detailed cost information for a specific responsible person's projects
    
    :param responsable: Name of the responsible person
    :return: Path of the generated CSV file
    """
    base_query = f"""
    (
        SELECT 
            c.project_name,
            COALESCE(p.responsable, 'SIN RESPONSABLE') AS responsable,
            c.servicio,
            ROUND(SUM(c.costo)::NUMERIC, 2) AS total_costo,
            ROUND(SUM(c.descuentos)::NUMERIC, 2) AS total_descuento,
            ROUND(SUM(c.promociones_y_otros)::NUMERIC, 2) AS total_promocion,
            ROUND(SUM(c.subtotal)::NUMERIC, 2) AS total_subtotal,
            1 AS orden
        FROM gcp_costos_mensuales c
        JOIN gcp_responsables p ON c.project_id = p.project_id
        WHERE p.responsable = '{responsable}'
        GROUP BY c.project_name, p.responsable, c.servicio
    )
    UNION ALL
    (
        SELECT 
            'TOTAL' AS project_name,
            'TOTAL' AS responsable,
            'TOTAL' AS servicio,
            ROUND(SUM(c.costo)::NUMERIC, 2) AS total_costo,
            ROUND(SUM(c.descuentos)::NUMERIC, 2) AS total_descuento,
            ROUND(SUM(c.promociones_y_otros)::NUMERIC, 2) AS total_promocion,
            ROUND(SUM(c.subtotal)::NUMERIC, 2) AS total_subtotal,
            2 AS orden
        FROM gcp_costos_mensuales c
        JOIN gcp_responsables p ON c.project_id = p.project_id
        WHERE p.responsable = '{responsable}'
    )
    ORDER BY orden, project_name NULLS LAST, responsable NULLS LAST, servicio NULLS LAST
    """

    try:
        # Execute the query and get the DataFrame
        df = pd.read_sql(base_query, engine)
        
        # Create a directory to store CSVs if it doesn't exist
        os.makedirs('reportes_costos', exist_ok=True)
        
        # Filename with current date
        fecha_actual = datetime.now().strftime("%Y%m%d")
        nombre_archivo = f'reportes_costos/detalle_costos_{fecha_actual}_{responsable}.csv'
        
        # Save the CSV
        df.to_csv(nombre_archivo, index=False, encoding='utf-8')
        
        print(f"CSV file generated: {nombre_archivo}")
        return nombre_archivo
    except Exception as e:
        print(f"Error generating cost details CSV: {e}")
        return None
'''
def enviar_correo(destinatario, asunto, html_contenido, archivo_adjunto=None):
    try:
        msg = MIMEMultipart('alternative')
        msg['From'] = key_gcp.MAIL_CONFIG["MAIL_USERNAME"]
        msg['To'] = destinatario
        msg['Subject'] = asunto

        # Convert HTML to MIMEText
        parte_html = MIMEText(html_contenido, 'html')
        msg.attach(parte_html)
        
        # Attach CSV file if it exists
        if archivo_adjunto and os.path.exists(archivo_adjunto):
            with open(archivo_adjunto, 'rb') as file:
                parte_csv = MIMEApplication(file.read(), _subtype='csv')
                parte_csv.add_header('Content-Disposition', 'attachment', filename=os.path.basename(archivo_adjunto))
                msg.attach(parte_csv)

        # Send email
        server = smtplib.SMTP(key_gcp.MAIL_CONFIG["MAIL_SERVER"], key_gcp.MAIL_CONFIG["MAIL_PORT"])
        server.starttls()
        server.login(key_gcp.MAIL_CONFIG["MAIL_USERNAME"], key_gcp.MAIL_CONFIG["MAIL_PASSWORD"])
        server.sendmail(key_gcp.MAIL_CONFIG["MAIL_USERNAME"], destinatario, msg.as_string())
        server.quit()
        print(f"Email sent to {destinatario}")
    except Exception as e:
        print(f"Error sending email to {destinatario}: {e}")

def generar_reportes_y_enviar():
    df, semana_actual, semana_anterior = obtener_datos()
    
    if df is not None and not df.empty:
        # Group by responsible person and email
        grouped = df.groupby(['responsable', 'email'])
        
        for (responsable, email), group_df in grouped:
            # Generate HTML table for the responsible person
            html_tabla = generar_html_tabla(group_df, semana_actual, semana_anterior)
            
            # Generate detailed CSV
            archivo_csv = generar_csv_detalle_costos(responsable)
            
            # Send email
            enviar_correo(
                email, 
                f"Reporte de Costos Plataforma GCP Semanal para {responsable}", 
                html_tabla,
                archivo_csv
            )
    else:
        print("No data found to generate reports")
'''

def generar_reportes_y_enviar():
    df, semana_actual, semana_anterior = obtener_datos()
    
    # Tu dirección de correo electrónico
    tu_email = "fabian.ramirez@grupogtd.com"  # Reemplaza esto con tu correo electrónico real
    
    if df is not None and not df.empty:
        # Generamos todos los reportes pero los enviamos en un solo correo
        
        # Primero generamos todos los archivos CSV
        archivos_csv = []
        html_contenido = "<h2>Reportes de Costos GCP para todos los responsables</h2><br>"
        
        # Group by responsible person
        grouped = df.groupby(['responsable', 'email'])
        
        for (responsable, _), group_df in grouped:
            # Generamos el HTML para este responsable
            html_tabla = generar_html_tabla(group_df, semana_actual, semana_anterior)
            html_contenido += f"<h3>Reporte para: {responsable}</h3>"
            html_contenido += html_tabla
            html_contenido += "<hr><br>"
            
            # Generamos el CSV para este responsable
            archivo_csv = generar_csv_detalle_costos(responsable)
            if archivo_csv:
                archivos_csv.append(archivo_csv)
        
        # Enviamos un solo correo con todo el contenido
        enviar_correo_multiple(
            tu_email,
            "Reporte Consolidado de Costos Plataforma GCP Semanal",
            html_contenido,
            archivos_csv
        )
    else:
        print("No data found to generate reports")

def enviar_correo_multiple(destinatario, asunto, html_contenido, archivos_adjuntos=None):
    try:
        msg = MIMEMultipart('alternative')
        msg['From'] = key_gcp.MAIL_CONFIG["MAIL_USERNAME"]
        msg['To'] = destinatario
        msg['Subject'] = asunto

        # Agregar pie de firma al contenido HTML
        pie_firma = """
        <hr>
        <p style="font-size: 12px; color: #666; margin-top: 20px;">
        Reporte generado automáticamente por área multicloud. 
        Si gusta conocer más del detalle de tus costos favor ingresa a la siguiente enlace 
        <a href="https://console.cloud.google.com/billing/01B1BF-B3DCC5-95B4E2?invt=AbttLw&organizationId=508520614573">Facturación Google</a>
        </p>
        """

        # Añadir el pie de firma al contenido HTML existente
        html_contenido_con_firma = html_contenido + pie_firma

        # Convert HTML to MIMEText
        parte_html = MIMEText(html_contenido_con_firma, 'html')
        msg.attach(parte_html)
        
        # Attach CSV files if they exist
        if archivos_adjuntos:
            for archivo in archivos_adjuntos:
                if os.path.exists(archivo):
                    with open(archivo, 'rb') as file:
                        parte_csv = MIMEApplication(file.read(), _subtype='csv')
                        parte_csv.add_header('Content-Disposition', 'attachment', 
                                           filename=os.path.basename(archivo))
                        msg.attach(parte_csv)

        # Send email
        server = smtplib.SMTP(key_gcp.MAIL_CONFIG["MAIL_SERVER"], key_gcp.MAIL_CONFIG["MAIL_PORT"])
        server.starttls()
        server.login(key_gcp.MAIL_CONFIG["MAIL_USERNAME"], key_gcp.MAIL_CONFIG["MAIL_PASSWORD"])
        server.sendmail(key_gcp.MAIL_CONFIG["MAIL_USERNAME"], destinatario, msg.as_string())
        server.quit()
        print(f"Email sent to {destinatario}")
    except Exception as e:
        print(f"Error sending email to {destinatario}: {e}")



# Execute the script
if __name__ == "__main__":
    generar_reportes_y_enviar()



