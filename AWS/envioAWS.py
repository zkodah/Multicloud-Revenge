
import smtplib
import pandas as pd
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from sqlalchemy import create_engine
import TablaAWS


def enviar_correo():
    remitente = "elasticsearch@grupogtd.com"
    destinatarios = ["multicloud.gtd@grupogtd.com"]
    copias = ["manuel.luque@grupogtd.com"]	
    fecha = datetime.datetime.today().strftime('%Y-%m-%d')

    # Obteniendo datos de AWS
    df_aws = TablaAWS.obtener_datos_aws()
    tabla_aws = TablaAWS.generar_tabla_aws(df_aws)

    if tabla_aws is None or tabla_aws.empty:
        print(" No se pudieron obtener datos de AWS.")
        return
    
    print(" Tabla de AWS generada con éxito.")
    if fecha in tabla_aws.columns:
        total_aws = tabla_aws[fecha].sum()
    else:
        total_aws = 0


    asunto = f"Costos de AWS {fecha} - Total: {total_aws:,.2f}"

    # Solo la tabla consolidada
    tablas_html = "<h3>Resumen de Costos - Total</h3>"
    tablas_html += tabla_aws.to_html(index=False, border=1)

    # Construcción del cuerpo del correo
    cuerpo_html = f"""
    <html>
    <body>
        <p>Adjunto los costos de AWS por día.</p>
        {tablas_html}
    </body>
    </html>
    """

    smtp_username = "elasticsearch@grupogtd.com"
    smtp_password = "H9vE]cLg(m$RhyhJvz6T9[W,m"

    mensaje = MIMEMultipart()
    mensaje["From"] = remitente
    mensaje["To"] = ", ".join(destinatarios)
    mensaje["Cc"] = ", ".join(copias)
    mensaje["Subject"] = asunto
    mensaje.attach(MIMEText(cuerpo_html, "html"))


    try:
        with smtplib.SMTP('smtp.office365.com', 587) as sesion_smtp:
            sesion_smtp.starttls()
            sesion_smtp.login(smtp_username, smtp_password)
            sesion_smtp.sendmail(remitente, destinatarios + copias, mensaje.as_string())
        print(" Correo enviado exitosamente.")
    except smtplib.SMTPAuthenticationError:
        print(" Error: Autenticación fallida. Verifica usuario y contraseña SMTP.")
    except Exception as e:
        print(f" Error al enviar el correo: {str(e)}")


