import os
import smtplib
import pandas as pd
import datetime
import numpy as np
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from sqlalchemy import create_engine
from fpdf import FPDF
from PIL import Image
import Key 
import Tabla2


def generar_pdf_imagenes(archivos_adjuntos, nombre_pdf):
    pdf = FPDF()
    for img_path in archivos_adjuntos:
        try:
            image = Image.open(img_path)
            pdf.add_page()
            pdf.image(img_path, x=10, y=10, w=190)
        except Exception as e:
            print(f" Error al agregar imagen {img_path} al PDF: {str(e)}")
    pdf.output(nombre_pdf, "F")
    return nombre_pdf if os.path.exists(nombre_pdf) else None

# Función para enviar el correo
def enviar_correo():
    remitente = "elasticsearch@grupogtd.com"
    destinatarios = ["multicloud.gtd@grupogtd.com"]
    copias = ["manuel.luque@grupogtd.com"]
    fecha = datetime.datetime.today().strftime('%Y-%m-%d')

    print("\n Recuperando datos actualizados para la tabla...")  
    df = Tabla2.obtener_datos()
    tabla_resultado = Tabla2.generar_tabla(df)
    
    if tabla_resultado is None or tabla_resultado.empty:
        print(" No se pudo generar la tabla.")
        return
    
    print("\n Tabla generada con éxito, incluyendo los datos de hoy.")

    if fecha in tabla_resultado.columns:
        total_hoy = tabla_resultado[fecha].sum()
    else:
        total_hoy = 0

    asunto = f"Costos de Azure {fecha} "

    #dividir la tabla por el "partnername
    tablas_html = ""
    for partner in tabla_resultado["partnername"].unique():
        tabla_partner = tabla_resultado[tabla_resultado["partnername"] == partner]
        tablas_html += f"<h3>Resumen de Costos - {partner}</h3>"
        tablas_html += tabla_partner.to_html(index=False, border=1)

    cuerpo_html = f"""
    <html>
    <body>
        <p>Adjunto los costos de Azure por día, organizados por Partner y Cliente.</p>
        <h3>Resumen de Costos AZURE</h3>
        {tablas_html}
    </body>
    </html>
    """

    carpeta_adjuntos = "graficos_clientes"
    smtp_username = "elasticsearch@grupogtd.com"
    smtp_password = "H9vE]cLg(m$RhyhJvz6T9[W,m"

    ruta_carpeta = os.path.abspath(carpeta_adjuntos)
    if not os.path.exists(ruta_carpeta):
        print(f"La carpeta {ruta_carpeta} no existe.")
        return
    
    archivos_adjuntos = []
    for partner_folder in os.listdir(ruta_carpeta):
        partner_path = os.path.join(ruta_carpeta, partner_folder)
        if os.path.isdir(partner_path):
            archivos_adjuntos.extend(
                os.path.join(partner_path, f) for f in os.listdir(partner_path) if f.lower().endswith(".jpg")
            )
    
    print(f"📎 Encontrados {len(archivos_adjuntos)} imágenes para generar el PDF.")
    
    # Generar PDF con las imágenes
    nombre_pdf = f"Costos_Azure_{fecha}.pdf"
    ruta_pdf = os.path.join(ruta_carpeta, nombre_pdf)
    pdf_generado = generar_pdf_imagenes(archivos_adjuntos, ruta_pdf)
    
    mensaje = MIMEMultipart()
    mensaje["From"] = remitente
    mensaje["To"] = ", ".join(destinatarios)
    mensaje["Cc"] = ", ".join(copias)
    mensaje["Subject"] = asunto
    mensaje.attach(MIMEText(cuerpo_html, "html"))
    
    # Adjuntar el PDF generado
    if pdf_generado:
        try:
            with open(ruta_pdf, "rb") as adjunto:
                adjunto_mime = MIMEBase("application", "pdf")
                adjunto_mime.set_payload(adjunto.read())
            encoders.encode_base64(adjunto_mime)
            adjunto_mime.add_header("Content-Disposition", f"attachment; filename={nombre_pdf}")
            mensaje.attach(adjunto_mime)
            print("✅ PDF adjuntado correctamente.")
        except Exception as e:
            print(f"🚨 Error al adjuntar el PDF {nombre_pdf}: {str(e)}")
    
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