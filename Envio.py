from GTDReport.Email import Sender
import os
import pandas as pd
import datetime
import numpy as np
from sqlalchemy import create_engine
from fpdf import FPDF
from PIL import Image
import Key 
import Tabla2
os.environ["POWER_AUTOMATE_ENDPOINT"] = "https://prod-10.brazilsouth.logic.azure.com:443/workflows/0905dfd89eca4839afb5543506ffc668/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=Xrr4awVOx4RIHzNGNtxBc4wbDKSkuzvvm_NYgRdlGCc" 


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


def enviar_correo():
    sender = Sender(
        sender = "automatizacionyconsistenciadedatos@grupogtd.com",
        sender_name = "Reportería e Informes",
        team = None,
        management = "Gerencia Corporativa de Tecnología y Operaciones"
    )
    
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

    print(f"📎 Encontradas {len(archivos_adjuntos)} imágenes para generar el PDF.")
    nombre_pdf = f"Costos_Azure_{fecha}.pdf"
    ruta_pdf = os.path.join(ruta_carpeta, nombre_pdf)
    pdf_generado = generar_pdf_imagenes(archivos_adjuntos, ruta_pdf)

    sender.send(
        to = destinatarios,
        cc = copias,
        subject = asunto,
        html_body = cuerpo_html,
        attachment = [ruta_pdf] if pdf_generado else []
    )

    print("✅ Correo enviado exitosamente con la clase Sender.")

