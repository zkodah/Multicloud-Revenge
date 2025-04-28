import datetime
import os
from GTDReport.Email import Sender
import TablaAWS  # Asegúrate de tenerlo importado

def enviar_correo():
    sender = Sender(
        sender="automatizacionyconsistenciadedatos@grupogtd.com",
        sender_name="Reportería de Costos AWS",
        team=None,
        management="Gerencia Corporativa de Tecnología y Operaciones"
    )

    destinatarios = ["multicloud.gtd@grupogtd.com"]
    copias = ["manuel.luque@grupogtd.com"]
    fecha = datetime.datetime.today().strftime('%Y-%m-%d')

    print("\n Recuperando datos actualizados para AWS...")

    df_aws = TablaAWS.obtener_datos_aws()
    tabla_aws = TablaAWS.generar_tabla_aws(df_aws)

    if tabla_aws is None or tabla_aws.empty:
        print(" No se pudieron obtener datos de AWS.")
        return

    print("\n  Tabla de AWS generada con éxito.")

    if fecha in tabla_aws.columns:
        total_aws = tabla_aws[fecha].sum()
    else:
        total_aws = 0

    asunto = f"Costos de AWS {fecha} - Total: {total_aws:,.2f}"

    tablas_html = "<h3>Resumen de Costos AWS</h3>"
    tablas_html += tabla_aws.to_html(index=False, border=1)

    cuerpo_html = f"""
    <html>
    <body>
        <p>Adjunto los costos de AWS por día.</p>
        {tablas_html}
    </body>
    </html>
    """

    sender.send(
        to=destinatarios,
        cc=copias,
        subject=asunto,
        html_body=cuerpo_html,
        attachment=[]  # No hay adjuntos en este caso
    )

    print(" Correo de costos AWS enviado exitosamente.")


