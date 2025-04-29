import datetime
import os
from GTDReport.Email import Sender
import TablaAWS  # Asegúrate de tenerlo importado
import pandas as pd

def enviar_correo():
    sender = Sender(
        sender="automatizacionyconsistenciadedatos@grupogtd.com",
        sender_name="Reportería de Costos AWS",
        team=None,
        management="Gerencia Corporativa de Tecnología y Operaciones"
    )

    destinatarios = ["multicloud.gtd@grupogtd.com"] #multicloud.gtd@grupogtd.com
    copias = ["manuel.luque@grupogtd.com"]
    fecha = datetime.datetime.today().strftime('%Y-%m-%d')

    print("\n Recuperando datos AWS y generando pivot de 7 días…")
    df_aws = TablaAWS.obtener_datos_aws()
    tabla_7d = TablaAWS.generar_tabla_aws(df_aws, days=7)
    if tabla_7d is None or tabla_7d.empty:
        print("No se pudieron generar los últimos 7 días de costos.")
        return

    print(" Generando tabla de diferencia hoy vs ayer…")
    tabla_diff_simple = TablaAWS.exportar_diferencias()


    if tabla_diff_simple is None or tabla_diff_simple.empty:
        print("No hay datos suficientes para calcular la diferencia.")
        return

    total_hoy = tabla_7d.get(datetime.datetime.today().date(), pd.Series([0])).sum()

    asunto = f"Costos AWS {fecha} | Total hoy: ${total_hoy:,.2f}"

    html = f"""
    <html><body>
      <p>Estimado equipo,</p>
      <p>A continuación el detalle de costos AWS:</p>

      <h3> Costos de AWS </h3>
      {tabla_7d.to_html(index=False, border=1)}

      <h3> Resumen de AWS {fecha} </h3>
      {tabla_diff_simple.to_html(index=False, border=1)}

      <p>Saludos,<br/>Reportería de Costos AWS</p>
    </body></html>
    """

    sender.send(
        to=destinatarios,
        cc=copias,
        subject=asunto,
        html_body=html,
        attachment=[]
    )
    print(" Correo de costos AWS enviado exitosamente.")