'''

from google.cloud import bigquery
import pandas as pd
import os
import key_gcp
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone


# Configurar conexión con PostgreSQL
DB_URL = f"postgresql://{key_gcp.DB_CONFIG['DB_USER']}:{key_gcp.DB_CONFIG['DB_PASSWORD']}@{key_gcp.DB_CONFIG['DB_HOST']}:{key_gcp.DB_CONFIG['DB_PORT']}/{key_gcp.DB_CONFIG['DB_NAME']}"
engine = create_engine(DB_URL)

# Configurar credenciales de GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/mluque/Proyecto/gcp/gtd-prj-billing-001-508c7bcbda9d.json"

# Configurar el proyecto de GCP
PROJECT_ID = "gtd-prj-billing-001"

# Calcular la fecha de ayer
#yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
#from datetime import datetime, timedelta, timezone

yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")



# Consulta SQL para obtener costos del día de ayer
QUERY = f"""
SELECT
  DATE(TIMESTAMP_TRUNC(usage_start_time, Day, 'US/Pacific')) AS `fecha`,
  project.name AS `project_name`,
  project.id AS `project_id`,
  project.number AS `project_number`,
  service.description AS `servicio`,
  SUM(CAST(cost AS NUMERIC)) AS `costo`,
  SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('SUSTAINED_USAGE_DISCOUNT',
          'DISCOUNT',
          'SPENDING_BASED_DISCOUNT',
          'COMMITTED_USAGE_DISCOUNT',
          'FREE_TIER',
          'COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE',
          'SUBSCRIPTION_BENEFIT',
          'RESELLER_MARGIN',
          'FEE_UTILIZATION_OFFSET')), 0)) AS `descuentos`,
  SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('CREDIT_TYPE_UNSPECIFIED',
          'PROMOTION')), 0)) AS `promociones_y_otros`,
  SUM(CAST(cost AS NUMERIC)) + SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('SUSTAINED_USAGE_DISCOUNT',
          'DISCOUNT',
          'SPENDING_BASED_DISCOUNT',
          'COMMITTED_USAGE_DISCOUNT',
          'FREE_TIER',
          'COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE',
          'SUBSCRIPTION_BENEFIT',
          'RESELLER_MARGIN',
          'FEE_UTILIZATION_OFFSET')), 0)) + SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('CREDIT_TYPE_UNSPECIFIED',
          'PROMOTION')), 0)) AS `subtotal`
FROM
  `gtd-prj-billing-001.all_billing_data.gcp_billing_export_resource_v1_01B1BF_B3DCC5_95B4E2`
WHERE
  cost_type != 'tax'
  AND cost_type != 'adjustment'
  AND DATE(TIMESTAMP_TRUNC(usage_start_time, Day, 'US/Pacific')) = "{yesterday}"

GROUP BY
  fecha,
  project.name,
  project.id,
  project.number,
  service.description
ORDER BY
  fecha DESC,
  subtotal DESC
"""

# Inicializar cliente de BigQuery
client = bigquery.Client(project=PROJECT_ID)
query_job = client.query(QUERY)

# Convertir los resultados a DataFrame
df = query_job.to_dataframe()

# Evitar notación científica en columnas numéricas
df["costo"] = df["costo"].map(lambda x: f"{x:.6f}")
df["descuentos"] = df["descuentos"].map(lambda x: f"{x:.6f}")
df["promociones_y_otros"] = df["promociones_y_otros"].map(lambda x: f"{x:.6f}")
df["subtotal"] = df["subtotal"].map(lambda x: f"{x:.6f}")

# Insertar en PostgreSQL
df.to_sql("gcp_costos_diarios", engine, if_exists="append", index=False)

#print(f"Datos del {yesterday} insertados correctamente en PostgreSQL.")

'''

from google.cloud import bigquery
import pandas as pd
import os
import key_gcp
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone


# Configurar conexión con PostgreSQL
DB_URL = f"postgresql://{key_gcp.DB_CONFIG['DB_USER']}:{key_gcp.DB_CONFIG['DB_PASSWORD']}@{key_gcp.DB_CONFIG['DB_HOST']}:{key_gcp.DB_CONFIG['DB_PORT']}/{key_gcp.DB_CONFIG['DB_NAME']}"
engine = create_engine(DB_URL)

# Configurar credenciales de GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/mluque/Proyecto/gcp/gtd-prj-billing-001-508c7bcbda9d.json"

# Configurar el proyecto de GCP
PROJECT_ID = "gtd-prj-billing-001"

# Calcular la fecha de ayer
yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

# Consulta SQL para obtener costos del día de ayer
QUERY = f"""
SELECT
  DATE(TIMESTAMP_TRUNC(usage_start_time, Day, 'US/Pacific')) AS `fecha`,
  project.name AS `project_name`,
  project.id AS `project_id`,
  project.number AS `project_number`,
  service.description AS `servicio`,
  SUM(CAST(cost AS NUMERIC)) AS `costo`,
  SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('SUSTAINED_USAGE_DISCOUNT',
          'DISCOUNT',
          'SPENDING_BASED_DISCOUNT',
          'COMMITTED_USAGE_DISCOUNT',
          'FREE_TIER',
          'COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE',
          'SUBSCRIPTION_BENEFIT',
          'RESELLER_MARGIN',
          'FEE_UTILIZATION_OFFSET')), 0)) AS `descuentos`,
  SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('CREDIT_TYPE_UNSPECIFIED',
          'PROMOTION')), 0)) AS `promociones_y_otros`,
  SUM(CAST(cost AS NUMERIC)) + SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('SUSTAINED_USAGE_DISCOUNT',
          'DISCOUNT',
          'SPENDING_BASED_DISCOUNT',
          'COMMITTED_USAGE_DISCOUNT',
          'FREE_TIER',
          'COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE',
          'SUBSCRIPTION_BENEFIT',
          'RESELLER_MARGIN',
          'FEE_UTILIZATION_OFFSET')), 0)) + SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('CREDIT_TYPE_UNSPECIFIED',
          'PROMOTION')), 0)) AS `subtotal`
FROM
  `gtd-prj-billing-001.all_billing_data.gcp_billing_export_resource_v1_01B1BF_B3DCC5_95B4E2`
WHERE
  cost_type != 'tax'
  AND cost_type != 'adjustment'
  AND DATE(TIMESTAMP_TRUNC(usage_start_time, Day, 'US/Pacific')) = "{yesterday}"

GROUP BY
  fecha,
  project.name,
  project.id,
  project.number,
  service.description
ORDER BY
  fecha DESC,
  subtotal DESC
"""

# Inicializar cliente de BigQuery
client = bigquery.Client(project=PROJECT_ID)
query_job = client.query(QUERY)

# Convertir los resultados a DataFrame
new_data_df = query_job.to_dataframe()

# Si no hay datos para el día consultado, terminar el script
if new_data_df.empty:
    print(f"No hay datos de costos para el {yesterday}")
    exit()

# Función para procesar cada fila
def process_data(engine, df):
    with engine.connect() as conn:
        # Para cada fila en el dataframe
        for index, row in df.iterrows():
            # Verificar si ya existe este registro en la base de datos
            check_query = text("""
                SELECT fecha, costo, descuentos, promociones_y_otros, subtotal 
                FROM gcp_costos_diarios 
                WHERE fecha = :fecha 
                AND project_id = :project_id 
                AND servicio = :servicio
            """)
            
            # Ejecutar consulta para verificar existencia
            result = conn.execute(check_query, {
                'fecha': row['fecha'],
                'project_id': row['project_id'],
                'servicio': row['servicio']
            }).fetchone()
            
            # Convertir valores numéricos (están como string en el dataframe)
            costo = float(row['costo'])
            descuentos = float(row['descuentos'])
            promociones_y_otros = float(row['promociones_y_otros'])
            subtotal = float(row['subtotal'])
            
            if result:
                # El registro existe, actualizar
                # Calculamos diferencias para posible actualización
                db_costo = float(result[1])
                db_descuentos = float(result[2])
                db_promociones_y_otros = float(result[3])
                db_subtotal = float(result[4])
                
                # Actualizar solo si los valores son diferentes
                if (costo != db_costo or 
                    descuentos != db_descuentos or 
                    promociones_y_otros != db_promociones_y_otros or
                    subtotal != db_subtotal):
                    
                    update_query = text("""
                        UPDATE gcp_costos_diarios
                        SET costo = :costo,
                            descuentos = :descuentos,
                            promociones_y_otros = :promociones_y_otros,
                            subtotal = :subtotal
                        WHERE fecha = :fecha
                        AND project_id = :project_id
                        AND servicio = :servicio
                    """)
                    
                    conn.execute(update_query, {
                        'fecha': row['fecha'],
                        'project_id': row['project_id'],
                        'servicio': row['servicio'],
                        'costo': f"{costo:.6f}",
                        'descuentos': f"{descuentos:.6f}",
                        'promociones_y_otros': f"{promociones_y_otros:.6f}",
                        'subtotal': f"{subtotal:.6f}"
                    })
                    print(f"Registro actualizado para fecha: {row['fecha']}, proyecto: {row['project_name']}, servicio: {row['servicio']}")
            else:
                # El registro no existe, insertar nuevo
                insert_query = text("""
                    INSERT INTO gcp_costos_diarios (
                        fecha, project_name, project_id, project_number, 
                        servicio, costo, descuentos, promociones_y_otros, subtotal
                    ) VALUES (
                        :fecha, :project_name, :project_id, :project_number,
                        :servicio, :costo, :descuentos, :promociones_y_otros, :subtotal
                    )
                """)
                
                conn.execute(insert_query, {
                    'fecha': row['fecha'],
                    'project_name': row['project_name'],
                    'project_id': row['project_id'],
                    'project_number': row['project_number'],
                    'servicio': row['servicio'],
                    'costo': f"{costo:.6f}",
                    'descuentos': f"{descuentos:.6f}",
                    'promociones_y_otros': f"{promociones_y_otros:.6f}",
                    'subtotal': f"{subtotal:.6f}"
                })
                print(f"Nuevo registro insertado para fecha: {row['fecha']}, proyecto: {row['project_name']}, servicio: {row['servicio']}")
        
        # Confirmar transacción
        conn.commit()

# Evitar notación científica en columnas numéricas antes de procesar
new_data_df["costo"] = new_data_df["costo"].map(lambda x: f"{x:.6f}")
new_data_df["descuentos"] = new_data_df["descuentos"].map(lambda x: f"{x:.6f}")
new_data_df["promociones_y_otros"] = new_data_df["promociones_y_otros"].map(lambda x: f"{x:.6f}")
new_data_df["subtotal"] = new_data_df["subtotal"].map(lambda x: f"{x:.6f}")

# Procesar los datos
process_data(engine, new_data_df)

print(f"Procesamiento de datos para {yesterday} completado.")
