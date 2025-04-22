from google.cloud import bigquery
import pandas as pd
import os
import key_gcp
from sqlalchemy import create_engine

# Configurar conexión con PostgreSQL
DB_URL = f"postgresql://{key_gcp.DB_CONFIG['DB_USER']}:{key_gcp.DB_CONFIG['DB_PASSWORD']}@{key_gcp.DB_CONFIG['DB_HOST']}:{key_gcp.DB_CONFIG['DB_PORT']}/{key_gcp.DB_CONFIG['DB_NAME']}"
engine = create_engine(DB_URL)

# Configurar credenciales de GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./gtd-prj-billing-001-508c7bcbda9d.json"

# Configurar el proyecto de GCP
PROJECT_ID = "gtd-prj-billing-001"

# Consulta SQL para obtener costos de los últimos 7 días
QUERY = """

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
  AND usage_start_time >= TIMESTAMP(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH), "US/Pacific")
  AND usage_start_time < TIMESTAMP(DATE_TRUNC(CURRENT_DATE(), MONTH), "US/Pacific")
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

# Evitar notación científica en 'cost'
df["costo"] = df["costo"].map(lambda x: f"{x:.6f}")
df["descuentos"] = df["descuentos"].map(lambda x: f"{x:.6f}")
df["promociones_y_otros"] = df["promociones_y_otros"].map(lambda x: f"{x:.6f}")
df["subtotal"] = df["subtotal"].map(lambda x: f"{x:.6f}")

# Insertar en PostgreSQL
df.to_sql("gcp_costos_mensuales", engine, if_exists="append", index=False)

# Guardar CSV
#csv_filename = "gcp_costos.csv"
#df.to_csv(csv_filename, index=False, decimal=".")

print("Datos insertados correctamente en PostgreSQL.")