import pandas as pd
from sqlalchemy import create_engine
import key 


engine = create_engine(
    f"postgresql://{key.DB_CONFIG['user']}:{key.DB_CONFIG['password']}@{key.DB_CONFIG['host']}:{key.DB_CONFIG['port']}/{key.DB_CONFIG['dbname']}"
)


query = """
    SELECT customer_name, cloud_account_number, product_name, usage_type, price_book,
           seller_cost_usd, customer_cost_usd, margin_usd, usage_quantity, fecha
    FROM azure_billing_data
"""
df = pd.read_sql(query, engine)


df = df.dropna(how="all")


df.to_csv("azure_export_to_aws_2025-04-13.csv", index=False)
print(" CSV limpio exportado correctamente como 'azure_export_to_aws.csv'")

resumen = df.groupby(['customer_name', 'fecha'], as_index=False).agg({
    'seller_cost_usd': 'sum',
    'margin_usd': 'sum',
    'customer_cost_usd': 'sum'
})


resumen = resumen.sort_values(by=["fecha", "customer_name"], ascending=[False, True])


print("\n Resumen de costos por cliente y fecha:")
print(resumen.to_string(index=False))