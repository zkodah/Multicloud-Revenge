import os
import requests
from dotenv import load_dotenv

load_dotenv()

# Parámetros
tenant_id = os.getenv("AZURE_TENANT_ID")
client_id = os.getenv("AZURE_CLIENT_ID")
client_secret = os.getenv("AZURE_CLIENT_SECRET")
billing_account_encoded = "b49d7261-556c-4548-a649-0619a833a80e%3A7943d75f-24f4-418f-9011-c6d392566d89_2018-09-30"

invoice_ids = [
    "G085681907",
    "G081146618",
    "G076898127",
    "G072828889",
    "G069056336",
    "G065320053",
    "G061650767",
    "G058479762",
    "G055363285",
    "G052325813",
    "G049354047",
    "G046470057"
]

def get_access_token():
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "https://management.azure.com/.default"
    }
    response = requests.post(url, data=payload)
    return response.json()["access_token"]

def get_invoice(token):
    headers = {
        "Authorization": f"Bearer {token}"
    }

    for invoice_id in invoice_ids:
        url = f"https://management.azure.com/providers/Microsoft.Billing/billingAccounts/{billing_account_encoded}/invoices/{invoice_id}?api-version=2024-04-01"
        response = requests.get(url, headers=headers)
        print(f"{invoice_id}: Status {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            estado = data.get("properties", {}).get("status", "No disponible")
            print(f"  Estado de pago: {estado}")

if __name__ == "__main__":
    token = get_access_token()
    get_invoice(token)
