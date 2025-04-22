import json
import time
import requests
import os

TOKEN_FILE = os.path.join(os.path.dirname(__file__), "token.json")
AUTH_URL = "https://ion.tdsynnex.com/oauth/token"

def cargar_tokens():
    try:
        with open(TOKEN_FILE, "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def guardar_tokens(access_token, refresh_token, expires_in):
    data = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_in": expires_in,
        "generated_at": int(time.time())
    }
    with open(TOKEN_FILE, "w") as file:
        json.dump(data, file, indent=4)

def token_expirado():
    tokens = cargar_tokens()
    if not tokens:
        return True  
    tiempo_transcurrido = int(time.time()) - tokens.get("generated_at", 0)
    return tiempo_transcurrido >= tokens.get("expires_in", 3600)

def renovar_token():
    tokens = cargar_tokens()
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        print("No hay refresh_token disponible. Debes generar uno nuevo manualmente.")
        return None
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(AUTH_URL, data=payload, headers=headers)
    if response.status_code == 200:
        new_tokens = response.json()
        guardar_tokens(
            new_tokens["access_token"],
            new_tokens["refresh_token"],
            new_tokens["expires_in"]
        )
        print("Token renovado correctamente.")
        return new_tokens["access_token"]
    else:
        print(f"Error al renovar el token: {response.text}")
        return None

def obtener_token():
    if token_expirado():
        return renovar_token()
    return cargar_tokens().get("access_token")
