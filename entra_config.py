# Databricks notebook: 01_entra_common_config

import requests
import re
import json
import time

TENANT_ID = dbutils.secrets.get("entra-secrets", "tenant-id")
CLIENT_ID = dbutils.secrets.get("entra-secrets", "client-id")
CLIENT_SECRET = dbutils.secrets.get("entra-secrets", "client-secret")

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"


def get_graph_token():
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }

    response = requests.post(token_url, data=payload)

    if response.status_code != 200:
        raise Exception(f"Token request failed: {response.status_code} - {response.text}")

    return response.json()["access_token"]


def get_headers():
    token = get_graph_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def make_mail_nickname(display_name):
    nickname = display_name.lower()
    nickname = re.sub(r"[^a-z0-9]", "", nickname)
    return nickname[:64]
