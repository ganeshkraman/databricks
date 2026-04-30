# Databricks notebook: add_owner_to_entra_group

import requests
import json

# -----------------------------
# 1. Inputs
# -----------------------------

GROUP_NAME = "databricks_us_commercial_developers"
USER_EMAIL = "john_smith@company.com"

SECRET_SCOPE = "entra-secrets"

TENANT_ID = dbutils.secrets.get(SECRET_SCOPE, "tenant-id")
CLIENT_ID = dbutils.secrets.get(SECRET_SCOPE, "client-id")
CLIENT_SECRET = dbutils.secrets.get(SECRET_SCOPE, "client-secret")

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"


# -----------------------------
# 2. Get Access Token
# -----------------------------

def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }

    response = requests.post(url, data=payload)

    if response.status_code != 200:
        raise Exception(f"Token error: {response.text}")

    return response.json()["access_token"]


def get_headers():
    return {
        "Authorization": f"Bearer {get_token()}",
        "Content-Type": "application/json"
    }


# -----------------------------
# 3. Get Group
# -----------------------------

def get_group(group_name):
    url = f"{GRAPH_BASE_URL}/groups"
    params = {
        "$filter": f"displayName eq '{group_name}'",
        "$select": "id,displayName"
    }

    response = requests.get(url, headers=get_headers(), params=params)

    if response.status_code != 200:
        raise Exception(f"Group lookup failed: {response.text}")

    groups = response.json().get("value", [])

    if not groups:
        raise Exception(f"Group not found: {group_name}")

    return groups[0]


# -----------------------------
# 4. Get User
# -----------------------------

def get_user(email):
    url = f"{GRAPH_BASE_URL}/users/{email}"

    response = requests.get(url, headers=get_headers())

    if response.status_code != 200:
        raise Exception(f"User lookup failed: {response.text}")

    return response.json()


# -----------------------------
# 5. Check if already owner
# -----------------------------

def is_owner(group_id, user_id):
    url = f"{GRAPH_BASE_URL}/groups/{group_id}/owners"

    response = requests.get(url, headers=get_headers())

    if response.status_code != 200:
        raise Exception(f"Owner check failed: {response.text}")

    owners = response.json().get("value", [])

    return any(owner["id"] == user_id for owner in owners)


# -----------------------------
# 6. Add Owner
# -----------------------------

def add_owner(group_id, user_id):
    if is_owner(group_id, user_id):
        print("User is already an owner.")
        return

    url = f"{GRAPH_BASE_URL}/groups/{group_id}/owners/$ref"

    body = {
        "@odata.id": f"{GRAPH_BASE_URL}/directoryObjects/{user_id}"
    }

    response = requests.post(url, headers=get_headers(), json=body)

    if response.status_code == 204:
        print("User added as owner successfully.")
    else:
        raise Exception(f"Add owner failed: {response.status_code} - {response.text}")


# -----------------------------
# 7. Execute
# -----------------------------

group = get_group(GROUP_NAME)
user = get_user(USER_EMAIL)

print("Group:", group["displayName"])
print("User:", user["userPrincipalName"])

add_owner(
    group_id=group["id"],
    user_id=user["id"]
)
