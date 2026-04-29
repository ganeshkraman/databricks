# Databricks notebook: assign_enterprise_app_to_entra_group

import requests
import json
import uuid

# -----------------------------
# 1. Inputs
# -----------------------------

APP_DISPLAY_NAME = "Enterprise Databrics"
GROUP_DISPLAY_NAME = "us-commercial-dev-databricks-developers"

SECRET_SCOPE = "entra-secrets"
TENANT_ID = dbutils.secrets.get(SECRET_SCOPE, "tenant-id")
CLIENT_ID = dbutils.secrets.get(SECRET_SCOPE, "client-id")
CLIENT_SECRET = dbutils.secrets.get(SECRET_SCOPE, "client-secret")

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"


# -----------------------------
# 2. Get Microsoft Graph token
# -----------------------------

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


# -----------------------------
# 3. Find Enterprise App service principal
# -----------------------------

def get_service_principal_by_display_name(app_display_name):
    url = f"{GRAPH_BASE_URL}/servicePrincipals"

    params = {
        "$filter": f"displayName eq '{app_display_name}'",
        "$select": "id,appId,displayName,appRoles"
    }

    response = requests.get(url, headers=get_headers(), params=params)

    if response.status_code != 200:
        raise Exception(f"Service principal lookup failed: {response.status_code} - {response.text}")

    results = response.json().get("value", [])

    if not results:
        raise Exception(f"Enterprise App not found: {app_display_name}")

    if len(results) > 1:
        print(f"Warning: multiple Enterprise Apps found with name: {app_display_name}. Using the first one.")

    return results[0]


# -----------------------------
# 4. Find Entra ID group
# -----------------------------

def get_group_by_display_name(group_display_name):
    url = f"{GRAPH_BASE_URL}/groups"

    params = {
        "$filter": f"displayName eq '{group_display_name}'",
        "$select": "id,displayName,securityEnabled"
    }

    response = requests.get(url, headers=get_headers(), params=params)

    if response.status_code != 200:
        raise Exception(f"Group lookup failed: {response.status_code} - {response.text}")

    results = response.json().get("value", [])

    if not results:
        raise Exception(f"Group not found: {group_display_name}")

    if len(results) > 1:
        print(f"Warning: multiple groups found with name: {group_display_name}. Using the first one.")

    group = results[0]

    if not group.get("securityEnabled", False):
        raise Exception(f"Group is not security-enabled: {group_display_name}")

    return group


# -----------------------------
# 5. Pick app role
# -----------------------------

def get_app_role_id(service_principal):
    app_roles = service_principal.get("appRoles", [])

    enabled_roles = [
        role for role in app_roles
        if role.get("isEnabled", False)
    ]

    # Prefer Default Access if available
    for role in enabled_roles:
        if role.get("displayName") == "Default Access":
            return role["id"]

    # Then prefer a role allowed for users/groups
    for role in enabled_roles:
        allowed_types = role.get("allowedMemberTypes", [])
        if "User" in allowed_types:
            return role["id"]

    # If the app has no app roles, use default app role assignment ID
    return "00000000-0000-0000-0000-000000000000"


# -----------------------------
# 6. Check whether assignment already exists
# -----------------------------

def assignment_exists(group_id, resource_sp_id):
    url = f"{GRAPH_BASE_URL}/groups/{group_id}/appRoleAssignments"

    response = requests.get(url, headers=get_headers())

    if response.status_code != 200:
        raise Exception(f"Assignment lookup failed: {response.status_code} - {response.text}")

    assignments = response.json().get("value", [])

    for assignment in assignments:
        if assignment.get("resourceId") == resource_sp_id:
            return True

    return False


# -----------------------------
# 7. Assign Enterprise App to group
# -----------------------------

def assign_enterprise_app_to_group(group_id, resource_sp_id, app_role_id):
    if assignment_exists(group_id, resource_sp_id):
        print("Assignment already exists. No action taken.")
        return None

    url = f"{GRAPH_BASE_URL}/groups/{group_id}/appRoleAssignments"

    body = {
        "principalId": group_id,
        "resourceId": resource_sp_id,
        "appRoleId": app_role_id
    }

    response = requests.post(url, headers=get_headers(), json=body)

    if response.status_code not in [200, 201]:
        raise Exception(f"App assignment failed: {response.status_code} - {response.text}")

    print("Enterprise App assigned to group successfully.")
    return response.json()


# -----------------------------
# 8. Execute
# -----------------------------

sp = get_service_principal_by_display_name(APP_DISPLAY_NAME)
group = get_group_by_display_name(GROUP_DISPLAY_NAME)
app_role_id = get_app_role_id(sp)

print("Enterprise App:")
print(json.dumps({
    "displayName": sp["displayName"],
    "servicePrincipalId": sp["id"],
    "appId": sp["appId"]
}, indent=2))

print("Group:")
print(json.dumps(group, indent=2))

print(f"App Role ID selected: {app_role_id}")

assignment = assign_enterprise_app_to_group(
    group_id=group["id"],
    resource_sp_id=sp["id"],
    app_role_id=app_role_id
)

if assignment:
    display(assignment)
