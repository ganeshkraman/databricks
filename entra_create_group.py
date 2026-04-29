# Databricks notebook: 02_create_entra_group

# Run common notebook first
# %run ./01_entra_common_config

group_name = "us-commercial-dev-databricks-developers"
group_description = "Databricks developers group for US Commercial Dev environment"


def get_group_by_display_name(display_name):
    url = f"{GRAPH_BASE_URL}/groups"

    params = {
        "$filter": f"displayName eq '{display_name}'",
        "$select": "id,displayName,mailNickname,securityEnabled,mailEnabled"
    }

    response = requests.get(url, headers=get_headers(), params=params)

    if response.status_code != 200:
        raise Exception(f"Group lookup failed: {response.status_code} - {response.text}")

    groups = response.json().get("value", [])
    return groups[0] if groups else None


def create_security_group(display_name, description=None):
    existing_group = get_group_by_display_name(display_name)

    if existing_group:
        print("Group already exists.")
        return existing_group

    url = f"{GRAPH_BASE_URL}/groups"

    body = {
        "displayName": display_name,
        "mailEnabled": False,
        "mailNickname": make_mail_nickname(display_name),
        "securityEnabled": True,
        "description": description or display_name
    }

    response = requests.post(url, headers=get_headers(), json=body)

    if response.status_code != 201:
        raise Exception(f"Group creation failed: {response.status_code} - {response.text}")

    print("Group created.")
    return response.json()


group = create_security_group(group_name, group_description)

display(group)
