# Databricks notebook: 03_add_member_to_entra_group

# Run common notebook first
# %run ./01_entra_common_config

group_name = "us-commercial-dev-databricks-developers"
user_email = "user1@company.com"


def get_group_by_display_name(display_name):
    url = f"{GRAPH_BASE_URL}/groups"

    params = {
        "$filter": f"displayName eq '{display_name}'",
        "$select": "id,displayName"
    }

    response = requests.get(url, headers=get_headers(), params=params)

    if response.status_code != 200:
        raise Exception(f"Group lookup failed: {response.status_code} - {response.text}")

    groups = response.json().get("value", [])

    if not groups:
        raise Exception(f"Group not found: {display_name}")

    return groups[0]


def get_user_by_email(email):
    url = f"{GRAPH_BASE_URL}/users/{email}"

    response = requests.get(url, headers=get_headers())

    if response.status_code != 200:
        raise Exception(f"User lookup failed: {response.status_code} - {response.text}")

    return response.json()


def add_user_to_group(group_id, user_id):
    url = f"{GRAPH_BASE_URL}/groups/{group_id}/members/$ref"

    body = {
        "@odata.id": f"{GRAPH_BASE_URL}/directoryObjects/{user_id}"
    }

    response = requests.post(url, headers=get_headers(), json=body)

    if response.status_code == 204:
        print("User added to group.")
    elif response.status_code == 400 and "added object references already exist" in response.text:
        print("User is already a member of the group.")
    else:
        raise Exception(f"Add member failed: {response.status_code} - {response.text}")


group = get_group_by_display_name(group_name)
user = get_user_by_email(user_email)

add_user_to_group(
    group_id=group["id"],
    user_id=user["id"]
)
