import requests

# --- 1. Get workspace context and token ---
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

host = context.apiUrl().get().rstrip("/")             # Workspace URL
workspace_id = context.workspaceId().get()            # Numeric workspace ID
token = dbutils.secrets.get(scope="secrets", key="pat")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/scim+json"
}

# --- 2. Retrieve all groups from SCIM API (with pagination) ---

def get_all_groups():
    all_groups = []
    start_index = 1
    page_size = 100

    while True:
        resp = requests.get(
            f"{host}/api/2.0/preview/scim/v2/Groups",
            headers=headers,
            params={"startIndex": start_index, "count": page_size}
        )
        resp.raiseForStatus = resp.raise_for_status()
        data = resp.json()

        resources = data.get("Resources", [])
        if not resources:
            break

        all_groups.extend(resources)
        start_index += len(resources)

        if start_index > data.get("totalResults", 0):
            break

    return all_groups

groups = get_all_groups()

# --- 3. Build dev_groups dataframe ---

group_rows = []

for g in groups:
    group_rows.append({
        "group_id": g.get("id"),
        "group_display": g.get("displayName"),  # renamed field
        "external_id": g.get("externalId"),
        "workspace_id": workspace_id
    })

df_groups = spark.createDataFrame(group_rows)

df_groups.write.mode("overwrite").saveAsTable("sandbox.admin.dev_groups")

print(f"Wrote {df_groups.count()} rows to sandbox.admin.dev_groups")

# --- 4. Build dev_group_members dataframe ---

member_rows = []

for g in groups:
    group_id = g.get("id")
    group_name = g.get("displayName")

    for m in g.get("members", []):
        member_rows.append({
            "group_id": group_id,
            "group_display": group_name,
            "member_id": m.get("value"),
            "member_display": m.get("display")
        })

# Create dataframe with exact schema if empty
df_members = (
    spark.createDataFrame(member_rows)
    if member_rows
    else spark.createDataFrame(
        [],
        "group_id string, group_display string, member_id string, member_display string"
    )
)

df_members.write.mode("overwrite").saveAsTable("sandbox.admin.dev_group_members")

print(f"Wrote {df_members.count()} rows to sandbox.admin.dev_group_members")
