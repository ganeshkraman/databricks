import requests
from datetime import datetime

# ============================================================
# 1. Workspace context & SCIM auth
# ============================================================

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

host = context.apiUrl().get().rstrip("/")          # e.g. https://abc-123.us-east-1.databricks.com
workspace_id = context.workspaceId().get()         # Numeric workspace ID
token = dbutils.secrets.get(scope="secrets", key="pat")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/scim+json"
}

load_ts = datetime.utcnow().isoformat()


# ============================================================
# 2. SCIM resource fetcher (Users or Groups)
# ============================================================

def get_all_scim_resources(resource_type: str):
    all_resources = []
    start_index = 1
    page_size = 100

    while True:
        resp = requests.get(
            f"{host}/api/2.0/preview/scim/v2/{resource_type}",
            headers=headers,
            params={"startIndex": start_index, "count": page_size}
        )
        resp.raise_for_status()
        data = resp.json()

        resources = data.get("Resources", [])
        if not resources:
            break

        all_resources.extend(resources)
        start_index += len(resources)

        if start_index > data.get("totalResults", 0):
            break

    return all_resources

users = get_all_scim_resources("Users")
groups = get_all_scim_resources("Groups")


# ============================================================
# 3. Build dev_users table
# ============================================================

user_rows = []

for u in users:
    emails = u.get("emails", [])
    primary_email = None

    for e in emails:
        if e.get("primary"):
            primary_email = e.get("value")
            break

    if primary_email is None and emails:
        primary_email = emails[0].get("value")

    user_rows.append({
        "user_id": u.get("id"),
        "user_name": u.get("userName"),
        "user_display": u.get("displayName"),
        "external_id": u.get("externalId"),
        "email": primary_email,
        "active": bool(u.get("active", True)),
        "user_type": u.get("userType"),
        "workspace_id": workspace_id,
        "load_timestamp": load_ts
    })

df_users = (
    spark.createDataFrame(user_rows)
    if user_rows else spark.createDataFrame(
        [],
        "user_id string, user_name string, user_display string, external_id string, "
        "email string, active boolean, user_type string, workspace_id long, load_timestamp string"
    )
)

df_users.write.mode("overwrite").saveAsTable("sandbox.admin.dev_users")
print(f"Wrote {df_users.count()} rows to sandbox.admin.dev_users")


# ============================================================
# 4. Build dev_groups table
# ============================================================

group_rows = []

for g in groups:
    group_rows.append({
        "group_id": g.get("id"),
        "group_name": g.get("displayName"),
        "external_id": g.get("externalId"),
        "workspace_id": workspace_id,
        "load_timestamp": load_ts
    })

df_groups = (
    spark.createDataFrame(group_rows)
    if group_rows else spark.createDataFrame(
        [],
        "group_id string, group_name string, external_id string, workspace_id long, load_timestamp string"
    )
)

df_groups.write.mode("overwrite").saveAsTable("sandbox.admin.dev_groups")
print(f"Wrote {df_groups.count()} rows to sandbox.admin.dev_groups")


# ============================================================
# 5. Build dev_group_members table (updated with workspace_id)
# ============================================================

member_rows = []

for g in groups:
    group_id = g.get("id")
    group_name = g.get("displayName")

    for m in g.get("members", []) or []:
        member_rows.append({
            "group_id": group_id,
            "group_name": group_name,
            "member_id": m.get("value"),
            "member_name": m.get("display"),
            "workspace_id": workspace_id,
            "load_timestamp": load_ts
        })

df_members = (
    spark.createDataFrame(member_rows)
    if member_rows else spark.createDataFrame(
        [],
        "group_id string, group_name string, member_id string, member_name string, "
        "workspace_id long, load_timestamp string"
    )
)

df_members.write.mode("overwrite").saveAsTable("sandbox.admin.dev_group_members")
print(f"Wrote {df_members.count()} rows to sandbox.admin.dev_group_members")
