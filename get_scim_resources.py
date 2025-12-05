import requests
from pyspark.sql.types import StructType, StructField, StringType, LongType

# ============================================================
# 1. Workspace context & SCIM authentication
# ============================================================

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

host = context.apiUrl().get().rstrip("/")          # e.g., https://abc-123.us-east-1.databricks.com
workspace_id = int(context.workspaceId().get())    # make sure it's a plain Python int
token = dbutils.secrets.get(scope="secrets", key="pat")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/scim+json"
}


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
# 3. Define schemas
# ============================================================

users_schema = StructType([
    StructField("user_id",      StringType(), True),
    StructField("user_name",    StringType(), True),
    StructField("email",        StringType(), True),
    StructField("workspace_id", LongType(),   True)
])

groups_schema = StructType([
    StructField("group_id",     StringType(), True),
    StructField("group_name",   StringType(), True),
    StructField("external_id",  StringType(), True),
    StructField("workspace_id", LongType(),   True)
])

members_schema = StructType([
    StructField("group_id",     StringType(), True),
    StructField("group_name",   StringType(), True),
    StructField("member_id",    StringType(), True),
    StructField("member_name",  StringType(), True),
    StructField("workspace_id", LongType(),   True)
])


# ============================================================
# 4. Build dev_users table (sandbox.admin.dev_users)
# ============================================================

user_rows = []

for u in users:
    emails = u.get("emails", []) or []
    primary_email = None

    for e in emails:
        if e.get("primary"):
            primary_email = e.get("value")
            break

    if primary_email is None and emails:
        primary_email = emails[0].get("value")

    user_rows.append({
        "user_id": str(u.get("id")) if u.get("id") is not None else None,
        "user_name": u.get("userName"),
        "email": primary_email,
        "workspace_id": workspace_id
    })

df_users = spark.createDataFrame(user_rows, schema=users_schema)
df_users.write.mode("overwrite").saveAsTable("sandbox.admin.dev_users")
print(f"Wrote {df_users.count()} rows to sandbox.admin.dev_users")


# ============================================================
# 5. Build dev_groups table (sandbox.admin.dev_groups)
# ============================================================

group_rows = []

for g in groups:
    group_rows.append({
        "group_id": str(g.get("id")) if g.get("id") is not None else None,
        "group_name": g.get("displayName"),
        "external_id": g.get("externalId"),
        "workspace_id": workspace_id
    })

df_groups = spark.createDataFrame(group_rows, schema=groups_schema)
df_groups.write.mode("overwrite").saveAsTable("sandbox.admin.dev_groups")
print(f"Wrote {df_groups.count()} rows to sandbox.admin.dev_groups")


# ============================================================
# 6. Build dev_group_members table (sandbox.admin.dev_group_members)
# ============================================================

member_rows = []

for g in groups:
    group_id = g.get("id")
    group_name = g.get("displayName")

    for m in g.get("members", []) or []:
        member_rows.append({
            "group_id": str(group_id) if group_id is not None else None,
            "group_name": group_name,
            "member_id": str(m.get("value")) if m.get("value") is not None else None,
            "member_name": m.get("display"),
            "workspace_id": workspace_id
        })

df_members = spark.createDataFrame(member_rows, schema=members_schema)
df_members.write.mode("overwrite").saveAsTable("sandbox.admin.dev_group_members")
print(f"Wrote {df_members.count()} rows to sandbox.admin.dev_group_members")
