# Databricks notebook source
# MAGIC %run ./01_common_api_helpers

# COMMAND ----------

workspaces_url = f"{BASE_URL}/v2/orgs/{ORG_ID}/workspaces"
workspaces = paginate_post(workspaces_url, json_body={})

print(f"Workspaces found: {len(workspaces)}")

workspace_rows = []
for ws in workspaces:
    attrs = ws.get("attributes", {}) or {}
    workspace_rows.append({
        "workspace_id": ws.get("id"),
        "workspace_type": ws.get("type"),
        "name": attrs.get("name"),
        "type_key": attrs.get("typeKey"),
        "product_type": attrs.get("type"),
        "owner": attrs.get("owner"),
        "status": attrs.get("status"),
        "host_url": attrs.get("hostUrl"),
        "created_at": attrs.get("createdAt"),
        "updated_at": attrs.get("updatedAt"),
        "realm": attrs.get("realm"),
        "raw_json": json.dumps(ws)
    })

workspaces_df = spark.createDataFrame(workspace_rows) if workspace_rows else spark.createDataFrame([], """
workspace_id string,
workspace_type string,
name string,
type_key string,
product_type string,
owner string,
status string,
host_url string,
created_at string,
updated_at string,
realm string,
raw_json string
""")

display(workspaces_df)

workspaces_df.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.atlassian_workspaces")

print("Written table: atlassian_workspaces")
