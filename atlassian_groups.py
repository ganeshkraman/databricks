# Databricks notebook source
# MAGIC %run ./01_common_api_helpers

# COMMAND ----------

directories_df = spark.table(f"{CATALOG}.{SCHEMA}.atlassian_directories")
directories = [row.asDict() for row in directories_df.collect()]

all_groups = []

for d in directories:
    directory_id = d["directory_id"]
    groups_url = f"{BASE_URL}/v2/orgs/{ORG_ID}/directories/{directory_id}/groups"
    groups = paginate_get(groups_url)
    print(f"Directory {directory_id} groups: {len(groups)}")

    for g in groups:
        all_groups.append({
            "group_id": g.get("id") or g.get("groupId"),
            "group_name": g.get("name"),
            "description": g.get("description"),
            "management_access": g.get("managementAccess"),
            "directory_id": directory_id,
            "directory_name": d.get("name"),
            "raw_json": json.dumps(g)
        })

groups_df = spark.createDataFrame(all_groups) if all_groups else spark.createDataFrame([], """
group_id string,
group_name string,
description string,
management_access string,
directory_id string,
directory_name string,
raw_json string
""")

display(groups_df)

groups_df.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.atlassian_groups")

print("
