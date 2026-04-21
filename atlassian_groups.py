# Databricks notebook source
# MAGIC %run ./00_config_and_secrets
# MAGIC %run ./01_common_api_helpers

def write_delta_table(df, table_name: str, mode: str = "overwrite"):
    if df is None:
        raise ValueError("DataFrame is None")

    if len(df.columns) == 0:
        raise ValueError("DataFrame has no columns")

    print(f"Writing table: {table_name} | mode={mode}")

    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print(f"Success: {table_name}")

import json

directories_df = spark.table(f"{CATALOG}.{SCHEMA}.atlassian_directories")
directories = [row.asDict() for row in directories_df.collect()]

all_groups = []

for d in directories:
    directory_id = d["directory_id"]
    groups_url = f"{BASE_URL}/v2/orgs/{ORG_ID}/directories/{directory_id}/groups"
    groups = paginate_get(groups_url)

    for g in groups:
        all_groups.append({
            "group_id": g.get("id") or g.get("groupId"),
            "group_name": g.get("name"),
            "description": g.get("description"),
            "management_access": str(g.get("managementAccess")),
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

write_delta_table(groups_df, f"{CATALOG}.{SCHEMA}.atlassian_groups")
