# Databricks notebook source
# MAGIC %run ./00_config_and_secrets
# MAGIC %run ./01_common_api_helpers

import json

def write_delta_table(df, table_name: str, mode: str = "overwrite"):
    print(f"Writing: {table_name}")
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

users_url = f"{BASE_URL}/v2/orgs/{ORG_ID}/users"
users = paginate_get(users_url)

user_rows = []
for u in users:
    user_rows.append({
        "account_id": u.get("accountId"),
        "account_type": u.get("accountType"),
        "account_status": u.get("accountStatus"),
        "claim_status": u.get("claimStatus"),
        "status": u.get("status"),
        "name": u.get("name"),
        "email": u.get("email"),
        "nickname": u.get("nickname"),
        "platform_roles": json.dumps(u.get("platformRoles", [])),
        "raw_json": json.dumps(u)
    })

users_df = spark.createDataFrame(user_rows) if user_rows else spark.createDataFrame([], """
account_id string,
account_type string,
account_status string,
claim_status string,
status string,
name string,
email string,
nickname string,
platform_roles string,
raw_json string
""")

display(users_df)

write_delta_table(users_df, f"{CATALOG}.{SCHEMA}.atlassian_org_users")
