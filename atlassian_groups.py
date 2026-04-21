# Databricks notebook source

import json
import time
import requests

# -----------------------------------
# Config
# -----------------------------------
CATALOG = "sandbox"
SCHEMA = "admin"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.atlassian_groups"

SECRET_SCOPE = "atlassian"
API_KEY_SECRET_KEY = "atlassian-admin-api-key"

BASE_URL_V2 = "https://api.atlassian.com/admin/v2"
PAGE_SIZE = 100
REQUEST_TIMEOUT_SECS = 60
MAX_RETRIES = 5

# -----------------------------------
# Helpers
# -----------------------------------
def write_delta_table(df, table_name: str, mode: str = "overwrite"):
    print(f"Writing: {table_name}")
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Success: {table_name}")

def api_get(session, url, params=None, timeout=REQUEST_TIMEOUT_SECS):
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(url, params=params, timeout=timeout)

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_secs = int(retry_after) if retry_after and retry_after.isdigit() else min(2 * attempt, 30)
            print(f"Rate limited. Sleeping {sleep_secs} seconds.")
            time.sleep(sleep_secs)
            continue

        if 500 <= resp.status_code < 600 and attempt < MAX_RETRIES:
            sleep_secs = min(2 ** attempt, 30)
            print(f"Server error {resp.status_code}. Retrying in {sleep_secs} seconds.")
            time.sleep(sleep_secs)
            continue

        raise RuntimeError(
            f"GET failed\n"
            f"URL: {url}\n"
            f"Params: {params}\n"
            f"Status: {resp.status_code}\n"
            f"Response: {resp.text}"
        )

    raise RuntimeError(f"GET failed after retries: {url}")

def paginate_get(session, url, initial_params=None):
    """
    Generic pagination for Atlassian Admin API endpoints that return:
    {
      "data": [...],
      "links": { "next": "..." }
    }
    """
    all_rows = []
    params = dict(initial_params or {})
    params.setdefault("limit", PAGE_SIZE)

    while True:
        payload = api_get(session, url, params=params)
        rows = payload.get("data", [])
        all_rows.extend(rows)

        links = payload.get("links", {}) or {}
        next_cursor = links.get("next")

        if not next_cursor:
            break

        params["cursor"] = next_cursor

    return all_rows

# -----------------------------------
# Read API key from Databricks secret
# -----------------------------------
API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=API_KEY_SECRET_KEY)

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
})

# -----------------------------------
# Create catalog/schema if needed
# -----------------------------------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# -----------------------------------
# Step 1: Resolve org from API key
# -----------------------------------
orgs_url = f"{BASE_URL_V2}/orgs"
orgs_payload = api_get(session, orgs_url, params={"limit": 10})
orgs = orgs_payload.get("data", [])

if not orgs:
    raise RuntimeError("No Atlassian organizations returned for this API key.")

org_id = orgs[0]["id"]
print(f"Resolved org_id: {org_id}")

# -----------------------------------
# Step 2: Get directories for the org
# -----------------------------------
directories_url = f"{BASE_URL_V2}/orgs/{org_id}/directories"
directories = paginate_get(session, directories_url, initial_params={"limit": PAGE_SIZE})

if not directories:
    raise RuntimeError(f"No directories found for org_id={org_id}")

print(f"Directories found: {len(directories)}")

# -----------------------------------
# Step 3: Get groups for each directory
# Official doc says groups endpoint requires:
#   orgId + directoryId
# -----------------------------------
all_group_rows = []

for d in directories:
    directory_id = d.get("directoryId")
    directory_name = d.get("name")

    if not directory_id:
        print(f"Skipping directory with missing directoryId: {d}")
        continue

    groups_url = f"{BASE_URL_V2}/orgs/{org_id}/directories/{directory_id}/groups"
    print(f"Fetching groups for directory_id={directory_id}")

    groups = paginate_get(session, groups_url, initial_params={"limit": PAGE_SIZE})
    print(f"Groups returned: {len(groups)}")

    for g in groups:
        all_group_rows.append({
            "org_id": org_id,
            "directory_id": directory_id,
            "directory_name": directory_name,
            "group_id": g.get("id") or g.get("groupId"),
            "group_name": g.get("name"),
            "description": g.get("description"),
            "management_access": str(g.get("managementAccess")),
            "raw_json": json.dumps(g)
        })

# -----------------------------------
# Step 4: Convert to Spark DataFrame
# -----------------------------------
if all_group_rows:
    groups_df = spark.createDataFrame(all_group_rows)
else:
    groups_df = spark.createDataFrame([], """
        org_id string,
        directory_id string,
        directory_name string,
        group_id string,
        group_name string,
        description string,
        management_access string,
        raw_json string
    """)

groups_df = groups_df.dropDuplicates(["org_id", "directory_id", "group_id"])

display(groups_df)

# -----------------------------------
# Step 5: Write Delta table
# -----------------------------------
write_delta_table(groups_df, TABLE_NAME)
