# Databricks notebook source

import json
import time
import requests
from requests.auth import HTTPBasicAuth

# --------------------------------------------------
# Config
# --------------------------------------------------
CATALOG = "sandbox"
SCHEMA = "admin"

GROUPS_TABLE = f"{CATALOG}.{SCHEMA}.confluence_site_groups"
MEMBERSHIP_TABLE = f"{CATALOG}.{SCHEMA}.confluence_group_user_membership"
USERS_TABLE = f"{CATALOG}.{SCHEMA}.confluence_users_from_groups"

SECRET_SCOPE = "atlassian"
BASE_URL_SECRET_KEY = "confluence-base-url"      # example: https://yourcompany.atlassian.net
EMAIL_SECRET_KEY = "confluence-user-email"
TOKEN_SECRET_KEY = "confluence-api-token"

PAGE_SIZE = 200
REQUEST_TIMEOUT_SECS = 60
MAX_RETRIES = 5

# Set to "" to extract all groups
GROUP_NAME_CONTAINS = ""

# --------------------------------------------------
# Helpers
# --------------------------------------------------
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


def api_get(session, url, params=None):
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECS)

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


# --------------------------------------------------
# Read secrets
# --------------------------------------------------
BASE_URL = dbutils.secrets.get(scope=SECRET_SCOPE, key=BASE_URL_SECRET_KEY).rstrip("/")
USER_EMAIL = dbutils.secrets.get(scope=SECRET_SCOPE, key=EMAIL_SECRET_KEY)
API_TOKEN = dbutils.secrets.get(scope=SECRET_SCOPE, key=TOKEN_SECRET_KEY)

session = requests.Session()
session.auth = HTTPBasicAuth(USER_EMAIL, API_TOKEN)
session.headers.update({"Accept": "application/json"})

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# --------------------------------------------------
# Step 1: Get Confluence groups
# Endpoint: GET /wiki/rest/api/group
# --------------------------------------------------
def get_confluence_groups(session, base_url, page_size=PAGE_SIZE, name_contains=""):
    all_groups = []
    start = 0

    while True:
        url = f"{base_url}/wiki/rest/api/group"
        params = {
            "start": start,
            "limit": page_size
        }

        payload = api_get(session, url, params=params)

        results = payload.get("results", [])
        if name_contains:
            results = [
                g for g in results
                if name_contains.lower() in (g.get("name") or "").lower()
            ]

        all_groups.extend(results)

        links = payload.get("_links", {}) or {}
        if "next" not in links:
            break

        start += page_size

    return all_groups


groups = get_confluence_groups(
    session=session,
    base_url=BASE_URL,
    page_size=PAGE_SIZE,
    name_contains=GROUP_NAME_CONTAINS
)

print(f"Confluence groups returned: {len(groups)}")

group_rows = []
for g in groups:
    group_rows.append({
        "site_url": BASE_URL,
        "group_id": g.get("id"),
        "group_name": g.get("name"),
        "type": g.get("type"),
        "raw_json": json.dumps(g)
    })

groups_df = spark.createDataFrame(group_rows) if group_rows else spark.createDataFrame([], """
site_url string,
group_id string,
group_name string,
type string,
raw_json string
""")

groups_df = groups_df.dropDuplicates(["site_url", "group_id"])

display(groups_df)

write_delta_table(groups_df, GROUPS_TABLE)

# --------------------------------------------------
# Step 2: Get members for each group by group ID
# Endpoint: GET /wiki/rest/api/group/{groupId}/membersByGroupId
# This avoids deprecated group-name member APIs that may return 410.
# --------------------------------------------------
def get_confluence_group_members_by_group_id(session, base_url, group_id, page_size=PAGE_SIZE):
    all_members = []
    start = 0

    while True:
        url = f"{base_url}/wiki/rest/api/group/{group_id}/membersByGroupId"
        params = {
            "start": start,
            "limit": page_size
        }

        payload = api_get(session, url, params=params)

        results = payload.get("results", [])
        all_members.extend(results)

        links = payload.get("_links", {}) or {}
        if "next" not in links:
            break

        start += page_size

    return all_members


membership_rows = []

for row in groups_df.collect():
    group_id = row["group_id"]
    group_name = row["group_name"]

    if not group_id:
        print(f"Skipping group with missing group_id: {group_name}")
        continue

    print(f"Fetching members for group: {group_name} | {group_id}")

    try:
        members = get_confluence_group_members_by_group_id(
            session=session,
            base_url=BASE_URL,
            group_id=group_id,
            page_size=PAGE_SIZE
        )

        print(f"Members found: {len(members)}")

        for m in members:
            membership_rows.append({
                "site_url": BASE_URL,
                "group_id": group_id,
                "group_name": group_name,
                "account_id": m.get("accountId"),
                "account_type": m.get("accountType"),
                "public_name": m.get("publicName"),
                "display_name": m.get("displayName"),
                "email": m.get("email"),
                "profile_picture": json.dumps(m.get("profilePicture", {})),
                "raw_json": json.dumps(m)
            })

    except Exception as e:
        print(f"Skipping group {group_name}. Reason: {e}")


membership_df = spark.createDataFrame(membership_rows) if membership_rows else spark.createDataFrame([], """
site_url string,
group_id string,
group_name string,
account_id string,
account_type string,
public_name string,
display_name string,
email string,
profile_picture string,
raw_json string
""")

membership_df = membership_df.dropDuplicates(["site_url", "group_id", "account_id"])

display(membership_df)

write_delta_table(membership_df, MEMBERSHIP_TABLE)

# --------------------------------------------------
# Step 3: Create distinct Confluence users table
# --------------------------------------------------
confluence_users_df = (
    membership_df
    .select(
        "site_url",
        "account_id",
        "account_type",
        "public_name",
        "display_name",
        "email"
    )
    .dropDuplicates(["site_url", "account_id"])
)

display(confluence_users_df)

write_delta_table(confluence_users_df, USERS_TABLE)

# --------------------------------------------------
# Step 4: Validation
# --------------------------------------------------
print("Validation counts")

spark.sql(f"""
SELECT COUNT(*) AS confluence_group_count
FROM {GROUPS_TABLE}
""").show()

spark.sql(f"""
SELECT COUNT(*) AS confluence_group_membership_count
FROM {MEMBERSHIP_TABLE}
""").show()

spark.sql(f"""
SELECT COUNT(*) AS confluence_distinct_user_count
FROM {USERS_TABLE}
""").show()

spark.sql(f"""
SELECT group_name, COUNT(DISTINCT account_id) AS member_count
FROM {MEMBERSHIP_TABLE}
GROUP BY group_name
ORDER BY member_count DESC, group_name
""").show(200, truncate=False)
