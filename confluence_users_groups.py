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

REQUEST_TIMEOUT_SECS = 60
MAX_RETRIES = 5
PAGE_SIZE = 200

# Optional filter. Use "" to keep all groups.
GROUP_NAME_CONTAINS = "confluence"

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

# --------------------------------------------------
# Read secrets
# --------------------------------------------------
BASE_URL = dbutils.secrets.get(scope=SECRET_SCOPE, key=BASE_URL_SECRET_KEY).rstrip("/")
USER_EMAIL = dbutils.secrets.get(scope=SECRET_SCOPE, key=EMAIL_SECRET_KEY)
API_TOKEN = dbutils.secrets.get(scope=SECRET_SCOPE, key=TOKEN_SECRET_KEY)

session = requests.Session()
session.auth = HTTPBasicAuth(USER_EMAIL, API_TOKEN)
session.headers.update({
    "Accept": "application/json"
})

# --------------------------------------------------
# Create schema
# --------------------------------------------------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# --------------------------------------------------
# Step 1: Get Confluence groups
# Endpoint:
# GET /wiki/rest/api/group?start=0&limit=200
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

        size = payload.get("size", len(payload.get("results", [])))
        limit = payload.get("limit", page_size)

        if size == 0:
            break

        start += limit

        # Stop when there is no next link
        links = payload.get("_links", {}) or {}
        if "next" not in links:
            break

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

groups_df = groups_df.dropDuplicates(["site_url", "group_name"])

display(groups_df)

write_delta_table(groups_df, GROUPS_TABLE)

# --------------------------------------------------
# Step 2: Get users from each Confluence group
# Endpoint:
# GET /wiki/rest/api/group/member?name=<group_name>&start=0&limit=200
# --------------------------------------------------
def get_confluence_group_members(session, base_url, group_name, page_size=PAGE_SIZE):
    all_members = []
    start = 0

    while True:
        url = f"{base_url}/wiki/rest/api/group/member"
        params = {
            "name": group_name,
            "start": start,
            "limit": page_size
        }

        payload = api_get(session, url, params=params)
        results = payload.get("results", [])

        all_members.extend(results)

        size = payload.get("size", len(results))
        limit = payload.get("limit", page_size)

        if size == 0:
            break

        start += limit

        links = payload.get("_links", {}) or {}
        if "next" not in links:
            break

    return all_members

membership_rows = []

for row in groups_df.collect():
    group_name = row["group_name"]
    group_id = row["group_id"]

    print(f"Fetching members for Confluence group: {group_name}")

    try:
        members = get_confluence_group_members(
            session=session,
            base_url=BASE_URL,
            group_name=group_name,
            page_size=PAGE_SIZE
        )

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

membership_df = membership_df.dropDuplicates(["site_url", "group_name", "account_id"])

display(membership_df)

write_delta_table(membership_df, MEMBERSHIP_TABLE)

# --------------------------------------------------
# Step 3: Build distinct Confluence users table
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
