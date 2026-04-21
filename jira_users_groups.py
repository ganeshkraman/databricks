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

GROUPS_TABLE = f"{CATALOG}.{SCHEMA}.jira_site_groups"
MEMBERSHIP_TABLE = f"{CATALOG}.{SCHEMA}.jira_group_user_membership"
USERS_TABLE = f"{CATALOG}.{SCHEMA}.jira_users_from_groups"

SECRET_SCOPE = "atlassian"
BASE_URL_SECRET_KEY = "jira-base-url"
EMAIL_SECRET_KEY = "jira-user-email"
TOKEN_SECRET_KEY = "jira-api-token"

REQUEST_TIMEOUT_SECS = 60
MAX_RETRIES = 5
DEFAULT_PAGE_SIZE = 100

# Optional filters
# Set to "" to keep all groups from Jira
GROUP_NAME_CONTAINS = "jira"

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

def get_all_groups(session, base_url, page_size=DEFAULT_PAGE_SIZE, name_contains=""):
    """
    Jira Cloud groups endpoint:
    GET /rest/api/3/group/bulk
    Response contains: isLast, startAt, maxResults, total, values
    """
    url = f"{base_url}/rest/api/3/group/bulk"
    start_at = 0
    all_rows = []

    while True:
        params = {
            "startAt": start_at,
            "maxResults": page_size
        }

        # The API supports groupName query parameter arrays.
        # For a simple practical filter, we fetch all and filter locally.
        payload = api_get(session, url, params=params)

        rows = payload.get("values", [])
        if name_contains:
            rows = [r for r in rows if name_contains.lower() in (r.get("name") or "").lower()]

        all_rows.extend(rows)

        is_last = payload.get("isLast", True)
        returned = len(payload.get("values", []))

        if is_last or returned == 0:
            break

        start_at += returned

    return all_rows

def get_group_members(session, base_url, group_id=None, group_name=None, page_size=DEFAULT_PAGE_SIZE, include_inactive=True):
    """
    Jira Cloud group members endpoint:
    GET /rest/api/3/group/member
    """
    if not group_id and not group_name:
        raise ValueError("Provide either group_id or group_name")

    url = f"{base_url}/rest/api/3/group/member"
    start_at = 0
    all_members = []

    while True:
        params = {
            "startAt": start_at,
            "maxResults": page_size,
            "includeInactiveUsers": str(include_inactive).lower()
        }

        if group_id:
            params["groupId"] = group_id
        else:
            params["groupname"] = group_name

        payload = api_get(session, url, params=params)

        rows = payload.get("values", [])
        all_members.extend(rows)

        is_last = payload.get("isLast", True)
        returned = len(rows)

        if is_last or returned == 0:
            break

        start_at += returned

    return all_members

# --------------------------------------------------
# Read secrets
# --------------------------------------------------
BASE_URL = dbutils.secrets.get(scope=SECRET_SCOPE, key=BASE_URL_SECRET_KEY).rstrip("/")
JIRA_EMAIL = dbutils.secrets.get(scope=SECRET_SCOPE, key=EMAIL_SECRET_KEY)
JIRA_API_TOKEN = dbutils.secrets.get(scope=SECRET_SCOPE, key=TOKEN_SECRET_KEY)

# --------------------------------------------------
# Session
# --------------------------------------------------
session = requests.Session()
session.auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
session.headers.update({
    "Accept": "application/json"
})

# --------------------------------------------------
# Create schema if needed
# --------------------------------------------------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# --------------------------------------------------
# Step 1: Get groups from Jira site
# --------------------------------------------------
groups = get_all_groups(
    session=session,
    base_url=BASE_URL,
    page_size=DEFAULT_PAGE_SIZE,
    name_contains=GROUP_NAME_CONTAINS
)

print(f"Groups returned from Jira site: {len(groups)}")

group_rows = []
for g in groups:
    group_rows.append({
        "site_url": BASE_URL,
        "group_id": g.get("groupId"),
        "group_name": g.get("name"),
        "raw_json": json.dumps(g)
    })

groups_df = spark.createDataFrame(group_rows) if group_rows else spark.createDataFrame([], """
site_url string,
group_id string,
group_name string,
raw_json string
""")

groups_df = groups_df.dropDuplicates(["site_url", "group_id"])
display(groups_df)

write_delta_table(groups_df, GROUPS_TABLE)

# --------------------------------------------------
# Step 2: Get users from each group
# --------------------------------------------------
membership_rows = []

for row in groups_df.collect():
    group_id = row["group_id"]
    group_name = row["group_name"]

    print(f"Fetching members for group: {group_name} ({group_id})")

    try:
        members = get_group_members(
            session=session,
            base_url=BASE_URL,
            group_id=group_id,
            page_size=DEFAULT_PAGE_SIZE,
            include_inactive=True
        )

        for m in members:
            membership_rows.append({
                "site_url": BASE_URL,
                "group_id": group_id,
                "group_name": group_name,
                "account_id": m.get("accountId"),
                "account_type": m.get("accountType"),
                "active": bool(m.get("active")) if m.get("active") is not None else None,
                "display_name": m.get("displayName"),
                "email_address": m.get("emailAddress"),
                "time_zone": m.get("timeZone"),
                "self_url": m.get("self"),
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
active boolean,
display_name string,
email_address string,
time_zone string,
self_url string,
raw_json string
""")

membership_df = membership_df.dropDuplicates(["site_url", "group_id", "account_id"])
display(membership_df)

write_delta_table(membership_df, MEMBERSHIP_TABLE)

# --------------------------------------------------
# Step 3: Build distinct Jira users table
# --------------------------------------------------
jira_users_df = (
    membership_df
    .select(
        "site_url",
        "account_id",
        "account_type",
        "active",
        "display_name",
        "email_address",
        "time_zone"
    )
    .dropDuplicates(["site_url", "account_id"])
)

display(jira_users_df)

write_delta_table(jira_users_df, USERS_TABLE)
