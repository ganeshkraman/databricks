import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# --- Configuration ---
workspace_url = "https://<your-databricks-workspace-url>"  # e.g., https://adb-1234567890123456.7.azuredatabricks.net
token = "<your-databricks-pat>"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/scim+json"
}

# --- Step 1: Get all groups ---
groups_url = f"{workspace_url}/api/2.0/preview/scim/v2/Groups"
response = requests.get(groups_url, headers=headers)

if response.status_code != 200:
    raise Exception(f"Error retrieving groups: {response.text}")

groups = response.json().get("Resources", [])
if not groups:
    raise Exception("No groups found")

# --- Step 2: Get members of each group ---
all_rows = []

for group in groups:
    group_id = group.get("id")
    group_detail_url = f"{groups_url}/{group_id}"
    response = requests.get(group_detail_url, headers=headers)

    if response.status_code != 200:
        print(f"Failed to get members for group ID: {group_id}")
        continue

    members = response.json().get("members", [])
    for member in members:
        all_rows.append((
            group_id,                      # group_id
            member.get("value"),           # member_id
            member.get("display")          # member_name
        ))

# --- Step 3: Convert to Spark DataFrame ---
schema = StructType([
    StructField("group_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("member_name", StringType(), True)
])

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(all_rows, schema)

# --- Step 4: Save to Delta table ---
df.write.format("delta").mode("overwrite").saveAsTable("sandbox.admin.group_members")
