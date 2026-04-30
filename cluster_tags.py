# Databricks notebook
# Purpose:
# Update mandatory custom tags on all-purpose clusters:
# business, workload_type, region, env

import requests
import json
import time

# -----------------------------
# 1. Parameters
# -----------------------------

dbutils.widgets.text("business", "commercial")
dbutils.widgets.text("workload_type", "all_purpose")
dbutils.widgets.text("region", "us")
dbutils.widgets.text("env", "dev")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])

business = dbutils.widgets.get("business")
workload_type = dbutils.widgets.get("workload_type")
region = dbutils.widgets.get("region")
env = dbutils.widgets.get("env")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"

mandatory_tags = {
    "business": business,
    "workload_type": workload_type,
    "region": region,
    "env": env
}

# -----------------------------
# 2. Auth setup
# -----------------------------
# Recommended:
# Store PAT token in Databricks secret scope.
#
# Example:
# Scope: gda_scope
# Key: databricks_pat

workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.secrets.get(scope="gda_scope", key="databricks_pat")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# -----------------------------
# 3. Helper functions
# -----------------------------

def api_get(path, params=None):
    url = f"{workspace_url}{path}"
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def api_post(path, payload):
    url = f"{workspace_url}{path}"
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()
    return response.json() if response.text else {}

def list_clusters():
    clusters = []
    next_page_token = None

    while True:
        params = {}
        if next_page_token:
            params["page_token"] = next_page_token

        result = api_get("/api/2.1/clusters/list", params=params)
        clusters.extend(result.get("clusters", []))

        next_page_token = result.get("next_page_token")
        if not next_page_token:
            break

    return clusters

def get_cluster(cluster_id):
    return api_get("/api/2.1/clusters/get", {"cluster_id": cluster_id})

def update_cluster_tags(cluster):
    cluster_id = cluster["cluster_id"]
    cluster_name = cluster.get("cluster_name")

    existing_tags = cluster.get("custom_tags", {}) or {}
    updated_tags = existing_tags.copy()
    updated_tags.update(mandatory_tags)

    if existing_tags == updated_tags:
        return {
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "status": "NO_CHANGE",
            "old_tags": existing_tags,
            "new_tags": updated_tags
        }

    update_payload = {
        "cluster_id": cluster_id,
        "custom_tags": updated_tags
    }

    if dry_run:
        return {
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "status": "DRY_RUN",
            "old_tags": existing_tags,
            "new_tags": updated_tags
        }

    api_post("/api/2.1/clusters/update", update_payload)

    return {
        "cluster_id": cluster_id,
        "cluster_name": cluster_name,
        "status": "UPDATED",
        "old_tags": existing_tags,
        "new_tags": updated_tags
    }

# -----------------------------
# 4. Main execution
# -----------------------------

clusters = list_clusters()

results = []

for c in clusters:
    cluster_id = c["cluster_id"]
    full_cluster = get_cluster(cluster_id)

    # Skip job clusters if you only want all-purpose clusters
    if full_cluster.get("cluster_source") == "JOB":
        continue

    result = update_cluster_tags(full_cluster)
    results.append(result)

# -----------------------------
# 5. Display results
# -----------------------------

result_df = spark.createDataFrame(results)
display(result_df)
