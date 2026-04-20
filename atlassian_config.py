CATALOG = "sandbox"
SCHEMA = "admin"

SECRET_SCOPE = "atlassian"   # change if needed
ORG_ID_SECRET_KEY = "atlassian-org-id"
API_KEY_SECRET_KEY = "atlassian-admin-api-key"

PAGE_SIZE = 100
REQUEST_TIMEOUT_SECS = 60
MAX_RETRIES = 5
SLEEP_SECS_ON_429 = 5

BASE_URL = "https://api.atlassian.com/admin"

# COMMAND ----------

ORG_ID = dbutils.secrets.get(scope=SECRET_SCOPE, key=ORG_ID_SECRET_KEY)
API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=API_KEY_SECRET_KEY)

if not ORG_ID:
    raise ValueError("ORG_ID is empty. Check Databricks secret.")

if not API_KEY:
    raise ValueError("API_KEY is empty. Check Databricks secret.")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print("Configuration loaded successfully.")
print(f"Target schema: {CATALOG}.{SCHEMA}")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="CATALOG", value=CATALOG)
dbutils.jobs.taskValues.set(key="SCHEMA", value=SCHEMA)
dbutils.jobs.taskValues.set(key="SECRET_SCOPE", value=SECRET_SCOPE)
dbutils.jobs.taskValues.set(key="ORG_ID_SECRET_KEY", value=ORG_ID_SECRET_KEY)
dbutils.jobs.taskValues.set(key="API_KEY_SECRET_KEY", value=API_KEY_SECRET_KEY)
dbutils.jobs.taskValues.set(key="PAGE_SIZE", value=str(PAGE_SIZE))
dbutils.jobs.taskValues.set(key="REQUEST_TIMEOUT_SECS", value=str(REQUEST_TIMEOUT_SECS))
dbutils.jobs.taskValues.set(key="MAX_RETRIES", value=str(MAX_RETRIES))
dbutils.jobs.taskValues.set(key="SLEEP_SECS_ON_429", value=str(SLEEP_SECS_ON_429))
dbutils.jobs.taskValues.set(key="BASE_URL", value=BASE_URL)

print("Task values set.")
