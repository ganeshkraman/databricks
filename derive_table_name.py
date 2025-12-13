def derive_table_name(hostname: str, suffix: str) -> str:
    # Derive table name from Databricks hostname.
    # Extract the part before the first dot
    core = hostname.split('.')[0]                     #

    # Split by '-' and take region + env
    parts = core.split('-')                           
    region = parts[-2]
    env = parts[-1]

    # Construct table name
    return f"{region}{env}_{suffix}".lower()















