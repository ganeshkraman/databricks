CREATE OR REPLACE VIEW group_permissions AS

SELECT
  'CATALOG' AS object_type,
  catalog_name AS catalog_name,
  NULL AS schema_name,
  NULL AS table_name,
  grantee AS group_name,
  privilege_type
FROM system.information_schema.catalog_privileges

UNION ALL

SELECT
  'SCHEMA' AS object_type,
  catalog_name,
  schema_name,
  NULL AS table_name,
  grantee AS group_name,
  privilege_type
FROM system.information_schema.schema_privileges

UNION ALL

SELECT
  'TABLE' AS object_type,
  table_catalog AS catalog_name,
  table_schema AS schema_name,
  table_name,
  grantee AS group_name,
  privilege_type
FROM system.information_schema.table_privileges;
