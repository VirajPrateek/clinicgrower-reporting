-- Incremental append to ga4_custom_rollup.custom_rollup_events
-- Takes all the GA4-BQ exports events tables (from different datasets analytics_XXX) and creates a rollup table

-- Declare variables for dynamic SQL
DECLARE dataset_list ARRAY<STRING>;
DECLARE query_string STRING;
DECLARE target_date_string STRING;
DECLARE target_date DATE;

-- Set the target date to the previous day in UTC
SET target_date_string = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY));
SET target_date = PARSE_DATE('%Y%m%d', target_date_string);

-- Fetch datasets with the specific events_YYYYMMDD table
SET dataset_list = (
  SELECT ARRAY_AGG(DISTINCT table_schema)
  FROM region-us-central1.INFORMATION_SCHEMA.TABLES
  WHERE table_schema LIKE 'analytics_%'
    AND table_name = 'events_' || target_date_string
);

-- Check if datasets exist
IF ARRAY_LENGTH(dataset_list) = 0 THEN
  INSERT INTO `clinicgrower-reporting.ga4_custom_rollup.update_log` (run_date, dataset_name, table_name, status, message)
  VALUES (CURRENT_DATE('UTC'), NULL, 'events_' || target_date_string, 'SKIPPED', 'No datasets found with events_' || target_date_string);
  RETURN;
END IF;

-- Check if target date is already processed
IF target_date IN (
  SELECT DISTINCT event_date_as_date
  FROM `clinicgrower-reporting.ga4_custom_rollup.custom_rollup_events`
) THEN
  INSERT INTO `clinicgrower-reporting.ga4_custom_rollup.update_log` (run_date, dataset_name, table_name, status, message)
  VALUES (CURRENT_DATE('UTC'), NULL, 'events_' || target_date_string, 'SKIPPED', 'Data for ' || target_date_string || ' already processed');
  RETURN;
END IF;

-- Build dynamic SQL query to append events_ tables for the target date
SET query_string = (
  SELECT STRING_AGG(
    'SELECT *, "' || REGEXP_EXTRACT(schema_name, r'analytics_([0-9]+)') || '" AS property_id, ' ||
    'PARSE_DATE("%Y%m%d", event_date) AS event_date_as_date ' ||
    'FROM `clinicgrower-reporting.' || schema_name || '.events_' || target_date_string || '`',
    ' UNION ALL '
  )
  FROM UNNEST(dataset_list) AS schema_name
);

-- Check if query string is NULL
IF query_string IS NULL THEN
  INSERT INTO `clinicgrower-reporting.ga4_custom_rollup.update_log` (run_date, dataset_name, table_name, status, message)
  VALUES (CURRENT_DATE('UTC'), NULL, 'events_' || target_date_string, 'FAILED', 'Failed to build query for events_' || target_date_string);
  RETURN;
END IF;

-- Append new data to the table
EXECUTE IMMEDIATE '''
INSERT INTO `clinicgrower-reporting.ga4_custom_rollup.custom_rollup_events`
''' || query_string;

-- Log successful update
INSERT INTO `clinicgrower-reporting.ga4_custom_rollup.update_log` (
  run_date, dataset_name, table_name, status, message
)
SELECT
  CURRENT_DATE('UTC'),
  schema_name,
  'events_' || target_date_string,
  'SUCCESS',
  'Appended data from events_' || target_date_string
FROM UNNEST(dataset_list) AS schema_name;