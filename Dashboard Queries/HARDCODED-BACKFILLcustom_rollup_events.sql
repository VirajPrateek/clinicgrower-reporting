-- Declare variables
DECLARE dataset_list ARRAY<STRING>;
DECLARE query_string STRING;
DECLARE target_date_string STRING;
DECLARE target_date DATE;
DECLARE date_array ARRAY<DATE>;
DECLARE i INT64;

-- Define the date range (June 4, 2025 to July 8, 2025)
SET date_array = ARRAY<DATE>[
  '2025-06-04', '2025-06-05', '2025-06-06', '2025-06-07', '2025-06-08',
  '2025-06-09', '2025-06-10', '2025-06-11', '2025-06-12', '2025-06-13',
  '2025-06-14', '2025-06-15', '2025-06-16', '2025-06-17', '2025-06-18',
  '2025-06-19', '2025-06-20', '2025-06-21', '2025-06-22', '2025-06-23',
  '2025-06-24', '2025-06-25', '2025-06-26', '2025-06-27', '2025-06-28',
  '2025-06-29', '2025-06-30', '2025-07-01', '2025-07-02', '2025-07-03',
  '2025-07-04', '2025-07-05', '2025-07-06', '2025-07-07', '2025-07-08'
];

-- Loop through the date array
SET i = 0;
WHILE i < ARRAY_LENGTH(date_array) DO
  SET target_date = date_array[OFFSET(i)];
  SET target_date_string = FORMAT_DATE('%Y%m%d', target_date);

  -- Fetch datasets with the specific events_YYYYMMDD table
  SET dataset_list = (
    SELECT ARRAY_AGG(DISTINCT table_schema)
    FROM `clinicgrower-reporting.region-us-central1.INFORMATION_SCHEMA.TABLES`
    WHERE table_schema LIKE 'analytics_%'
      AND table_name = 'events_' || target_date_string
  );

  -- Check if datasets exist
  IF ARRAY_LENGTH(dataset_list) = 0 THEN
    INSERT INTO `clinicgrower-reporting.ga4_custom_rollup.update_log` (
      run_date, dataset_name, table_name, status, message
    )
    VALUES (
      CURRENT_DATE('UTC'),
      NULL,
      'events_' || target_date_string,
      'SKIPPED',
      'No datasets found with events_' || target_date_string
    );
  ELSE
    -- Check if target date is already processed
    IF target_date IN (
      SELECT DISTINCT event_date_as_date
      FROM `clinicgrower-reporting.ga4_custom_rollup.custom_rollup_events`
    ) THEN
      INSERT INTO `clinicgrower-reporting.ga4_custom_rollup.update_log` (
        run_date, dataset_name, table_name, status, message
      )
      VALUES (
        CURRENT_DATE('UTC'),
        NULL,
        'events_' || target_date_string,
        'SKIPPED',
        'Data for ' || target_date_string || ' already processed'
      );
    ELSE
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
        INSERT INTO `clinicgrower-reporting.ga4_custom_rollup.update_log` (
          run_date, dataset_name, table_name, status, message
        )
        VALUES (
          CURRENT_DATE('UTC'),
          NULL,
          'events_' || target_date_string,
          'FAILED',
          'Failed to build query for events_' || target_date_string
        );
      ELSE
        -- Append new data to the table
        EXECUTE IMMEDIATE '''
        INSERT INTO `clinicgrower-reporting.ga4_custom_rollup.custom_rollup_events`
        ''' || query_string;

        -- Log successful update with row count
        INSERT INTO `clinicgrower-reporting.ga4_custom_rollup.update_log` (
          run_date, dataset_name, table_name, status, message
        )
        SELECT
          CURRENT_DATE('UTC'),
          schema_name,
          'events_' || target_date_string,
          'SUCCESS',
          'Appended ' || (
            SELECT COUNT(*)
            FROM `clinicgrower-reporting.ga4_custom_rollup.custom_rollup_events`
            WHERE event_date_as_date = target_date
          ) || ' rows from events_' || target_date_string
        FROM UNNEST(dataset_list) AS schema_name;
      END IF;
    END IF;
  END IF;

  -- Increment the loop counter
  SET i = i + 1;
END WHILE;