WITH TableUnion AS (
  SELECT
    property_id,
    streamName AS stream_name,
    eventName AS event_name,
    startDate AS event_date,
    eventCount AS event_count,
    sessions,
    totalUsers AS total_users,
    activeUsers AS active_users,
    eventCountPerUser AS event_count_per_user
  FROM `clinicgrower-reporting.ga4_roll_up_account_345631103.events__report*`
  WHERE _TABLE_SUFFIX IS NOT NULL
),
ExtractedCGID AS (
  SELECT
    property_id,
    stream_name,
    event_name,
    event_date,
    event_count,
    sessions,
    total_users,
    active_users,
    event_count_per_user,
    REGEXP_EXTRACT(stream_name, r'^([^-\s]+)') AS cgid
  FROM TableUnion
),
AggregatedData AS (
  SELECT
    property_id,
    stream_name,
    cgid,
    event_name,
    DATE_TRUNC(event_date, MONTH) AS report_month,
    event_date,
    SUM(event_count) AS total_event_count,
    SUM(sessions) AS total_sessions,
    SUM(total_users) AS total_users,
    SUM(active_users) AS total_active_users,
    AVG(event_count_per_user) AS avg_event_count_per_user
  FROM ExtractedCGID
  WHERE event_date IS NOT NULL
    AND event_date BETWEEN DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 3 MONTH) AND CURRENT_DATE('America/New_York')
  GROUP BY property_id, stream_name, cgid, event_name, report_month, event_date
),
AgencyLevel AS (
  SELECT
    CAST(NULL AS STRING) AS property_id,
    CAST(NULL AS STRING) AS stream_name,
    CAST(NULL AS STRING) AS cgid,
    event_name,
    report_month,
    event_date,
    SUM(total_event_count) AS total_event_count,
    SUM(total_sessions) AS total_sessions,
    SUM(total_users) AS total_users,
    SUM(total_active_users) AS total_active_users,
    AVG(avg_event_count_per_user) AS avg_event_count_per_user,
    'Agency' AS level
  FROM AggregatedData
  GROUP BY event_name, report_month, event_date
),
ClientLevel AS (
  SELECT
    CAST(property_id AS STRING) AS property_id,
    CAST(stream_name AS STRING) AS stream_name,
    CAST(cgid AS STRING) AS cgid,
    event_name,
    report_month,
    event_date,
    total_event_count,
    total_sessions,
    total_users,
    total_active_users,
    avg_event_count_per_user,
    'Client' AS level
  FROM AggregatedData
)
SELECT
  property_id,
  stream_name,
  cgid,
  event_name,
  report_month,
  event_date,
  total_event_count,
  total_sessions,
  total_users,
  total_active_users,
  avg_event_count_per_user,
  level
FROM (
  SELECT * FROM AgencyLevel
  UNION ALL
  SELECT * FROM ClientLevel
)
ORDER BY report_month DESC, event_date DESC, property_id NULLS FIRST, stream_name, cgid, event_name