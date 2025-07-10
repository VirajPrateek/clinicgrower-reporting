#standardSQL
CREATE OR REPLACE VIEW
  `clinicgrower-reporting.dashboard_views.budget_tracker_datewise_view` AS
WITH
  MetaSpend AS (
    SELECT
      REGEXP_EXTRACT(campaign_name, r'^CG0*\d+') AS cg_id,
      _TABLE_SUFFIX AS debug_table_suffix,
      DATE_TRUNC(date_start, MONTH) AS report_month,
      date_start AS date_spend,
      SUM(COALESCE(spend, 0)) AS total_spend
    FROM
      `clinicgrower-reporting.meta_ads_new.*`
    WHERE
      _TABLE_SUFFIX LIKE '%_ads_insights'
      AND date_start IS NOT NULL
      AND date_start BETWEEN DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 3 MONTH) AND CURRENT_DATE('America/New_York')
      AND campaign_name IS NOT NULL
      AND REGEXP_CONTAINS(campaign_name, r'^CG0*\d+')
    GROUP BY
      cg_id,
      debug_table_suffix,
      report_month,
      date_spend
  ),
  GoogleAdsSpend AS (
    SELECT
      cg_id,
      report_month,
      date_spend,
      google_ads_actual_spend
    FROM
      `clinicgrower-reporting.dashboard_views.google_ads_budget_tracker_datewise_view`
  ),
  AggregatedMonthlySpend AS (
    SELECT
      COALESCE(meta.cg_id, google.cg_id) AS cg_id,
      COALESCE(meta.report_month, google.report_month) AS report_month,
      COALESCE(meta.date_spend, google.date_spend) AS date_spend,
      COALESCE(meta.total_spend, 0) AS meta_actual_spend,
      COALESCE(google.google_ads_actual_spend, 0) AS google_ads_actual_spend,
      meta.debug_table_suffix AS meta_debug_table_suffix
    FROM
      MetaSpend meta
    FULL OUTER JOIN
      GoogleAdsSpend google
    ON
      LOWER(meta.cg_id) = LOWER(google.cg_id)
      AND meta.date_spend = google.date_spend
  ),
  DeduplicatedItems AS (
    SELECT
      cg_id,
      client_name,
      monday_updated_at,
      monday_extracted_at,
      report_month,
      fb_monthly_ad_budget,
      monthly_ad_budget,
      google_monthly_ad_budget,
      ROW_NUMBER() OVER (PARTITION BY cg_id, report_month ORDER BY monday_updated_at DESC NULLS LAST) AS rn
    FROM (
      SELECT
        cg_id,
        client_name,
        updated_at AS monday_updated_at,
        _airbyte_extracted_at AS monday_extracted_at,
        SAFE.PARSE_DATE('%Y-%m', report_month) AS report_month,
        fb_monthly_ad_budget,
        monthly_ad_budget,
        google_monthly_ad_budget
      FROM
        `clinicgrower-reporting.dashboard_views.monday_budget_rollup`
      UNION ALL
      SELECT
        cg_id,
        NULL AS client_name,
        NULL AS monday_updated_at,
        NULL AS monday_extracted_at,
        report_month,
        0 AS fb_monthly_ad_budget,
        0 AS monthly_ad_budget,
        0 AS google_monthly_ad_budget
      FROM (
        SELECT
          cg_id,
          report_month
        FROM AggregatedMonthlySpend
      ) defaults
    ) combined
  )
SELECT
  di.cg_id,
  di.client_name,
  di.monday_updated_at,
  di.monday_extracted_at,
  ams.report_month,
  ams.date_spend,
  COALESCE(di.fb_monthly_ad_budget, 0) AS fb_monthly_ad_budget,
  COALESCE(di.monthly_ad_budget, 0) AS monthly_ad_budget,
  COALESCE(di.google_monthly_ad_budget, 0) AS google_monthly_ad_budget,
  COALESCE(ams.meta_actual_spend, 0) AS meta_actual_spend,
  COALESCE(ams.google_ads_actual_spend, 0) AS google_ads_actual_spend,
  'Data Present' AS data_status
FROM
  DeduplicatedItems di
LEFT JOIN
  AggregatedMonthlySpend ams
ON
  LOWER(di.cg_id) = LOWER(ams.cg_id)
  AND DATE_TRUNC(di.report_month, MONTH) = ams.report_month
WHERE
  di.rn = 1