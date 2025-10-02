CREATE OR REPLACE VIEW `dashboard_views.budget_tracker_datewise_optimized` AS
WITH
  MetaSpend AS (
    SELECT
      cgid AS cg_id,
      DATE_TRUNC(date_spend, MONTH) AS report_month,
      date_spend,
      SUM(total_spend) AS meta_actual_spend,
      MAX(client_name) AS meta_client_name  -- Get client_name for fallback
    FROM
      `dashboard_views.metaads_combined_materialized`
    WHERE
      cgid IS NOT NULL
      AND date_spend IS NOT NULL
    GROUP BY
      cgid,
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
      `dashboard_views.google_ads_budget_tracker_datewise_view`
    WHERE
      cg_id IS NOT NULL
      AND date_spend IS NOT NULL
  ),
  AggregatedSpend AS (
    SELECT
      COALESCE(meta.cg_id, google.cg_id) AS cg_id,
      COALESCE(meta.report_month, google.report_month) AS report_month,
      COALESCE(meta.date_spend, google.date_spend) AS date_spend,
      COALESCE(meta.meta_actual_spend, 0) AS meta_actual_spend,
      COALESCE(google.google_ads_actual_spend, 0) AS google_ads_actual_spend,
      meta.meta_client_name
    FROM
      MetaSpend meta
    FULL OUTER JOIN
      GoogleAdsSpend google
    ON
      LOWER(meta.cg_id) = LOWER(google.cg_id)
      AND meta.date_spend = google.date_spend
  ),
  DeduplicatedBudgets AS (
    SELECT
      cg_id,
      client_name,
      updated_at AS monday_updated_at,
      _airbyte_extracted_at AS monday_extracted_at,
      report_month_date AS report_month,
      COALESCE(fb_monthly_ad_budget, 0) AS fb_monthly_ad_budget,
      COALESCE(monthly_ad_budget, 0) AS monthly_ad_budget,
      COALESCE(google_monthly_ad_budget, 0) AS google_monthly_ad_budget,
      ROW_NUMBER() OVER (PARTITION BY cg_id, report_month_date ORDER BY updated_at DESC NULLS LAST) AS rn
    FROM
      `dashboard_views.monday_board_mapping_materialized`
    WHERE
      cg_id IS NOT NULL
      AND report_month_date IS NOT NULL
  )
SELECT
  COALESCE(sp.cg_id, db.cg_id) AS cg_id,
  COALESCE(db.client_name, sp.meta_client_name, 'Unknown') AS client_name,
  db.monday_updated_at,
  db.monday_extracted_at,
  COALESCE(sp.report_month, db.report_month) AS report_month,
  COALESCE(sp.date_spend, db.report_month) AS date_spend,
  COALESCE(db.fb_monthly_ad_budget, 0) AS fb_monthly_ad_budget,
  COALESCE(db.monthly_ad_budget, 0) AS monthly_ad_budget,
  COALESCE(db.google_monthly_ad_budget, 0) AS google_monthly_ad_budget,
  COALESCE(sp.meta_actual_spend, 0) AS meta_actual_spend,
  COALESCE(sp.google_ads_actual_spend, 0) AS google_ads_actual_spend,
  CASE
    WHEN sp.meta_actual_spend > 0 OR sp.google_ads_actual_spend > 0 THEN 'Data Present'
    WHEN sp.cg_id IS NOT NULL THEN 'Spend Data Missing'
    ELSE 'No Spend Data'
  END AS data_status,
  CASE WHEN sp.cg_id IS NULL AND db.cg_id IS NOT NULL THEN 'CGID Missing in Spend Data' END AS debug_cgid_issue,
  CASE WHEN sp.date_spend IS NULL AND sp.cg_id IS NOT NULL THEN 'Date Spend Missing in Spend Data' END AS debug_date_spend_issue
FROM
  AggregatedSpend sp
FULL OUTER JOIN
  DeduplicatedBudgets db
ON
  LOWER(db.cg_id) = LOWER(sp.cg_id)
  AND DATE_TRUNC(db.report_month, MONTH) = DATE_TRUNC(sp.date_spend, MONTH)
WHERE
  (db.rn = 1 OR db.rn IS NULL)
  AND (sp.report_month IS NOT NULL OR db.report_month IS NOT NULL);