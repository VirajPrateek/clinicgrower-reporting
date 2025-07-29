CREATE OR REPLACE VIEW `dashboard_views.budget_tracker_datewise_optimized` AS
WITH
  MetaSpend AS (
    SELECT
      cgid AS cg_id,
      DATE_TRUNC(report_month, MONTH) AS report_month,
      date_spend,
      SUM(total_spend) AS meta_actual_spend
    FROM
      `dashboard_views.agency_ads_conversions_materialized`
    WHERE
      level = 'Client'
      AND date_spend IS NOT NULL
      AND cgid IS NOT NULL
      AND date_spend BETWEEN DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 3 MONTH) AND CURRENT_DATE('America/New_York')
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
      date_spend BETWEEN DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 3 MONTH) AND CURRENT_DATE('America/New_York')
  ),
  MondayBudgets AS (
    SELECT
      cg_id,
      client_name,
      report_month_date AS report_month,
      COALESCE(fb_monthly_ad_budget, 0) AS fb_monthly_ad_budget,
      COALESCE(google_monthly_ad_budget, 0) AS google_monthly_ad_budget,
      COALESCE(monthly_ad_budget, 0) AS monthly_ad_budget,
      updated_at AS monday_updated_at,
      _airbyte_extracted_at AS monday_extracted_at,
      ROW_NUMBER() OVER (PARTITION BY cg_id, report_month_date ORDER BY updated_at DESC NULLS LAST) AS rn
    FROM
      `dashboard_views.monday_board_mapping_materialized`
    WHERE
      report_month_date IS NOT NULL
      AND report_month_date BETWEEN DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 3 MONTH) AND CURRENT_DATE('America/New_York')
  ),
  CombinedSpend AS (
    SELECT
      COALESCE(ms.cg_id, gs.cg_id) AS cg_id,
      COALESCE(ms.report_month, gs.report_month) AS report_month,
      COALESCE(ms.date_spend, gs.date_spend) AS date_spend,
      COALESCE(ms.meta_actual_spend, 0) AS meta_actual_spend,
      COALESCE(gs.google_ads_actual_spend, 0) AS google_ads_actual_spend
    FROM
      MetaSpend ms
    FULL OUTER JOIN
      GoogleAdsSpend gs
    ON
      LOWER(ms.cg_id) = LOWER(gs.cg_id)
      AND ms.date_spend = gs.date_spend
  )
SELECT
  mb.cg_id,
  mb.client_name,
  mb.monday_updated_at,
  mb.monday_extracted_at,
  cs.report_month,
  cs.date_spend,
  mb.fb_monthly_ad_budget,
  mb.google_monthly_ad_budget,
  mb.monthly_ad_budget,
  cs.meta_actual_spend AS fb_actual_spend,
  cs.google_ads_actual_spend,
  CASE
    WHEN cs.meta_actual_spend > 0 OR cs.google_ads_actual_spend > 0 THEN 'Data Present'
    ELSE 'No Data'
  END AS data_status
FROM
  MondayBudgets mb
LEFT JOIN
  CombinedSpend cs
ON
  LOWER(mb.cg_id) = LOWER(cs.cg_id)
  AND DATE_TRUNC(mb.report_month, MONTH) = DATE_TRUNC(cs.report_month, MONTH)
WHERE
  mb.rn = 1;