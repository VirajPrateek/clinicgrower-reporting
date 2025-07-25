CREATE OR REPLACE TABLE `dashboard_views.metaads_combined_materialized`
PARTITION BY DATE_TRUNC(report_month, MONTH)
CLUSTER BY cgid, campaign_name, adset_name, date_spend
AS
WITH AdsSpend AS (
  SELECT
    REGEXP_EXTRACT(campaign_name, r'^(CG0*\d+)') AS cgid,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    adset_id,
    adset_name,
    ad_id,
    ad_name,
    DATE_TRUNC(date_start, MONTH) AS report_month,
    date_start AS date_spend,
    ROUND(SUM(COALESCE(spend, 0)), 2) AS total_spend,  -- Added rounding to 2 decimals
    SUM(COALESCE(impressions, 0)) AS total_impressions,
    SUM(COALESCE(clicks, 0)) AS total_clicks,
    SUM(COALESCE(reach, 0)) AS total_reach,
    SUM(COALESCE(unique_clicks, 0)) AS total_unique_clicks,
    SUM(
      COALESCE(
        (SELECT SAFE_CAST(JSON_EXTRACT_SCALAR(action, '$.value') AS INT64)
         FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS action
         WHERE JSON_EXTRACT_SCALAR(action, '$.action_type') = 'lead'),
        0
      )
    ) AS meta_leads,
    SUM(
      COALESCE(
        (SELECT SAFE_CAST(JSON_EXTRACT_SCALAR(conversion, '$.value') AS INT64)
         FROM UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS conversion
         WHERE JSON_EXTRACT_SCALAR(conversion, '$.action_type') = 'offsite_conversion.fb_pixel_lead'),
        0
      )
    ) AS total_offsite_leads,
    SUM(
      COALESCE(
        (SELECT SAFE_CAST(JSON_EXTRACT_SCALAR(conversion, '$.value') AS INT64)
         FROM UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS conversion
         WHERE JSON_EXTRACT_SCALAR(conversion, '$.action_type') = 'schedule_total'),
        0
      )
    ) AS total_schedules,
    SUM(
      COALESCE(
        (SELECT SAFE_CAST(JSON_EXTRACT_SCALAR(conversion, '$.value') AS INT64)
         FROM UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS conversion
         WHERE JSON_EXTRACT_SCALAR(conversion, '$.action_type') = 'schedule_website'),
        0
      )
    ) AS total_schedule_website,
    SUM(
      COALESCE(
        (SELECT SAFE_CAST(JSON_EXTRACT_SCALAR(conversion, '$.value') AS INT64)
         FROM UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS conversion
         WHERE JSON_EXTRACT_SCALAR(conversion, '$.action_type') = 'submit_application_total'),
        0
      )
    ) AS total_submit_applications,
    SUM(
      COALESCE(
        (SELECT SAFE_CAST(JSON_EXTRACT_SCALAR(conversion, '$.value') AS INT64)
         FROM UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS conversion
         WHERE JSON_EXTRACT_SCALAR(conversion, '$.action_type') = 'submit_application_website'),
        0
      )
    ) AS total_submit_application_website,
    SUM(
      COALESCE(
        (SELECT SAFE_CAST(JSON_EXTRACT_SCALAR(conversion, '$.value') AS INT64)
         FROM UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS conversion
         WHERE JSON_EXTRACT_SCALAR(conversion, '$.action_type') = 'subscribe_total'),
        0
      )
    ) AS total_subscriptions,
    SUM(
      COALESCE(
        (SELECT SAFE_CAST(JSON_EXTRACT_SCALAR(conversion, '$.value') AS INT64)
         FROM UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS conversion
         WHERE JSON_EXTRACT_SCALAR(conversion, '$.action_type') = 'subscribe_website'),
        0
      )
    ) AS total_subscribe_website
  FROM `clinicgrower-reporting.meta_ads_new.*`
  WHERE _TABLE_SUFFIX LIKE '%_ads_insights'
    AND date_start IS NOT NULL
    AND campaign_name IS NOT NULL
    AND REGEXP_CONTAINS(campaign_name, r'^CG0*\d+')
  GROUP BY
    cgid,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    adset_id,
    adset_name,
    ad_id,
    ad_name,
    report_month,
    date_spend
),
AdsMetrics AS (
  SELECT
    a.cgid,
    a.account_id,
    a.account_name,
    m.client_name,
    a.campaign_id,
    a.campaign_name,
    a.adset_id,
    a.adset_name,
    a.ad_id,
    a.ad_name,
    a.report_month,
    a.date_spend,
    a.total_spend,
    a.total_impressions,
    a.total_clicks,
    a.total_reach,
    a.total_unique_clicks,
    a.meta_leads,
    a.total_offsite_leads,
    a.total_schedules,
    a.total_schedule_website,
    a.total_submit_applications,
    a.total_submit_application_website,
    a.total_subscriptions,
    a.total_subscribe_website,
    SAFE_DIVIDE(a.total_spend, a.total_clicks) AS avg_cpc,
    SAFE_DIVIDE(a.total_spend, a.total_impressions) * 1000 AS avg_cpm,
    SAFE_DIVIDE(a.total_clicks, a.total_impressions) * 100 AS avg_ctr
  FROM AdsSpend a
  LEFT JOIN `dashboard_views.monday_board_mapping_materialized` m
    ON a.cgid = m.cg_id
  WHERE a.total_impressions > 0
)
SELECT
  cgid,
  account_id,
  account_name,
  client_name,
  campaign_id,
  campaign_name,
  adset_id,
  adset_name,
  ad_id,
  ad_name,
  report_month,
  date_spend,
  total_spend,
  total_impressions,
  total_clicks,
  avg_cpc,
  avg_cpm,
  avg_ctr,
  total_reach,
  total_unique_clicks,
  meta_leads,
  total_offsite_leads,
  total_schedules,
  total_schedule_website,
  total_submit_applications,
  total_submit_application_website,
  total_subscriptions,
  total_subscribe_website
FROM AdsMetrics