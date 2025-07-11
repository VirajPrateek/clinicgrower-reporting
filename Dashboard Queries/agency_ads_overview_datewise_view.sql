WITH
  -- Aggregate Ads Insights Data with Daily Granularity
  AdsSpend AS (
  SELECT
    CONCAT('CG', SAFE_CAST(REGEXP_EXTRACT(campaign_name, r'^CG0*(\d+)') AS INT64)) AS cg_id,
    account_id,
    account_name,
    DATE_TRUNC(date_start, MONTH) AS report_month,
    date_start AS date_spend,  -- New column for daily granularity
    SUM(COALESCE(spend, 0)) AS total_spend,
    SUM(COALESCE(impressions, 0)) AS total_impressions,
    SUM(COALESCE(clicks, 0)) AS total_clicks,
    SUM(COALESCE(reach, 0)) AS total_reach,
    SUM(COALESCE(unique_clicks, 0)) AS total_unique_clicks
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
    account_id,
    account_name,
    report_month,
    date_spend
  ),
  -- Calculate Derived Metrics Post-Aggregation
  AdsMetrics AS (
  SELECT
    cg_id,
    account_id,
    account_name,
    report_month,
    date_spend,  -- Carry forward daily granularity
    total_spend,
    total_impressions,
    total_clicks,
    total_reach,
    total_unique_clicks,
    SAFE_DIVIDE(total_spend, total_clicks) AS avg_cpc,  -- Spend per click
    SAFE_DIVIDE(total_spend, total_impressions) * 1000 AS avg_cpm,  -- Cost per mille
    SAFE_DIVIDE(total_clicks, total_impressions) * 100 AS avg_ctr  -- Click-through rate
  FROM AdsSpend
  WHERE total_impressions > 0  -- Avoid division by zero
  ),
  -- Agency and Client Level Aggregation
  AgencyAdsOverview AS (
  SELECT
    NULL AS cg_id,  -- Agency-wide row
    NULL AS account_id,
    NULL AS account_name,
    report_month,
    date_spend,  -- Include daily granularity
    SUM(total_spend) AS total_spend,
    SUM(total_impressions) AS total_impressions,
    SUM(total_clicks) AS total_clicks,
    SUM(total_reach) AS total_reach,
    SUM(total_unique_clicks) AS total_unique_clicks,
    SAFE_DIVIDE(SUM(total_spend), SUM(total_clicks)) AS avg_cpc,
    SAFE_DIVIDE(SUM(total_spend), SUM(total_impressions)) * 1000 AS avg_cpm,
    SAFE_DIVIDE(SUM(total_clicks), SUM(total_impressions)) * 100 AS avg_ctr,
    'Agency' AS level
  FROM AdsMetrics
  GROUP BY report_month, date_spend

  UNION ALL

  SELECT
    cg_id,
    account_id,
    account_name,
    report_month,
    date_spend,  -- Include daily granularity
    total_spend,
    total_impressions,
    total_clicks,
    total_reach,
    total_unique_clicks,
    avg_cpc,
    avg_cpm,
    avg_ctr,
    'Client' AS level
  FROM AdsMetrics
  )
SELECT
  cg_id,
  account_id,
  account_name,
  report_month,
  date_spend,  -- New daily column for Looker Studio
  total_spend,
  total_impressions,
  total_clicks,
  avg_cpc,
  avg_cpm,
  avg_ctr,
  total_reach,
  total_unique_clicks,
  level
FROM AgencyAdsOverview
ORDER BY report_month DESC, date_spend DESC, cg_id NULLS FIRST