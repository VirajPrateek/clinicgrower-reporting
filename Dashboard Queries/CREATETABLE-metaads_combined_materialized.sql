CREATE OR REPLACE TABLE `dashboard_views.metaads_combined_materialized`
PARTITION BY DATE_TRUNC(report_month, MONTH)
CLUSTER BY cgid, campaign_name, adset_name, date_spend
AS
WITH RawAds AS (
    SELECT
        REGEXP_EXTRACT(UPPER(TRIM(campaign_name)), r'^(CG0*\d+)') AS cgid,
        account_id,
        account_name,
        campaign_id,
        TRIM(campaign_name) AS campaign_name,
        adset_id,
        TRIM(adset_name) AS adset_name,
        ad_id,
        TRIM(ad_name) AS ad_name,
        date_start,
        ROUND(COALESCE(spend, 0), 2) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(reach, 0) AS reach,
        COALESCE(unique_clicks, 0) AS unique_clicks,
        _TABLE_SUFFIX AS table_suffix,
        _airbyte_extracted_at,
        FARM_FINGERPRINT(CONCAT(
            CAST(account_id AS STRING),
            CAST(campaign_id AS STRING),
            CAST(adset_id AS STRING),
            CAST(ad_id AS STRING),
            CAST(date_start AS STRING),
            CAST(ROUND(COALESCE(spend, 0), 2) AS STRING),
            CAST(COALESCE(impressions, 0) AS STRING),
            CAST(COALESCE(clicks, 0) AS STRING),
            CAST(COALESCE(reach, 0) AS STRING),
            CAST(COALESCE(unique_clicks, 0) AS STRING)
        )) AS row_checksum,
        ROW_NUMBER() OVER (
            PARTITION BY
                account_id,
                campaign_id,
                adset_id,
                ad_id,
                date_start,
                FARM_FINGERPRINT(CONCAT(
                    CAST(ROUND(COALESCE(spend, 0), 2) AS STRING),
                    CAST(COALESCE(impressions, 0) AS STRING),
                    CAST(COALESCE(clicks, 0) AS STRING),
                    CAST(COALESCE(reach, 0) AS STRING),
                    CAST(COALESCE(unique_clicks, 0) AS STRING)
                ))
            ORDER BY _airbyte_extracted_at DESC
        ) AS rn
    FROM `clinicgrower-reporting.meta_ads_new.*`
    WHERE _TABLE_SUFFIX LIKE '%_ads_insights'
      AND date_start IS NOT NULL
      AND campaign_name IS NOT NULL
      AND REGEXP_CONTAINS(UPPER(TRIM(campaign_name)), r'^CG0*\d+')
      AND date_start >= DATE('2024-11-01')
      AND impressions > 0
),
AdsSpend AS (
    SELECT
        cgid,
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
        ROUND(SUM(spend), 2) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(reach) AS total_reach,
        SUM(unique_clicks) AS total_unique_clicks,
        CURRENT_DATE() AS row_added_date,
        MAX(table_suffix) AS table_suffix
    FROM RawAds
    WHERE rn = 1
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
        a.row_added_date,
        a.table_suffix,
        SAFE_DIVIDE(a.total_spend, a.total_clicks) AS avg_cpc,
        SAFE_DIVIDE(a.total_spend, a.total_impressions) * 1000 AS avg_cpm,
        SAFE_DIVIDE(a.total_clicks, a.total_impressions) * 100 AS avg_ctr
    FROM AdsSpend a
    LEFT JOIN (
        SELECT DISTINCT cg_id, client_name
        FROM `dashboard_views.monday_board_mapping_materialized`
    ) m
        ON a.cgid = m.cg_id
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
    row_added_date,
    table_suffix
FROM AdsMetrics;