CREATE OR REPLACE VIEW `dashboard_views.metaads_ga4_spend_leads_pivot`
AS
WITH StreamMapping AS (
  SELECT
    cg_id AS cgid,
    MAX(ga4_property_id) AS property_id,
    MAX(client_name) AS client_name
  FROM `dashboard_views.monday_board_mapping_materialized`
  WHERE cg_id IS NOT NULL
  GROUP BY cg_id
),
EventsAggregated AS (
  SELECT
    e.property_id,
    e.event_date_as_date AS report_date,
    e.collected_traffic_source.manual_campaign_name AS campaign_name,
    e.collected_traffic_source.manual_content AS ad_name,
    e.collected_traffic_source.manual_medium AS adset_name,
    COUNTIF(e.event_name IN ('subscribe_form_meta_ghl', 'subscribe_form_google_ghl', 'subscribe_survey_meta_ghl', 'subscribe_survey_google_ghl')) AS lead_submissions
  FROM `clinicgrower-reporting.ga4_custom_rollup.custom_rollup_events` e
  INNER JOIN StreamMapping smap
    ON e.property_id = smap.property_id
  WHERE e.event_date_as_date IS NOT NULL
  GROUP BY e.property_id, e.event_date_as_date, e.collected_traffic_source.manual_campaign_name, e.collected_traffic_source.manual_content, e.collected_traffic_source.manual_medium
),
MappedEvents AS (
  SELECT
    smap.client_name,
    smap.cgid,
    e.report_date,
    e.campaign_name,
    e.ad_name,
    e.adset_name,
    e.lead_submissions
  FROM EventsAggregated e
  LEFT JOIN StreamMapping smap
    ON e.property_id = smap.property_id
),
DateSpendAgg AS (
  SELECT
    cgid,
    client_name,
    date_spend AS report_date,
    campaign_name,
    adset_name,
    ad_name,
    SUM(total_spend) AS total_spend_agg,
    MAX(total_impressions) AS total_impressions,
    MAX(total_clicks) AS total_clicks,
    MAX(avg_cpc) AS avg_cpc,
    MAX(avg_cpm) AS avg_cpm,
    MAX(avg_ctr) AS avg_ctr,
    MAX(total_reach) AS total_reach,
    MAX(total_unique_clicks) AS total_unique_clicks
  FROM `dashboard_views.metaads_combined_materialized`
  GROUP BY cgid, client_name, date_spend, campaign_name, adset_name, ad_name
),
JoinedData AS (
  SELECT
    COALESCE(m.cgid, a.cgid) AS cgid,
    COALESCE(m.client_name, a.client_name, 'Unknown') AS client_name,
    COALESCE(m.report_date, a.report_date) AS report_date,
    COALESCE(m.campaign_name, a.campaign_name, '(no campaign)') AS campaign_name,
    COALESCE(m.adset_name, a.adset_name, '(no adset)') AS adset_name,
    COALESCE(m.ad_name, a.ad_name, '(no ad)') AS ad_name,
    COALESCE(m.lead_submissions, 0) AS lead_submissions,
    COALESCE(a.total_spend_agg, 0) AS total_spend,
    COALESCE(a.total_impressions, 0) AS total_impressions,
    COALESCE(a.total_clicks, 0) AS total_clicks,
    COALESCE(a.avg_cpc, 0) AS avg_cpc,
    COALESCE(a.avg_cpm, 0) AS avg_cpm,
    COALESCE(a.avg_ctr, 0) AS avg_ctr,
    COALESCE(a.total_reach, 0) AS total_reach,
    COALESCE(a.total_unique_clicks, 0) AS total_unique_clicks
  FROM MappedEvents m
  FULL OUTER JOIN DateSpendAgg a
    ON m.cgid = a.cgid
    AND m.report_date = a.report_date
    AND COALESCE(m.campaign_name, '(no campaign)') = COALESCE(a.campaign_name, '(no campaign)')
    AND COALESCE(m.adset_name, '(no adset)') = COALESCE(a.adset_name, '(no adset)')
    AND COALESCE(m.ad_name, '(no ad)') = COALESCE(a.ad_name, '(no ad)')
)
SELECT
  cgid,
  client_name,
  DATE_TRUNC(report_date, MONTH) AS report_month,
  report_date,
  campaign_name,
  adset_name,
  ad_name,
  lead_submissions,
  total_spend,
  total_impressions,
  total_clicks,
  avg_cpc,
  avg_cpm,
  avg_ctr,
  total_reach,
  total_unique_clicks
FROM JoinedData
ORDER BY report_date DESC, cgid, client_name, campaign_name, adset_name, ad_name