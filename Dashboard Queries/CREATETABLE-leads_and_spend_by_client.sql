CREATE OR REPLACE TABLE `clinicgrower-reporting.dashboard_views.leads_and_spend_by_client`
PARTITION BY DATE_TRUNC(event_date, MONTH)
CLUSTER BY cgid, campaign_name
AS
WITH EventsAggregated AS (
  SELECT
    property_id,
    event_date_as_date AS event_date,
    collected_traffic_source.manual_campaign_name AS campaign_name,
    collected_traffic_source.manual_content AS ad_name,
    collected_traffic_source.manual_medium AS adset_name,
    COUNT(*) AS lead_count
  FROM
    `clinicgrower-reporting.ga4_custom_rollup.custom_rollup_events`
  WHERE
    event_name IN (
      'subscribe_survey_meta_ghl',
      'subscribe_form_meta_ghl',
      'subscribe_form_google_ghl',
      'subscribe_survey_google_ghl'
    )
  GROUP BY
    property_id,
    event_date_as_date,
    collected_traffic_source.manual_campaign_name,
    collected_traffic_source.manual_content,
    collected_traffic_source.manual_medium
),
MappedEvents AS (
  SELECT
    m.client_name,
    m.cg_id AS cgid,
    e.event_date,
    e.campaign_name,
    e.ad_name,
    e.adset_name,
    e.lead_count
  FROM
    EventsAggregated e
  LEFT JOIN
    `clinicgrower-reporting.dashboard_views.monday_board_mapping_materialized` m
    ON e.property_id = m.ga4_property_id
)
SELECT
  m.client_name,
  m.cgid,
  m.event_date,
  m.campaign_name,
  m.ad_name,
  m.adset_name,
  COALESCE(m.lead_count, 0) AS lead_count,
  COALESCE(SUM(a.total_spend), 0) AS total_spend
FROM
  MappedEvents m
LEFT JOIN
  `clinicgrower-reporting.dashboard_views.simplified_meta_ads_spend` a
  ON m.cgid = a.cgid
  AND m.event_date = a.date_spend
  AND LOWER(TRIM(COALESCE(m.campaign_name, ''))) = LOWER(TRIM(COALESCE(a.campaign_name, '')))
GROUP BY
  m.client_name,
  m.cgid,
  m.event_date,
  m.campaign_name,
  m.ad_name,
  m.adset_name,
  m.lead_count;