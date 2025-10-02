CREATE OR REPLACE VIEW `clinicgrower-reporting.dashboard_views.googleads_ga4_spend_leads_pivot` AS
WITH StreamMapping AS (
  SELECT
    m1.cg_id,
    MAX(m1.ga4_property_id) AS property_id,
    MAX(m1.client_name) AS client_name,
    MAX(m1.google_ad_id) AS google_ad_id,
    MAX(m1.google_monthly_ad_budget) AS google_monthly_ad_budget
  FROM `clinicgrower-reporting.dashboard_views.monday_board_mapping_materialized` m1
  WHERE m1.cg_id IS NOT NULL
  GROUP BY m1.cg_id
),
GoogleAdsAggregated AS (
  SELECT
    m.cg_id,
    m.client_name,
    m.google_monthly_ad_budget,
    g.segments_date AS report_date,
    g.campaign_id,
    g.campaign_name,
    g.ad_group_name,
    g.ad_group_ad_ad_name,
    SUM(g.metrics_cost_micros) / 1000000.0 AS total_spend,
    SUM(g.metrics_impressions) AS total_impressions,
    SUM(g.metrics_clicks) AS total_clicks,
    AVG(g.metrics_average_cpc) / 1000000.0 AS avg_cpc,
    SUM(g.metrics_cost_micros) / NULLIF(SUM(g.metrics_impressions), 0) * 1000.0 / 1000000.0 AS avg_cpm,
    AVG(g.metrics_ctr) AS avg_ctr
  FROM `clinicgrower-reporting.google_ads_mcc.ads_mcc_daily_ad_metrics_6215764695` g
  INNER JOIN `clinicgrower-reporting.dashboard_views.monday_board_mapping_materialized` m
    ON CAST(g.customer_id AS STRING) = REPLACE(m.google_ad_id, '-', '')
  WHERE g.segments_date IS NOT NULL
    AND (m.cg_id != 'CG284' OR g.campaign_name LIKE 'CG284%' OR g.campaign_name IS NULL OR g.campaign_name = '')
  GROUP BY m.cg_id, m.client_name, m.google_monthly_ad_budget, g.segments_date, g.campaign_id, g.campaign_name, g.ad_group_name, g.ad_group_ad_ad_name
),
EventsAggregated AS (
  SELECT
    smap.cg_id,
    smap.client_name,
    smap.google_monthly_ad_budget,
    e.property_id,
    e.event_date_as_date AS report_date,
    CAST(e.collected_traffic_source.manual_campaign_id AS STRING) AS campaign_id,
    e.collected_traffic_source.manual_campaign_name AS campaign_name,
    e.collected_traffic_source.manual_content AS ad_group_name,
    e.collected_traffic_source.manual_source AS manual_source,
    COUNTIF(e.event_name = 'subscribe_survey_meta_ghl') AS subscribe_survey_meta_ghl,
    COUNTIF(e.event_name = 'subscribe_form_meta_ghl') AS subscribe_form_meta_ghl,
    COUNTIF(e.event_name = 'subscribe_form_google_ghl') AS subscribe_form_google_ghl,
    COUNTIF(e.event_name = 'subscribe_survey_google_ghl') AS subscribe_survey_google_ghl,
    COUNTIF(e.event_name = 'subscribe_call_meta_ghl') AS subscribe_call_meta_ghl,
    COUNTIF(e.event_name = 'subscribe_call_google_ghl') AS subscribe_call_google_ghl,
    COUNTIF(e.event_name = 'subscribe_call_citations_ghl') AS subscribe_call_citations_ghl,
    COUNTIF(e.event_name = 'subscribe_call_website_ghl') AS subscribe_call_website_ghl,
    COUNTIF(e.event_name = 'subscribe_call_press_ghl') AS subscribe_call_press_ghl,
    COUNTIF(e.event_name = 'subscribe_call_gbp_ghl') AS subscribe_call_gbp_ghl,
    COUNTIF(e.event_name = 'subscribe_call_main_ghl') AS subscribe_call_main_ghl,
    COUNTIF(e.event_name = 'subscribe_call_seo_ghl') AS subscribe_call_seo_ghl,
    COUNTIF(e.event_name = 'subscribe_fb_messenger_ghl') AS subscribe_fb_messenger_ghl,
    COUNTIF(e.event_name = 'subscribe_ig_messenger_ghl') AS subscribe_ig_messenger_ghl,
    COUNTIF(e.event_name = 'subscribe_appt_booked_ghl') AS subscribe_appt_booked_ghl,
    COUNTIF(e.event_name = 'subscribe_form_website_ghl') AS subscribe_form_website_ghl,
    COUNTIF(e.event_name = 'subscribe_chat_website_ghl') AS subscribe_chat_website_ghl,
    COUNTIF(e.event_name = 'subscribe_chat_fbfunnel_ghl') AS subscribe_chat_fbfunnel_ghl,
    COUNTIF(e.event_name = 'subscribe_chat_googlefunnel_ghl') AS subscribe_chat_googlefunnel_ghl,
    COUNTIF(e.event_name = 'appt_cancelled_ghl') AS appt_cancelled_ghl,
    COUNTIF(e.event_name = 'appt_noshow_ghl') AS appt_noshow_ghl,
    COUNTIF(e.event_name = 'appt_show_ghl') AS appt_show_ghl
  FROM `clinicgrower-reporting.ga4_custom_rollup.custom_rollup_events` e
  LEFT JOIN StreamMapping smap
    ON e.property_id = smap.property_id
  WHERE e.event_date_as_date IS NOT NULL
    AND (smap.cg_id != 'CG284' OR e.collected_traffic_source.manual_campaign_name LIKE 'CG284%' OR e.collected_traffic_source.manual_campaign_name IS NULL OR e.collected_traffic_source.manual_campaign_name = '')
  GROUP BY smap.cg_id, smap.client_name, smap.google_monthly_ad_budget, e.property_id, e.event_date_as_date, e.collected_traffic_source.manual_campaign_id, e.collected_traffic_source.manual_campaign_name, e.collected_traffic_source.manual_content, e.collected_traffic_source.manual_source
),
JoinedData AS (
  SELECT
    COALESCE(a.cg_id, e.cg_id) AS cgid,
    COALESCE(a.client_name, e.client_name, 'Unknown') AS client_name,
    COALESCE(a.google_monthly_ad_budget, e.google_monthly_ad_budget, 0) AS google_monthly_ad_budget,
    COALESCE(a.report_date, e.report_date) AS report_date,
    COALESCE(CAST(e.campaign_id AS STRING), CAST(a.campaign_id AS STRING), '(no campaign)') AS campaign_id,
    COALESCE(e.campaign_name, a.campaign_name, '(no campaign)') AS campaign_name,
    COALESCE(e.ad_group_name, a.ad_group_name, '(no ad group)') AS ad_group_name,
    COALESCE(a.ad_group_ad_ad_name, '(no ad)') AS ad_group_ad_ad_name,
    COALESCE(e.manual_source, '(no source)') AS manual_source,
    COALESCE(e.subscribe_survey_meta_ghl, 0) AS subscribe_survey_meta_ghl,
    COALESCE(e.subscribe_form_meta_ghl, 0) AS subscribe_form_meta_ghl,
    COALESCE(e.subscribe_form_google_ghl, 0) AS subscribe_form_google_ghl,
    COALESCE(e.subscribe_survey_google_ghl, 0) AS subscribe_survey_google_ghl,
    COALESCE(e.subscribe_call_meta_ghl, 0) AS subscribe_call_meta_ghl,
    COALESCE(e.subscribe_call_google_ghl, 0) AS subscribe_call_google_ghl,
    COALESCE(e.subscribe_call_citations_ghl, 0) AS subscribe_call_citations_ghl,
    COALESCE(e.subscribe_call_website_ghl, 0) AS subscribe_call_website_ghl,
    COALESCE(e.subscribe_call_press_ghl, 0) AS subscribe_call_press_ghl,
    COALESCE(e.subscribe_call_gbp_ghl, 0) AS subscribe_call_gbp_ghl,
    COALESCE(e.subscribe_call_main_ghl, 0) AS subscribe_call_main_ghl,
    COALESCE(e.subscribe_call_seo_ghl, 0) AS subscribe_call_seo_ghl,
    COALESCE(e.subscribe_fb_messenger_ghl, 0) AS subscribe_fb_messenger_ghl,
    COALESCE(e.subscribe_ig_messenger_ghl, 0) AS subscribe_ig_messenger_ghl,
    COALESCE(e.subscribe_appt_booked_ghl, 0) AS subscribe_appt_booked_ghl,
    COALESCE(e.subscribe_form_website_ghl, 0) AS subscribe_form_website_ghl,
    COALESCE(e.subscribe_chat_website_ghl, 0) AS subscribe_chat_website_ghl,
    COALESCE(e.subscribe_chat_fbfunnel_ghl, 0) AS subscribe_chat_fbfunnel_ghl,
    COALESCE(e.subscribe_chat_googlefunnel_ghl, 0) AS subscribe_chat_googlefunnel_ghl,
    COALESCE(e.appt_cancelled_ghl, 0) AS appt_cancelled_ghl,
    COALESCE(e.appt_noshow_ghl, 0) AS appt_noshow_ghl,
    COALESCE(e.appt_show_ghl, 0) AS appt_show_ghl,
    COALESCE(a.total_spend, 0) AS total_spend,
    COALESCE(a.total_impressions, 0) AS total_impressions,
    COALESCE(a.total_clicks, 0) AS total_clicks,
    COALESCE(a.avg_cpc, 0) AS avg_cpc,
    COALESCE(a.avg_cpm, 0) AS avg_cpm,
    COALESCE(a.avg_ctr, 0) AS avg_ctr
  FROM GoogleAdsAggregated a
  FULL OUTER JOIN EventsAggregated e
    ON a.cg_id = e.cg_id
    AND a.report_date = e.report_date
    AND COALESCE(a.campaign_name, '(no campaign)') = COALESCE(e.campaign_name, '(no campaign)')
    AND COALESCE(a.ad_group_name, '(no ad group)') = COALESCE(e.ad_group_name, '(no ad group)')
),
FinalData AS (
  SELECT
    cgid,
    client_name,
    google_monthly_ad_budget,
    report_date,
    campaign_id,
    campaign_name,
    ad_group_name,
    ad_group_ad_ad_name,
    manual_source,
    SUM(total_spend) AS total_spend,
    SUM(total_impressions) AS total_impressions,
    SUM(total_clicks) AS total_clicks,
    AVG(avg_cpc) AS avg_cpc,
    AVG(avg_cpm) AS avg_cpm,
    AVG(avg_ctr) AS avg_ctr,
    SUM(subscribe_survey_meta_ghl) AS subscribe_survey_meta_ghl,
    SUM(subscribe_form_meta_ghl) AS subscribe_form_meta_ghl,
    SUM(subscribe_form_google_ghl) AS subscribe_form_google_ghl,
    SUM(subscribe_survey_google_ghl) AS subscribe_survey_google_ghl,
    SUM(subscribe_call_meta_ghl) AS subscribe_call_meta_ghl,
    SUM(subscribe_call_google_ghl) AS subscribe_call_google_ghl,
    SUM(subscribe_call_citations_ghl) AS subscribe_call_citations_ghl,
    SUM(subscribe_call_website_ghl) AS subscribe_call_website_ghl,
    SUM(subscribe_call_press_ghl) AS subscribe_call_press_ghl,
    SUM(subscribe_call_gbp_ghl) AS subscribe_call_gbp_ghl,
    SUM(subscribe_call_main_ghl) AS subscribe_call_main_ghl,
    SUM(subscribe_call_seo_ghl) AS subscribe_call_seo_ghl,
    SUM(subscribe_fb_messenger_ghl) AS subscribe_fb_messenger_ghl,
    SUM(subscribe_ig_messenger_ghl) AS subscribe_ig_messenger_ghl,
    SUM(subscribe_appt_booked_ghl) AS subscribe_appt_booked_ghl,
    SUM(subscribe_form_website_ghl) AS subscribe_form_website_ghl,
    SUM(subscribe_chat_website_ghl) AS subscribe_chat_website_ghl,
    SUM(subscribe_chat_fbfunnel_ghl) AS subscribe_chat_fbfunnel_ghl,
    SUM(subscribe_chat_googlefunnel_ghl) AS subscribe_chat_googlefunnel_ghl,
    SUM(appt_cancelled_ghl) AS appt_cancelled_ghl,
    SUM(appt_noshow_ghl) AS appt_noshow_ghl,
    SUM(appt_show_ghl) AS appt_show_ghl
  FROM JoinedData
  GROUP BY 
    cgid,
    client_name,
    google_monthly_ad_budget,
    report_date,
    campaign_id,
    campaign_name,
    ad_group_name,
    ad_group_ad_ad_name,
    manual_source
)
SELECT
  cgid,
  client_name,
  google_monthly_ad_budget,
  DATE_TRUNC(report_date, MONTH) AS report_month,
  report_date,
  campaign_id,
  campaign_name,
  ad_group_name,
  ad_group_ad_ad_name,
  manual_source,
  total_spend,
  total_impressions,
  total_clicks,
  avg_cpc,
  avg_cpm,
  avg_ctr,
  subscribe_survey_meta_ghl,
  subscribe_form_meta_ghl,
  subscribe_form_google_ghl,
  subscribe_survey_google_ghl,
  subscribe_call_meta_ghl,
  subscribe_call_google_ghl,
  subscribe_call_citations_ghl,
  subscribe_call_website_ghl,
  subscribe_call_press_ghl,
  subscribe_call_gbp_ghl,
  subscribe_call_main_ghl,
  subscribe_call_seo_ghl,
  subscribe_fb_messenger_ghl,
  subscribe_ig_messenger_ghl,
  subscribe_appt_booked_ghl,
  subscribe_form_website_ghl,
  subscribe_chat_website_ghl,
  subscribe_chat_fbfunnel_ghl,
  subscribe_chat_googlefunnel_ghl,
  appt_cancelled_ghl,
  appt_noshow_ghl,
  appt_show_ghl
FROM FinalData
ORDER BY report_date DESC, cgid, client_name, campaign_id, campaign_name, ad_group_name, ad_group_ad_ad_name, manual_source;