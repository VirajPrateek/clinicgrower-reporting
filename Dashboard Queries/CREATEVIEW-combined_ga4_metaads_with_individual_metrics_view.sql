CREATE OR REPLACE VIEW `dashboard_views.combined_ga4_metaads_with_individual_metrics_view` AS
WITH StreamMapping AS (
  SELECT
    cg_id AS cg_id,
    MAX(ga4_property_id) AS property_id
  FROM `dashboard_views.monday_board_mapping_materialized`
  WHERE cg_id IS NOT NULL
  GROUP BY cg_id
),
AccountMapping AS (
  SELECT
    REGEXP_EXTRACT(campaign_name, r'^(CG0*\d+)') AS cg_id,
    MAX(account_name) AS account_name
  FROM `clinicgrower-reporting.meta_ads_new.*`
  WHERE _TABLE_SUFFIX LIKE '%_ads_insights'
    AND campaign_name IS NOT NULL
    AND REGEXP_CONTAINS(campaign_name, r'^CG0*\d+')
  GROUP BY cg_id
),
AllDates AS (
  SELECT DISTINCT e.event_date_as_date AS report_date, smap.cg_id AS cg_id
  FROM `clinicgrower-reporting.ga4_custom_rollup.custom_rollup_events` e
  INNER JOIN StreamMapping smap
    ON e.property_id = smap.property_id
  WHERE e.event_date_as_date IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT date_start AS report_date, REGEXP_EXTRACT(campaign_name, r'^(CG0*\d+)') AS cg_id
  FROM `clinicgrower-reporting.meta_ads_new.*`
  WHERE _TABLE_SUFFIX LIKE '%_ads_insights'
    AND REGEXP_CONTAINS(campaign_name, r'^CG0*\d+')
    AND date_start IS NOT NULL
),
GA4LeadData AS (
  SELECT
    e.property_id,
    smap.cg_id,
    DATE_TRUNC(e.event_date_as_date, MONTH) AS report_month,
    e.event_date_as_date AS event_date,
    COUNTIF(e.event_name = 'subscribe_survey_meta_gtm') AS subscribe_survey_meta_gtm,
    COUNTIF(e.event_name = 'subscribe_form_meta_gtm') AS subscribe_form_meta_gtm,
    COUNTIF(e.event_name = 'subscribe_form_google_gtm') AS subscribe_form_google_gtm,
    COUNTIF(e.event_name = 'subscribe_survey_google_gtm') AS subscribe_survey_google_gtm,
    COUNTIF(e.event_name = 'subscribe_call_meta_gtm') AS subscribe_call_meta_gtm,
    COUNTIF(e.event_name = 'subscribe_call_google_gtm') AS subscribe_call_google_gtm,
    COUNTIF(e.event_name = 'subscribe_call_citations_gtm') AS subscribe_call_citations_gtm,
    COUNTIF(e.event_name = 'subscribe_call_website_gtm') AS subscribe_call_website_gtm,
    COUNTIF(e.event_name = 'subscribe_call_press_gtm') AS subscribe_call_press_gtm,
    COUNTIF(e.event_name = 'subscribe_call_gbp_gtm') AS subscribe_call_gbp_gtm,
    COUNTIF(e.event_name = 'subscribe_call_main_gtm') AS subscribe_call_main_gtm,
    COUNTIF(e.event_name = 'subscribe_call_seo_gtm') AS subscribe_call_seo_gtm,
    COUNTIF(e.event_name = 'subscribe_call_youtube_gtm') AS subscribe_call_youtube_gtm,
    COUNTIF(e.event_name = 'subscribe_fb_messenger_gtm') AS subscribe_fb_messenger_gtm,
    COUNTIF(e.event_name = 'subscribe_ig_messenger_gtm') AS subscribe_ig_messenger_gtm,
    COUNTIF(e.event_name = 'subscribe_appt_request_gtm') AS subscribe_appt_request_gtm,
    COUNTIF(e.event_name = 'subscribe_form_website_gtm') AS subscribe_form_website_gtm,
    COUNTIF(e.event_name = 'subscribe_chat_website_gtm') AS subscribe_chat_website_gtm,
    COUNTIF(e.event_name = 'subscribe_chat_fbfunnel_gtm') AS subscribe_chat_fbfunnel_gtm,
    COUNTIF(e.event_name = 'subscribe_chat_googlefunnel_gtm') AS subscribe_chat_googlefunnel_gtm,
    COUNTIF(e.event_name = 'pageview_funnel_gtm') AS pageview_funnel_gtm,
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
    COUNTIF(e.event_name = 'appt_show_ghl') AS appt_show_ghl,
    COUNT(*) AS total_events
  FROM `clinicgrower-reporting.ga4_custom_rollup.custom_rollup_events` e
  INNER JOIN StreamMapping smap
    ON e.property_id = smap.property_id
  WHERE e.event_date_as_date IS NOT NULL
  GROUP BY e.property_id, smap.cg_id, report_month, e.event_date_as_date
),
AdsDailySummary AS (
  SELECT
    REGEXP_EXTRACT(campaign_name, r'^(CG0*\d+)') AS cg_id,
    date_start AS date_spend,
    SUM(CAST(COALESCE(spend, 0) AS FLOAT64)) AS total_spend,
    SUM(CAST(COALESCE(impressions, 0) AS INT64)) AS total_impressions,
    SUM(CAST(COALESCE(clicks, 0) AS INT64)) AS total_clicks,
    AVG(CAST(COALESCE(cpc, 0) AS FLOAT64)) AS avg_cpc,
    AVG(CAST(COALESCE(cpm, 0) AS FLOAT64)) AS avg_cpm,
    AVG(CAST(COALESCE(ctr, 0) AS FLOAT64)) AS avg_ctr,
    SUM(CAST(COALESCE(reach, 0) AS INT64)) AS total_reach,
    SUM(CAST(COALESCE(unique_clicks, 0) AS INT64)) AS total_unique_clicks,
    MAX(account_id) AS account_id,
    MAX(account_name) AS account_name
  FROM `clinicgrower-reporting.meta_ads_new.*`
  WHERE _TABLE_SUFFIX LIKE '%_ads_insights'
    AND REGEXP_CONTAINS(campaign_name, r'^CG0*\d+')
    AND date_start IS NOT NULL
  GROUP BY cg_id, date_spend
),
ClientLevel AS (
  SELECT
    COALESCE(lead.property_id, smap.property_id) AS property_id,
    COALESCE(lead.cg_id, ads.cg_id, smap.cg_id) AS cg_id,
    COALESCE(ads.account_name, amap.account_name) AS account_name,
    lead.report_month,
    lead.event_date AS report_date,
    ads.date_spend,
    lead.subscribe_survey_meta_gtm,
    lead.subscribe_form_meta_gtm,
    lead.subscribe_form_google_gtm,
    lead.subscribe_survey_google_gtm,
    lead.subscribe_call_meta_gtm,
    lead.subscribe_call_google_gtm,
    lead.subscribe_call_citations_gtm,
    lead.subscribe_call_website_gtm,
    lead.subscribe_call_press_gtm,
    lead.subscribe_call_gbp_gtm,
    lead.subscribe_call_main_gtm,
    lead.subscribe_call_seo_gtm,
    lead.subscribe_call_youtube_gtm,
    lead.subscribe_fb_messenger_gtm,
    lead.subscribe_ig_messenger_gtm,
    lead.subscribe_appt_request_gtm,
    lead.subscribe_form_website_gtm,
    lead.subscribe_chat_website_gtm,
    lead.subscribe_chat_fbfunnel_gtm,
    lead.subscribe_chat_googlefunnel_gtm,
    lead.pageview_funnel_gtm,
    lead.subscribe_survey_meta_ghl,
    lead.subscribe_form_meta_ghl,
    lead.subscribe_form_google_ghl,
    lead.subscribe_survey_google_ghl,
    lead.subscribe_call_meta_ghl,
    lead.subscribe_call_google_ghl,
    lead.subscribe_call_citations_ghl,
    lead.subscribe_call_website_ghl,
    lead.subscribe_call_press_ghl,
    lead.subscribe_call_gbp_ghl,
    lead.subscribe_call_main_ghl,
    lead.subscribe_call_seo_ghl,
    lead.subscribe_fb_messenger_ghl,
    lead.subscribe_ig_messenger_ghl,
    lead.subscribe_appt_booked_ghl,
    lead.subscribe_form_website_ghl,
    lead.subscribe_chat_website_ghl,
    lead.subscribe_chat_fbfunnel_ghl,
    lead.subscribe_chat_googlefunnel_ghl,
    lead.appt_cancelled_ghl,
    lead.appt_noshow_ghl,
    lead.appt_show_ghl,
    lead.total_events,
    ads.total_spend,
    ads.total_impressions,
    ads.total_clicks,
    ads.avg_cpc,
    ads.avg_cpm,
    ads.avg_ctr,
    ads.total_reach,
    ads.total_unique_clicks,
    'Client' AS level,
    COUNT(*) OVER (PARTITION BY lead.cg_id) AS debug_row_count
  FROM AllDates dates
  LEFT JOIN GA4LeadData lead
    ON dates.cg_id = lead.cg_id
    AND dates.report_date = lead.event_date
  LEFT JOIN AdsDailySummary ads
    ON dates.cg_id = ads.cg_id
    AND dates.report_date = ads.date_spend
  LEFT JOIN StreamMapping smap
    ON dates.cg_id = smap.cg_id
  LEFT JOIN AccountMapping amap
    ON dates.cg_id = amap.cg_id
),
ClientLevelFiltered AS (
  SELECT
    property_id,
    cg_id AS cgid,
    account_name,
    report_month,
    report_date,
    date_spend,
    subscribe_survey_meta_gtm,
    subscribe_form_meta_gtm,
    subscribe_form_google_gtm,
    subscribe_survey_google_gtm,
    subscribe_call_meta_gtm,
    subscribe_call_google_gtm,
    subscribe_call_citations_gtm,
    subscribe_call_website_gtm,
    subscribe_call_press_gtm,
    subscribe_call_gbp_gtm,
    subscribe_call_main_gtm,
    subscribe_call_seo_gtm,
    subscribe_call_youtube_gtm,
    subscribe_fb_messenger_gtm,
    subscribe_ig_messenger_gtm,
    subscribe_appt_request_gtm,
    subscribe_form_website_gtm,
    subscribe_chat_website_gtm,
    subscribe_chat_fbfunnel_gtm,
    subscribe_chat_googlefunnel_gtm,
    pageview_funnel_gtm,
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
    appt_show_ghl,
    total_events,
    total_spend,
    total_impressions,
    total_clicks,
    avg_cpc,
    avg_cpm,
    avg_ctr,
    total_reach,
    total_unique_clicks,
    level,
    debug_row_count
  FROM ClientLevel
  WHERE report_date IS NOT NULL
)
SELECT
  property_id,
  cgid,
  account_name,
  report_month,
  report_date,
  date_spend,
  IFNULL(subscribe_survey_meta_gtm, 0) AS subscribe_survey_meta_gtm,
  IFNULL(subscribe_form_meta_gtm, 0) AS subscribe_form_meta_gtm,
  IFNULL(subscribe_form_google_gtm, 0) AS subscribe_form_google_gtm,
  IFNULL(subscribe_survey_google_gtm, 0) AS subscribe_survey_google_gtm,
  IFNULL(subscribe_call_meta_gtm, 0) AS subscribe_call_meta_gtm,
  IFNULL(subscribe_call_google_gtm, 0) AS subscribe_call_google_gtm,
  IFNULL(subscribe_call_citations_gtm, 0) AS subscribe_call_citations_gtm,
  IFNULL(subscribe_call_website_gtm, 0) AS subscribe_call_website_gtm,
  IFNULL(subscribe_call_press_gtm, 0) AS subscribe_call_press_gtm,
  IFNULL(subscribe_call_gbp_gtm, 0) AS subscribe_call_gbp_gtm,
  IFNULL(subscribe_call_main_gtm, 0) AS subscribe_call_main_gtm,
  IFNULL(subscribe_call_seo_gtm, 0) AS subscribe_call_seo_gtm,
  IFNULL(subscribe_call_youtube_gtm, 0) AS subscribe_call_youtube_gtm,
  IFNULL(subscribe_fb_messenger_gtm, 0) AS subscribe_fb_messenger_gtm,
  IFNULL(subscribe_ig_messenger_gtm, 0) AS subscribe_ig_messenger_gtm,
  IFNULL(subscribe_appt_request_gtm, 0) AS subscribe_appt_request_gtm,
  IFNULL(subscribe_form_website_gtm, 0) AS subscribe_form_website_gtm,
  IFNULL(subscribe_chat_website_gtm, 0) AS subscribe_chat_website_gtm,
  IFNULL(subscribe_chat_fbfunnel_gtm, 0) AS subscribe_chat_fbfunnel_gtm,
  IFNULL(subscribe_chat_googlefunnel_gtm, 0) AS subscribe_chat_googlefunnel_gtm,
  IFNULL(pageview_funnel_gtm, 0) AS pageview_funnel_gtm,
  IFNULL(subscribe_survey_meta_ghl, 0) AS subscribe_survey_meta_ghl,
  IFNULL(subscribe_form_meta_ghl, 0) AS subscribe_form_meta_ghl,
  IFNULL(subscribe_form_google_ghl, 0) AS subscribe_form_google_ghl,
  IFNULL(subscribe_survey_google_ghl, 0) AS subscribe_survey_google_ghl,
  IFNULL(subscribe_call_meta_ghl, 0) AS subscribe_call_meta_ghl,
  IFNULL(subscribe_call_google_ghl, 0) AS subscribe_call_google_ghl,
  IFNULL(subscribe_call_citations_ghl, 0) AS subscribe_call_citations_ghl,
  IFNULL(subscribe_call_website_ghl, 0) AS subscribe_call_website_ghl,
  IFNULL(subscribe_call_press_ghl, 0) AS subscribe_call_press_ghl,
  IFNULL(subscribe_call_gbp_ghl, 0) AS subscribe_call_gbp_ghl,
  IFNULL(subscribe_call_main_ghl, 0) AS subscribe_call_main_ghl,
  IFNULL(subscribe_call_seo_ghl, 0) AS subscribe_call_seo_ghl,
  IFNULL(subscribe_fb_messenger_ghl, 0) AS subscribe_fb_messenger_ghl,
  IFNULL(subscribe_ig_messenger_ghl, 0) AS subscribe_ig_messenger_ghl,
  IFNULL(subscribe_appt_booked_ghl, 0) AS subscribe_appt_booked_ghl,
  IFNULL(subscribe_form_website_ghl, 0) AS subscribe_form_website_ghl,
  IFNULL(subscribe_chat_website_ghl, 0) AS subscribe_chat_website_ghl,
  IFNULL(subscribe_chat_fbfunnel_ghl, 0) AS subscribe_chat_fbfunnel_ghl,
  IFNULL(subscribe_chat_googlefunnel_ghl, 0) AS subscribe_chat_googlefunnel_ghl,
  IFNULL(appt_cancelled_ghl, 0) AS appt_cancelled_ghl,
  IFNULL(appt_noshow_ghl, 0) AS appt_noshow_ghl,
  IFNULL(appt_show_ghl, 0) AS appt_show_ghl,
  IFNULL(total_events, 0) AS total_events,
  IFNULL(total_spend, 0) AS total_spend,
  IFNULL(total_impressions, 0) AS total_impressions,
  IFNULL(total_clicks, 0) AS total_clicks,
  IFNULL(avg_cpc, 0) AS avg_cpc,
  IFNULL(avg_cpm, 0) AS avg_cpm,
  IFNULL(avg_ctr, 0) AS avg_ctr,
  IFNULL(total_reach, 0) AS total_reach,
  IFNULL(total_unique_clicks, 0) AS total_unique_clicks,
  level,
  debug_row_count
FROM ClientLevelFiltered
ORDER BY report_month DESC, report_date DESC, cgid;