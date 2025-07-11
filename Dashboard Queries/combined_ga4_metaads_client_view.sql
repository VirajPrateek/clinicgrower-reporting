WITH StreamMapping AS (
  SELECT
    cgid,
    MAX(stream_name) AS stream_name,
    MAX(property_id) AS property_id
  FROM `clinicgrower-reporting.dashboard_views.ga4_rollup_aggregated_view`
  WHERE level = 'Client'
    AND cgid IS NOT NULL
  GROUP BY cgid
),
AccountMapping AS (
  SELECT
    cg_id,
    MAX(account_name) AS account_name
  FROM `clinicgrower-reporting.dashboard_views.agency_ads_overview_datewise_view`
  WHERE level = 'Client'
    AND cg_id IS NOT NULL
    AND account_name IS NOT NULL
  GROUP BY cg_id
),
AllDates AS (
  SELECT DISTINCT event_date AS report_date, cgid
  FROM `clinicgrower-reporting.dashboard_views.ga4_rollup_aggregated_view`
  WHERE level = 'Client' AND cgid IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT date_spend AS report_date, cg_id AS cgid
  FROM `clinicgrower-reporting.dashboard_views.agency_ads_overview_datewise_view`
  WHERE level = 'Client' AND cg_id IS NOT NULL
),
GA4LeadData AS (
  SELECT
    property_id,
    stream_name,
    cgid,
    'lead' AS event_name,
    report_month,
    event_date,
    SUM(total_event_count) AS total_leads,
    SUM(total_sessions) AS total_sessions,
    SUM(total_users) AS total_users,
    SUM(total_active_users) AS total_active_users,
    AVG(avg_event_count_per_user) AS avg_event_count_per_user,
    level
  FROM `clinicgrower-reporting.dashboard_views.ga4_rollup_aggregated_view`
  WHERE event_name IN (
    'subscribe_survey_meta_ghl',
    'subscribe_form_meta_ghl',
    'subscribe_form_google_ghl',
    'subscribe_survey_google_ghl'
  )
  AND level = 'Client'
  GROUP BY property_id, stream_name, cgid, report_month, event_date, level
),
GA4ApptData AS (
  SELECT
    property_id,
    stream_name,
    cgid,
    report_month,
    event_date,
    SUM(CASE WHEN event_name = 'subscribe_appt_request_gtm' THEN total_event_count ELSE 0 END) AS appt_request_gtm_count,
    SUM(CASE WHEN event_name = 'subscribe_appt_booked_ghl' THEN total_event_count ELSE 0 END) AS appt_booked_ghl_count,
    level
  FROM `clinicgrower-reporting.dashboard_views.ga4_rollup_aggregated_view`
  WHERE event_name IN ('subscribe_appt_request_gtm', 'subscribe_appt_booked_ghl')
  AND level = 'Client'
  GROUP BY property_id, stream_name, cgid, report_month, event_date, level
),
AdsData AS (
  SELECT
    cg_id,
    MAX(account_id) AS account_id,
    MAX(account_name) AS account_name,
    report_month,
    date_spend,
    SUM(total_spend) AS total_spend,
    SUM(total_impressions) AS total_impressions,
    SUM(total_clicks) AS total_clicks,
    AVG(avg_cpc) AS avg_cpc,
    AVG(avg_cpm) AS avg_cpm,
    AVG(avg_ctr) AS avg_ctr,
    SUM(total_reach) AS total_reach,
    SUM(total_unique_clicks) AS total_unique_clicks,
    level
  FROM `clinicgrower-reporting.dashboard_views.agency_ads_overview_datewise_view`
  WHERE level = 'Client'
  GROUP BY cg_id, report_month, date_spend, level
),
ClientLevel AS (
  SELECT
    COALESCE(lead.property_id, appt.property_id, smap.property_id) AS property_id,
    COALESCE(lead.stream_name, appt.stream_name, smap.stream_name, 'Not fetched') AS stream_name,  -- Ensure stream_name is never NULL
    COALESCE(ads.account_name, amap.account_name) AS account_name,
    dates.cgid,
    lead.event_name,
    COALESCE(lead.report_month, appt.report_month, ads.report_month) AS report_month,
    lead.event_date,
    ads.date_spend,
    dates.report_date,
    lead.total_leads,
    appt.appt_request_gtm_count,
    appt.appt_booked_ghl_count,
    lead.total_sessions,
    lead.total_users,
    lead.total_active_users,
    lead.avg_event_count_per_user,
    ads.total_spend,
    ads.total_impressions,
    ads.total_clicks,
    ads.avg_cpc,
    ads.avg_cpm,
    ads.avg_ctr,
    ads.total_reach,
    ads.total_unique_clicks,
    'Client' AS level
  FROM AllDates dates
  LEFT JOIN GA4LeadData lead
    ON dates.cgid = lead.cgid
    AND dates.report_date = lead.event_date
  LEFT JOIN GA4ApptData appt
    ON dates.cgid = appt.cgid
    AND dates.report_date = appt.event_date
  LEFT JOIN AdsData ads
    ON LOWER(dates.cgid) = LOWER(ads.cg_id)
    AND dates.report_date = ads.date_spend
  LEFT JOIN StreamMapping smap
    ON dates.cgid = smap.cgid
  LEFT JOIN AccountMapping amap
    ON dates.cgid = amap.cg_id
)
SELECT
  property_id,
  stream_name,
  account_name,
  cgid,
  event_name,
  report_month,
  event_date,
  date_spend,
  report_date,
  total_leads,
  appt_request_gtm_count,
  appt_booked_ghl_count,
  total_sessions,
  total_users,
  total_active_users,
  avg_event_count_per_user,
  total_spend,
  total_impressions,
  total_clicks,
  avg_cpc,
  avg_cpm,
  avg_ctr,
  total_reach,
  total_unique_clicks,
  level
FROM ClientLevel
ORDER BY report_month DESC, report_date DESC NULLS LAST, cgid NULLS FIRST