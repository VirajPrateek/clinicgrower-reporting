CREATE OR REPLACE VIEW `dashboard_views.gmb_metrics_with_cgid` AS
SELECT
  m.cg_id AS cgid,
  m.client_name,
  g.location_id,
  g.location_title,
  g.store_code,
  g.is_verified,
  g.date,
  g.BUSINESS_IMPRESSIONS_DESKTOP_MAPS,
  g.BUSINESS_IMPRESSIONS_DESKTOP_SEARCH,
  g.BUSINESS_IMPRESSIONS_MOBILE_MAPS,
  g.BUSINESS_IMPRESSIONS_MOBILE_SEARCH,
  g.BUSINESS_CONVERSATIONS,
  g.BUSINESS_DIRECTION_REQUESTS,
  g.CALL_CLICKS,
  g.WEBSITE_CLICKS,
  g.BUSINESS_BOOKINGS,
  g.BUSINESS_FOOD_ORDERS,
  g.BUSINESS_FOOD_MENU_CLICKS,
  g.load_timestamp,
  m.report_month
FROM `gmb_data.daily_metrics` g
LEFT JOIN `dashboard_views.monday_board_mapping_materialized` m
  ON g.location_id = m.gmb_location_id
ORDER BY m.cg_id, g.date DESC;