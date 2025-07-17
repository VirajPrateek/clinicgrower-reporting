CREATE OR REPLACE VIEW `clinicgrower-reporting.dashboard_views.monday_board_mapping` AS
WITH Calendar AS (
  -- Generate a list of months to report on (last 3 months + current month)
  SELECT
    FORMAT_DATE('%Y-%m', month_start) AS report_month
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_TRUNC(DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 3 MONTH), MONTH),
      DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH),
      INTERVAL 1 MONTH
    )
  ) AS month_start
),
RankedItems AS (
  -- Get all records, ranked by extraction time within each month
  SELECT
    id,
    name,
    updated_at,
    _airbyte_extracted_at,
    FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(_airbyte_extracted_at), MONTH)) AS data_month,
    column_values,
    ROW_NUMBER() OVER (PARTITION BY id, FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(_airbyte_extracted_at), MONTH)) ORDER BY _airbyte_extracted_at DESC, updated_at DESC) AS rn
  FROM `clinicgrower-reporting.budget_from_monday_board.items`
),
UnnestedData AS (
  SELECT
    id,
    name,
    updated_at,
    _airbyte_extracted_at,
    data_month,
    JSON_VALUE(column_value, '$.id') AS column_id,
    JSON_VALUE(column_value, '$.text') AS column_text
  FROM RankedItems,
  UNNEST(
    IFNULL(JSON_QUERY_ARRAY(column_values), [])
  ) AS column_value
  WHERE rn = 1
),
LatestData AS (
  SELECT
    id,
    name AS client_name,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'text25') AS cg_id,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'text_mkrp4a0w') AS ga4_property_id,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'text_mkrwm3w0') AS gmb_location_id,
    CAST(
      FLOOR(
        CAST(
          IFNULL(
            NULLIF(
              (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'numbers'),
              ''
            ),
            '0'
          ) AS FLOAT64
        )
      ) AS INT64
    ) AS monthly_ad_budget,
    CAST(
      FLOOR(
        CAST(
          IFNULL(
            NULLIF(
              (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'dup__of_monthly_ad_budget5'),
              ''
            ),
            '0'
          ) AS FLOAT64
        )
      ) AS INT64
    ) AS fb_monthly_ad_budget,
    CAST(
      FLOOR(
        CAST(
          IFNULL(
            NULLIF(
              (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'dup__of_fb_monthly_ad_budget'),
              ''
            ),
            '0'
          ) AS FLOAT64
        )
      ) AS INT64
    ) AS google_monthly_ad_budget,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'text3') AS clinic_owner_name,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'text7') AS clinic_owner_email,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'phone') AS main_client_phone_number,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'text8') AS meta_ad_account_id,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'text42') AS google_ad_id,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'formula') AS facebook_billing_url,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'status') AS client_status,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'status_1') AS main_client_type,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'status_124') AS client_type,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'date3') AS sale_date,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'date') AS welcome_call_date,
    (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'timeline') AS initial_campaign_dates,
    CAST(
      FLOOR(
        CAST(
          IFNULL(
            NULLIF(
              (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'numbers27'),
              ''
            ),
            '0'
          ) AS FLOAT64
        )
      ) AS INT64
    ) AS fb_mtd_spend,
    CAST(
      FLOOR(
        CAST(
          IFNULL(
            NULLIF(
              (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'numbers62'),
              ''
            ),
            '0'
          ) AS FLOAT64
        )
      ) AS INT64
    ) AS google_mtd_spend,
    CAST(
      FLOOR(
        CAST(
          IFNULL(
            NULLIF(
              (SELECT column_text FROM UnnestedData AS sub WHERE sub.id = main.id AND sub.data_month = main.data_month AND sub.column_id = 'numbers2'),
              ''
            ),
            '0'
          ) AS FLOAT64
        )
      ) AS INT64
    ) AS cpl_goal,
    updated_at,
    _airbyte_extracted_at,
    data_month
  FROM UnnestedData AS main
  GROUP BY id, name, updated_at, _airbyte_extracted_at, data_month
),
CarryForward AS (
  -- For each report month, find the most recent budget data prior to or on that month
  SELECT
    cal.report_month,
    ld.id,
    ld.client_name,
    ld.cg_id,
    ld.ga4_property_id,
    ld.gmb_location_id,
    ld.monthly_ad_budget,
    ld.fb_monthly_ad_budget,
    ld.google_monthly_ad_budget,
    ld.clinic_owner_name,
    ld.clinic_owner_email,
    ld.main_client_phone_number,
    ld.meta_ad_account_id,
    ld.google_ad_id,
    ld.facebook_billing_url,
    ld.client_status,
    ld.main_client_type,
    ld.client_type,
    ld.sale_date,
    ld.welcome_call_date,
    ld.initial_campaign_dates,
    ld.fb_mtd_spend,
    ld.google_mtd_spend,
    ld.cpl_goal,
    ld.updated_at,
    ld._airbyte_extracted_at,
    ROW_NUMBER() OVER (PARTITION BY ld.id, cal.report_month ORDER BY ld.data_month DESC) AS rn
  FROM Calendar cal
  LEFT JOIN LatestData ld
    ON ld.data_month <= cal.report_month
  WHERE ld.id IS NOT NULL
)
SELECT
  id,
  client_name,
  cg_id,
  ga4_property_id,
  gmb_location_id,
  monthly_ad_budget,
  fb_monthly_ad_budget,
  google_monthly_ad_budget,
  clinic_owner_name,
  clinic_owner_email,
  main_client_phone_number,
  meta_ad_account_id,
  google_ad_id,
  facebook_billing_url,
  client_status,
  main_client_type,
  client_type,
  sale_date,
  welcome_call_date,
  initial_campaign_dates,
  fb_mtd_spend,
  google_mtd_spend,
  cpl_goal,
  updated_at,
  _airbyte_extracted_at,
  report_month
FROM CarryForward
WHERE rn = 1
ORDER BY report_month DESC, id