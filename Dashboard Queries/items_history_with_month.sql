WITH RankedItems AS (
    SELECT
        id,
        name,
        updated_at,
        _airbyte_extracted_at,
        FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(_airbyte_extracted_at), MONTH)) AS report_month,
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
        report_month,
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
        (SELECT column_text
         FROM UnnestedData AS sub
         WHERE sub.id = main.id
         AND sub.report_month = main.report_month
         AND sub.column_id = 'text25') AS cg_id,
        CAST(
          FLOOR(
            CAST(
              IFNULL(
                NULLIF(
                  (SELECT column_text
                   FROM UnnestedData AS sub
                   WHERE sub.id = main.id
                   AND sub.report_month = main.report_month
                   AND sub.column_id = 'numbers'),
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
                  (SELECT column_text
                   FROM UnnestedData AS sub
                   WHERE sub.id = main.id
                   AND sub.report_month = main.report_month
                   AND sub.column_id = 'dup__of_monthly_ad_budget5'),
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
                  (SELECT column_text
                   FROM UnnestedData AS sub
                   WHERE sub.id = main.id
                   AND sub.report_month = main.report_month
                   AND sub.column_id = 'dup__of_fb_monthly_ad_budget'),
                  ''
                ),
                '0'
              ) AS FLOAT64
            )
          ) AS INT64
        ) AS google_monthly_ad_budget,
        updated_at,
        _airbyte_extracted_at,
        report_month
    FROM UnnestedData AS main
    GROUP BY id, name, updated_at, _airbyte_extracted_at, report_month
)
SELECT
    id,
    client_name,
    cg_id,
    monthly_ad_budget,
    fb_monthly_ad_budget,
    google_monthly_ad_budget,
    updated_at,
    _airbyte_extracted_at,
    report_month
FROM LatestData