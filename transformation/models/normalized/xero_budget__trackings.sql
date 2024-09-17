{{ config(
    tags=['normalized', 'xero', 'budget_trackings']
) }}

WITH budget_trackings_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BudgetID') AS budget_id,
        tracking.value.Name AS tracking_name,
        tracking.value.Option AS option_name,
        tracking.value.TrackingCategoryID AS tracking_category_id
    FROM 
        {{ source('raw', 'xero_budgets') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Tracking')) AS tracking
)

SELECT
    ingestion_time,
    budget_id,
    tracking_name,
    option_name,
    tracking_category_id
FROM 
    budget_trackings_raw