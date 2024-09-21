{{ config(
    tags=['normalized', 'xero', 'budgets', 'budget__trackings']
) }}

WITH budget_trackings_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BudgetID') AS budget_id,
        JSON_VALUE(tracking, '$.Name') AS tracking_name,
        JSON_VALUE(tracking, '$.Option') AS option_name,
        JSON_VALUE(tracking, '$.TrackingCategoryID') AS tracking_category_id,
        JSON_VALUE(tracking, '$.Options') AS tracking_options
    FROM 
        {{ source('raw', 'xero_budgets') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Tracking')) AS tracking
)

SELECT
    ingestion_time,
    budget_id,
    tracking_name,
    option_name,
    tracking_category_id,
    tracking_options
FROM 
    budget_trackings_raw