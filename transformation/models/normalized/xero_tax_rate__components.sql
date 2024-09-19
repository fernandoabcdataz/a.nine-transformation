-- models/normalized/xero_tax_rate__components.sql

{{ config(
    tags=['normalized', 'xero', 'tax_rate_components']
) }}

WITH tax_rate_components_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.TaxRateID') AS tax_rate_id,
        JSON_VALUE(component, '$.TrackingComponentID') AS tracking_component_id,
        JSON_VALUE(component, '$.Name') AS name,
        SAFE_CAST(JSON_VALUE(component, '$.Rate') AS NUMERIC) AS rate,
        SAFE_CAST(JSON_VALUE(component, '$.IsCompound') AS BOOL) AS is_compound,
        SAFE_CAST(JSON_VALUE(component, '$.IsNonRecoverable') AS BOOL) AS is_non_recoverable
    FROM 
        {{ source('raw', 'xero_tax_rates') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.TaxComponents')) AS component
)

SELECT
    ingestion_time,
    tax_rate_id,
    tracking_component_id,
    name,
    rate,
    is_compound,
    is_non_recoverable
FROM 
    tax_rate_components_raw