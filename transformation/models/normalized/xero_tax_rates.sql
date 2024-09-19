-- models/normalized/xero_tax_rates.sql

{{ config(
    tags=['normalized', 'xero', 'tax_rates']
) }}

WITH tax_rates_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.TaxRateID') AS tax_rate_id,
        JSON_VALUE(data, '$.Name') AS name,
        JSON_VALUE(data, '$.TaxType') AS tax_type,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.ReportTaxType') AS report_tax_type,
        SAFE_CAST(JSON_VALUE(data, '$.CanApplyToAssets') AS BOOL) AS can_apply_to_assets,
        SAFE_CAST(JSON_VALUE(data, '$.CanApplyToEquity') AS BOOL) AS can_apply_to_equity,
        SAFE_CAST(JSON_VALUE(data, '$.CanApplyToExpenses') AS BOOL) AS can_apply_to_expenses,
        SAFE_CAST(JSON_VALUE(data, '$.CanApplyToLiabilities') AS BOOL) AS can_apply_to_liabilities,
        SAFE_CAST(JSON_VALUE(data, '$.CanApplyToRevenue') AS BOOL) AS can_apply_to_revenue,
        SAFE_CAST(JSON_VALUE(data, '$.DisplayTaxRate') AS NUMERIC) AS display_tax_rate,
        SAFE_CAST(JSON_VALUE(data, '$.EffectiveRate') AS NUMERIC) AS effective_rate
    FROM 
        {{ source('raw', 'xero_tax_rates') }}
)

SELECT
    ingestion_time,
    tax_rate_id,
    name,
    tax_type,
    status,
    report_tax_type,
    can_apply_to_assets,
    can_apply_to_equity,
    can_apply_to_expenses,
    can_apply_to_liabilities,
    can_apply_to_revenue,
    display_tax_rate,
    effective_rate
FROM 
    tax_rates_raw