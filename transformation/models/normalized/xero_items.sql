{{ config(
    tags=['normalized', 'xero', 'items']
) }}

WITH items_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ItemID') AS item_id,
        JSON_VALUE(data, '$.Code') AS code,
        JSON_VALUE(data, '$.Name') AS name,
        JSON_VALUE(data, '$.Description') AS description,
        CAST(JSON_VALUE(data, '$.IsSold') AS BOOL) AS is_sold,
        CAST(JSON_VALUE(data, '$.IsPurchased') AS BOOL) AS is_purchased,
        SAFE_CAST(JSON_VALUE(data, '$.PurchaseDetails.UnitPrice') AS FLOAT64) AS purchase_unit_price,
        JSON_VALUE(data, '$.PurchaseDetails.AccountCode') AS purchase_account_code,
        SAFE_CAST(JSON_VALUE(data, '$.SalesDetails.UnitPrice') AS FLOAT64) AS sales_unit_price,
        JSON_VALUE(data, '$.SalesDetails.AccountCode') AS sales_account_code,
        SAFE_CAST(JSON_VALUE(data, '$.TotalCostPool') AS FLOAT64) AS total_cost_pool,
        SAFE_CAST(JSON_VALUE(data, '$.QuantityOnHand') AS FLOAT64) AS quantity_on_hand
    FROM 
        {{ source('raw', 'xero_items') }}
)

SELECT
    ingestion_time,
    item_id,
    code,
    name,
    description,
    is_sold,
    is_purchased,
    purchase_unit_price,
    purchase_account_code,
    sales_unit_price,
    sales_account_code,
    total_cost_pool,
    quantity_on_hand
FROM 
    items_raw