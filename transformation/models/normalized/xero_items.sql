{{ config(
    tags=['normalized', 'xero', 'items']
) }}

WITH items_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.ItemID') AS item_id,
        JSON_VALUE(data, '$.Code') AS code,
        JSON_VALUE(data, '$.Name') AS name,
        SAFE_CAST(JSON_VALUE(data, '$.IsSold') AS BOOL) AS is_sold,
        SAFE_CAST(JSON_VALUE(data, '$.IsPurchased') AS BOOL) AS is_purchased,
        JSON_VALUE(data, '$.Description') AS description,
        JSON_VALUE(data, '$.PurchaseDescription') AS purchase_description,
        JSON_VALUE(data, '$.PurchaseDetails.AccountCode') AS purchase_details_account_code,
        JSON_VALUE(data, '$.PurchaseDetails.TaxType') AS purchase_details_tax_type,
        SAFE_CAST(JSON_VALUE(data, '$.PurchaseDetails.UnitPrice') AS NUMERIC) AS purchase_details_unit_price,
        JSON_VALUE(data, '$.PurchaseDetails.COGSAccountCode') AS purchase_details_cogs_account_code,
        JSON_VALUE(data, '$.SalesDetails.AccountCode') AS sales_details_account_code,
        JSON_VALUE(data, '$.SalesDetails.TaxType') AS sales_details_tax_type,
        SAFE_CAST(JSON_VALUE(data, '$.SalesDetails.UnitPrice') AS NUMERIC) AS sales_details_unit_price,
        SAFE_CAST(JSON_VALUE(data, '$.IsTrackedAsInventory') AS BOOL) AS is_tracked_as_inventory,
        JSON_VALUE(data, '$.InventoryAssetAccountCode') AS inventory_asset_account_code,
        SAFE_CAST(JSON_VALUE(data, '$.TotalCostPool') AS NUMERIC) AS total_cost_pool,
        SAFE_CAST(JSON_VALUE(data, '$.QuantityOnHand') AS NUMERIC) AS quantity_on_hand,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc
    FROM 
        {{ source('raw', 'xero_items') }}
)

SELECT
    ingestion_time,
    item_id,
    code,
    name,
    is_sold,
    is_purchased,
    description,
    purchase_description,
    purchase_details_account_code,
    purchase_details_tax_type,
    purchase_details_unit_price,
    purchase_details_cogs_account_code,
    sales_details_account_code,
    sales_details_tax_type,
    sales_details_unit_price,
    is_tracked_as_inventory,
    inventory_asset_account_code,
    total_cost_pool,
    quantity_on_hand,
    updated_date_utc
FROM 
    items_raw