{{ config(
    tags=['normalized', 'xero', 'linked_transactions']
) }}

WITH linked_transactions_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.LinkedTransactionID') AS linked_transaction_id,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.Type') AS type,
        JSON_VALUE(data, '$.SourceTransactionID') AS source_transaction_id,
        JSON_VALUE(data, '$.SourceLineItemID') AS source_line_item_id,
        JSON_VALUE(data, '$.SourceTransactionTypeCode') AS source_transaction_type_code,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        JSON_VALUE(data, '$.TargetTransactionID') AS target_transaction_id,
        JSON_VALUE(data, '$.TargetLineItemID') AS target_line_item_id,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc
    FROM 
        {{ source('raw', 'xero_linked_transactions') }}
)

SELECT
    ingestion_time,
    linked_transaction_id,
    status,
    type,
    source_transaction_id,
    source_line_item_id,
    source_transaction_type_code,
    contact_id,
    target_transaction_id,
    target_line_item_id,
    updated_date_utc
FROM 
    linked_transactions_raw