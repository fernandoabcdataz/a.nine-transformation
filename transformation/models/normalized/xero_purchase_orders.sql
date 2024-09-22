{{ config(
    tags=['normalized', 'xero', 'purchase_orders']
) }}

WITH purchase_orders_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.PurchaseOrderID') AS purchase_order_id,
        JSON_VALUE(data, '$.PurchaseOrderNumber') AS purchase_order_number,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Contact.Name') AS contact_name,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS date,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.DeliveryDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS delivery_date,
        JSON_VALUE(data, '$.LineAmountTypes') AS line_amount_types,
        JSON_VALUE(data, '$.Reference') AS reference,
        JSON_VALUE(data, '$.BrandingThemeID') AS branding_theme_id,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.SentToContact') AS sent_to_contact,
        JSON_VALUE(data, '$.DeliveryAddress') AS delivery_address,
        JSON_VALUE(data, '$.AttentionTo') AS attention_to,
        JSON_VALUE(data, '$.Telephone') AS telephone,
        JSON_VALUE(data, '$.DeliveryInstructions') AS delivery_instructions,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.ExpectedArrivalDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS expected_arrival_date,
        SAFE_CAST(JSON_VALUE(data, '$.CurrencyRate') AS NUMERIC) AS currency_rate,
        SAFE_CAST(JSON_VALUE(data, '$.SubTotal') AS NUMERIC) AS subtotal,
        SAFE_CAST(JSON_VALUE(data, '$.TotalTax') AS NUMERIC) AS total_tax,
        SAFE_CAST(JSON_VALUE(data, '$.Total') AS NUMERIC) AS total,
        SAFE_CAST(JSON_VALUE(data, '$.TotalDiscount') AS NUMERIC) AS total_discount,
        SAFE_CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc
    FROM 
        {{ source('raw', 'xero_purchase_orders') }}
)

SELECT
ingestion_time,
    purchase_order_id,
    purchase_order_number,
    contact_id,
    contact_name,
    date,
    delivery_date,
    line_amount_types,
    reference,
    branding_theme_id,
    currency_code,
    status,
    sent_to_contact,
    delivery_address,
    attention_to,
    telephone,
    delivery_instructions,
    expected_arrival_date,
    currency_rate,
    subtotal,
    total_tax,
    total,
    total_discount,
    has_attachments,
    updated_date_utc
FROM 
    purchase_orders_raw