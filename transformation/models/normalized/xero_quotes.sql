{{ config(
    tags=['normalized', 'xero', 'quotes']
) }}

WITH quotes_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.QuoteID') AS quote_id,
        JSON_VALUE(data, '$.QuoteNumber') AS quote_number,
        JSON_VALUE(data, '$.Reference') AS reference,
        JSON_VALUE(data, '$.Terms') AS terms,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Contact.Name') AS contact_name,
        JSON_VALUE(data, '$.Contact.Name') AS contact_email_address,
        JSON_VALUE(data, '$.Contact.Name') AS contact_first_name,
        JSON_VALUE(data, '$.Contact.Name') AS contact_last_name,
        JSON_VALUE(data, '$.Title') AS title,
        JSON_VALUE(data, '$.Summary') AS summary,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS quote_date,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.ExpiryDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS expiry_date,
        JSON_VALUE(data, '$.Status') AS status,
        SAFE_CAST(JSON_VALUE(data, '$.CurrencyRate') AS NUMERIC) AS currency_rate,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        SAFE_CAST(JSON_VALUE(data, '$.SubTotal') AS NUMERIC) AS subtotal,
        SAFE_CAST(JSON_VALUE(data, '$.TotalTax') AS NUMERIC) AS total_tax,
        SAFE_CAST(JSON_VALUE(data, '$.Total') AS NUMERIC) AS total,
        SAFE_CAST(JSON_VALUE(data, '$.TotalDiscount') AS NUMERIC) AS total_discount,
        JSON_VALUE(data, '$.BrandingThemeID') AS branding_theme_id,
        JSON_VALUE(data, '$.LineAmountTypes') AS line_amount_types,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc
    FROM 
        {{ source('raw', 'xero_quotes') }}
)

SELECT
    ingestion_time,
    quote_id,
    quote_number,
    reference,
    terms,
    contact_id,
    contact_name,
    contact_email_address,
    contact_first_name,
    contact_last_name,
    title,
    summary,
    quote_date,
    expiry_date,
    status,
    currency_rate,
    currency_code,
    subtotal,
    total_tax,
    total,
    total_discount,
    branding_theme_id,
    line_amount_types,
    updated_date_utc
FROM 
    quotes_raw