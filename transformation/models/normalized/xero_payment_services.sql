{{ config(
    tags=['normalized', 'xero', 'payment_services']
) }}

WITH payment_services_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.PaymentServiceID') AS payment_service_id,
        JSON_VALUE(data, '$.PaymentServiceName') AS payment_service_name,
        JSON_VALUE(data, '$.PaymentServiceUrl') AS payment_service_url,
        JSON_VALUE(data, '$.PayNowText') AS pay_now_text,
        JSON_VALUE(data, '$.PaymentServiceType') AS payment_service_type
    FROM 
        {{ source('raw', 'xero_payment_services') }}
)

SELECT
    ingestion_time,
    payment_service_id,
    payment_service_name,
    payment_service_url,
    pay_now_text,
    payment_service_type
FROM 
    payment_services_raw