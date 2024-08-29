{{ config(
    materialized='view',
    tags=['staging', 'xero', 'daily']
) }}

SELECT
    payment_id,
    account_id,
    amount,
    date,
    status,
    _loaded_at
FROM {{ source('xero', 'payments') }}