{{ config(
    materialized='view',
    tags=['staging', 'xero', 'daily']
) }}

SELECT
    PaymentID as payment_id,
    Account as account_id,
    Amount as amount,
    Date as date,
    Status as status,
    UpdatedDateUTC as _loaded_at
FROM {{ source('raw', 'xero_payments') }}