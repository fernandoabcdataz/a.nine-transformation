{{ config(
    materialized='view',
    tags=['staging', 'xero', 'daily']
) }}

SELECT
    account_id,
    name,
    type,
    code,
    _loaded_at
FROM {{ source('xero', 'accounts') }}