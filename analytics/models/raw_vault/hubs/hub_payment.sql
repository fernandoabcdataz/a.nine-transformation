{{ config(
    materialized='table',
    unique_key='payment_hk',
    tags=['raw_vault', 'hub']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_id']) }} AS payment_hk,
    payment_id AS payment_bk,
    _loaded_at AS load_date
FROM {{ ref('stg_xero__payments') }}