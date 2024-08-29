{{ config(
    materialized='table',
    unique_key='payment_account_hk',
    tags=['raw_vault', 'link']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_id', 'account_id']) }} AS payment_account_hk,
    {{ dbt_utils.generate_surrogate_key(['payment_id']) }} AS payment_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} AS account_hk,
    _loaded_at AS load_date
FROM {{ ref('stg_xero__payments') }}