{{ config(
    materialized='incremental',
    unique_key='payment_hk',
    tags=['raw_vault', 'satellite']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_id']) }} AS payment_hk,
    amount,
    date,
    status,
    _loaded_at AS load_date
FROM {{ ref('stg_xero__payments') }}