{{ config(
    materialized='table',
    unique_key='account_hk',
    tags=['raw_vault', 'hub']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} AS account_hk,
    account_id AS account_bk,
    _loaded_at AS load_date
FROM {{ ref('stg_xero__accounts') }}