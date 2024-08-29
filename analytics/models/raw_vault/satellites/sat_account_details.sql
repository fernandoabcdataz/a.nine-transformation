{{ config(
    materialized='incremental',
    unique_key='account_hk',
    tags=['raw_vault', 'satellite']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} AS account_hk,
    name,
    type,
    code,
    _loaded_at AS load_date
FROM {{ ref('stg_xero__accounts') }}