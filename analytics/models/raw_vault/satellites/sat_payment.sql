{{ config(
    materialized='incremental',
    unique_key='payment_hk',
    tags=['raw_vault', 'satellite']
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['payment_id']) }} AS payment_hk,
  , payment_id
  , account
  , has_account
  , currency_rate
  , is_reconciled
  , payment_type
  , reference
  , date
  , updated_date_utc
  , has_validation_errors
  , bank_amount
  , invoice
  , status
  , amount
  , _loaded_at
  , 'xero' as source_system
FROM {{ ref('stg_xero__payments') }}