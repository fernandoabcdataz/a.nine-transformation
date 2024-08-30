{{ config(
    materialized='incremental',
    unique_key='payment_hk',
    tags=['raw_vault', 'satellite']
) }}

SELECT
    {{ dbt_utils.surrogate_key(['payment_id']) }} as payment_hkey,
    date,
    amount,
    reference,
    currency_rate,
    payment_type,
    status,
    updated_date_utc,
    is_reconciled,
    has_account,
    has_validation_errors,
    bank_amount,
    account_id,
    account_code,
    invoice_id,
    invoice_number,
    invoice_currency_code,
    invoice_type,
    invoice_has_errors,
    invoice_is_discounted,
    contact_id,
    contact_name,
    contact_has_validation_errors,
    _loaded_at as valid_from,
    '9999-12-31'::timestamp as valid_to
FROM 
    {{ ref('stg_xero__payments') }}
{% if is_incremental() %}
WHERE 
    _loaded_at > (SELECT MAX(valid_from) FROM {{ this }})
{% endif %}