{{ config(
    materialized='incremental',
    unique_key='payment_hkey',
    tags=['raw_vault', 'satellite']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_id']) }} as payment_hkey,
    {{ dbt_utils.generate_surrogate_key([
        'date',
        'amount',
        'reference',
        'currency_rate',
        'payment_type',
        'status',
        'updated_date_utc',
        'is_reconciled',
        'has_account',
        'has_validation_errors',
        'bank_amount',
        'account_id',
        'account_code',
        'invoice_id',
        'invoice_number',
        'invoice_currency_code',
        'invoice_type',
        'invoice_has_errors',
        'invoice_is_discounted',
        'contact_id',
        'contact_name',
        'contact_has_validation_errors'
    ]) }} as hash_diff,
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
    _loaded_at as loaded_at,
    'xero' as record_source
FROM 
    {{ ref('stg_xero__payments') }}
{% if is_incremental() %}
WHERE 
    _loaded_at > (SELECT MAX(valid_from) FROM {{ this }})
{% endif %}