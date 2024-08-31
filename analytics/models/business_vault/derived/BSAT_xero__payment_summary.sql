{{ config(
    materialized='table',
    tags=['business_vault', 'xero', 'payment']
) }}

WITH latest_payment_info AS (
    SELECT 
        payment_hkey,
        payment_id,
        date,
        amount,
        currency_rate,
        payment_type,
        status,
        bank_amount,
        account_id,
        invoice_id,
        ROW_NUMBER() OVER (PARTITION BY payment_hkey ORDER BY loaded_at DESC) as row_num
    FROM 
        {{ ref('sat_xero__payment') }}
)
SELECT 
    p.payment_hkey,
    p.payment_id,
    p.date,
    p.amount,
    p.currency_rate,
    p.payment_type,
    p.status,
    p.bank_amount,
    p.account_id,
    p.invoice_id,
    i.invoice_number,
    i.invoice_currency_code,
    i.invoice_type,
    i.contact_id,
    i.contact_name
FROM 
    latest_payment_info p
LEFT JOIN 
    {{ ref('sat_xero__payment') }} i ON p.payment_hkey = i.payment_hkey
WHERE 
    p.row_num = 1