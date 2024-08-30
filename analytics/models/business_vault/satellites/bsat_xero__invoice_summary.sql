{{ config(
    materialized='materialized_view',
    tags=['business_vault', 'xero', 'invoice']
) }}

WITH invoice_payments AS (
    SELECT 
        invoice_id,
        SUM(amount) as total_paid_amount,
        COUNT(*) as payment_count,
        MIN(date) as first_payment_date,
        MAX(date) as last_payment_date
    FROM {{ ref('bv_xero__payment_summary') }}
    GROUP BY invoice_id
)
SELECT 
    p.invoice_id,
    p.invoice_number,
    p.invoice_currency_code,
    p.invoice_type,
    p.contact_id,
    p.contact_name,
    i.total_paid_amount,
    i.payment_count,
    i.first_payment_date,
    i.last_payment_date
FROM {{ ref('bv_xero__payment_summary') }} p
LEFT JOIN invoice_payments i ON p.invoice_id = i.invoice_id
GROUP BY 
    p.invoice_id, p.invoice_number, p.invoice_currency_code, 
    p.invoice_type, p.contact_id, p.contact_name,
    i.total_paid_amount, i.payment_count, i.first_payment_date, i.last_payment_date
