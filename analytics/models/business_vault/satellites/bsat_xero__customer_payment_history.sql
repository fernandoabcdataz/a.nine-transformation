{{ config(
    materialized='materialized_view',
    tags=['business_vault', 'xero', 'customer']
) }}

SELECT 
    contact_id,
    contact_name,
    COUNT(DISTINCT payment_id) as total_payments,
    SUM(amount) as total_amount_paid,
    MIN(date) as first_payment_date,
    MAX(date) as last_payment_date,
    AVG(amount) as average_payment_amount
FROM {{ ref('bv_xero__payment_summary') }}
GROUP BY contact_id, contact_name