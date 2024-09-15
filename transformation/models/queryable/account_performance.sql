/*
purpose:
provides aggregated financial performance per account,
combining xero_invoices, xero_payments, and xero_accounts
*/

{{ config(
    materialized='table',
    tags=['analytics', 'xero', 'account_performance']
) }}

WITH invoice_data AS (
    SELECT
        a.account_id,
        a.name AS account_name,
        COUNT(i.invoice_id) AS total_invoices,
        SUM(i.total) AS total_invoiced_amount,
        SUM(i.amount_due) AS total_amount_due
    FROM 
        {{ ref('xero_accounts') }} AS a
    LEFT JOIN 
        {{ ref('xero_invoices') }} AS i
    ON 
        a.account_id = i.account_id
    GROUP BY 
        a.account_id,
        a.name
),

payment_data AS (
    SELECT
        p.account_id,
        COUNT(p.payment_id) AS total_payments,
        SUM(p.amount) AS total_payment_amount
    FROM 
        {{ ref('xero_payments') }} AS p
    GROUP BY 
        p.account_id
)

SELECT
    id.account_id,
    id.account_name,
    id.total_invoices,
    id.total_invoiced_amount,
    id.total_amount_due,
    pd.total_payments,
    pd.total_payment_amount
FROM
    invoice_data AS id
LEFT JOIN
    payment_data AS pd
ON
    id.account_id = pd.account_id