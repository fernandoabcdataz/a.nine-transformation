/*
purpose:
provides aggregated financial performance per account,
combining xero_invoices, xero_payments, and xero_accounts
*/

{{ config(
    tags=['queryable', 'xero', 'account_performance']
) }}

WITH invoice_data AS (
    SELECT
        ili.account_id,
        COUNT(i.invoice_id) AS total_invoices,
        SUM(i.total) AS total_invoiced_amount,
        SUM(i.amount_due) AS total_amount_due
    FROM 
        {{ ref('xero_invoices') }} AS i
    LEFT JOIN 
        {{ ref('xero_invoice__line_items') }} AS ili
    ON 
        i.invoice_id = ili.invoice_id
    GROUP BY 
        ili.account_id
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
    a.account_id,
    a.name AS account_name,
    id.total_invoices,
    id.total_invoiced_amount,
    id.total_amount_due,
    pd.total_payments,
    pd.total_payment_amount
FROM 
    {{ ref('xero_accounts') }} AS a
LEFT JOIN
    invoice_data AS id
ON
    a.account_id = id.account_id
LEFT JOIN
    payment_data AS pd
ON
    a.account_id = pd.account_id