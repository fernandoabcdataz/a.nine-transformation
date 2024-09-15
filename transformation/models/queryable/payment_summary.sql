/*
purpose: 
aggregates payment information, 
combining details from xero_payments, xero_invoices, and xero_contacts
to provide insights into payment statuses and amounts
*/

{{ config(
    tags=['analytics', 'xero', 'payment_summary']
) }}

WITH payment_data AS (
    SELECT
        p.payment_id,
        p.invoice_id,
        i.invoice_number,
        p.payment_date,
        p.amount,
        p.currency_rate,
        p.status,
        p.reference,
        p.is_reconciled,
        p.currency_code,
        i.contact_id,
        c.name AS contact_name,
        il.account_id
    FROM 
        {{ ref('xero_payments') }} AS p
    LEFT JOIN 
        {{ ref('xero_invoices') }} AS i
    ON 
        p.invoice_id = i.invoice_id
    LEFT JOIN 
        {{ ref('xero_contacts') }} AS c
    ON 
        i.contact_id = c.contact_id
    LEFT JOIN
        {{ ref('xero_invoice__line_items') }} AS il
    ON
        i.invoice_id = il.invoice_id
)

SELECT
    payment_id,
    invoice_id,
    invoice_number,
    payment_date,
    amount,
    currency_rate,
    status,
    reference,
    is_reconciled,
    currency_code,
    contact_id,
    contact_name,
    account_id
FROM
    payment_data