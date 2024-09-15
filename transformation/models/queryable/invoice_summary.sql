/*
purpose: 
provides a summary of sales invoices,
pulling data from the xero_invoices, xero_contacts, and xero_invoice_line_items normalized tables
*/

{{ config(
    tags=['queryable', 'xero', 'invoice_summary']
) }}

WITH invoice_data AS (
    SELECT
        i.invoice_id,
        i.invoice_number,
        i.invoice_date,
        i.due_date,
        i.status,
        i.contact_id,
        c.name AS contact_name,
        i.total,
        i.sub_total,
        i.total_tax,
        i.amount_due,
        i.amount_paid,
        i.currency_code
    FROM 
        {{ ref('xero_invoices') }} AS i
    LEFT JOIN 
        {{ ref('xero_contacts') }} AS c
    ON 
        i.contact_id = c.contact_id
),

invoice_line_items AS (
    SELECT
        ili.invoice_id,
        COUNT(ili.line_item_id) AS total_items,
        SUM(ili.unit_amount) AS total_unit_amount,
        SUM(ili.tax_amount) AS total_tax_amount
    FROM
        {{ ref('xero_invoice__line_items') }} AS ili
    GROUP BY
        ili.invoice_id
)

SELECT
    id.invoice_id,
    id.invoice_number,
    id.invoice_date,
    id.due_date,
    id.status,
    id.contact_id,
    id.contact_name,
    id.total,
    id.sub_total,
    id.total_tax,
    id.amount_due,
    id.amount_paid,
    id.currency_code,
    il.total_items,
    il.total_unit_amount,
    il.total_tax_amount
FROM
    invoice_data AS id
LEFT JOIN
    invoice_line_items AS il
ON
    id.invoice_id = il.invoice_id