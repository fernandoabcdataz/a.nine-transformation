/*
purpose: 
summarizes contact-related data, linking the xero_contacts, xero_invoices, and xero_payments tables
to provide a complete view of financial interactions with a specific contact
*/

{{ config(
    tags=['analytics', 'xero', 'contact_summary']
) }}

WITH contact_data AS (
    SELECT
        c.contact_id,
        c.name AS contact_name,
        COUNT(i.invoice_id) AS total_invoices,
        SUM(i.total) AS total_invoiced_amount,
        SUM(i.amount_due) AS total_amount_due
    FROM 
        {{ ref('xero_contacts') }} AS c
    LEFT JOIN 
        {{ ref('xero_invoices') }} AS i
    ON 
        c.contact_id = i.contact_id
    GROUP BY 
        c.contact_id,
        c.name
),

payment_data AS (
    SELECT
        i.contact_id,
        COUNT(p.payment_id) AS total_payments,
        SUM(p.amount) AS total_payment_amount
    FROM 
        {{ ref('xero_payments') }} AS p
    LEFT JOIN 
        {{ ref('xero_invoices') }} AS i
    ON 
        p.invoice_id = i.invoice_id
    GROUP BY 
        i.contact_id
)

SELECT
    cd.contact_id,
    cd.contact_name,
    cd.total_invoices,
    cd.total_invoiced_amount,
    cd.total_amount_due,
    pd.total_payments,
    pd.total_payment_amount
FROM
    contact_data AS cd
LEFT JOIN
    payment_data AS pd
ON
    cd.contact_id = pd.contact_id