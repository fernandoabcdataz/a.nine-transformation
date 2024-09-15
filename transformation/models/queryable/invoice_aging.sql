/*
purpose:
analyzes overdue invoices by calculating the number of days overdue,
using data from xero_invoices
*/

{{ config(
    materialized='table',
    tags=['analytics', 'xero', 'invoice_aging']
) }}

WITH aging_data AS (
    SELECT
        invoice_id,
        invoice_number,
        invoice_date,
        due_date,
        status,
        total,
        amount_due,
        CURRENT_DATE() - CAST(due_date AS DATE) AS days_overdue
    FROM 
        {{ ref('xero_invoices') }}
    WHERE 
        status = 'AUTHORISED'
        AND amount_due > 0
)

SELECT
    invoice_id,
    invoice_number,
    invoice_date,
    due_date,
    total,
    amount_due,
    days_overdue
FROM
    aging_data
ORDER BY
    days_overdue DESC