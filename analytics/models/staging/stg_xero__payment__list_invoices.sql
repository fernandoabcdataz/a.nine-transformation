{{ config(
    materialized='table',
    tags=['staging', 'xero', 'daily']
) }}

SELECT
    PaymentID as payment_id,
    'ContactPersons' as list_type,
    CAST(cp AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.Contact.ContactPersons) as cp
WHERE 
    Invoice.Contact.ContactPersons IS NOT NULL

UNION ALL

SELECT
    PaymentID as payment_id,
    'ContactPhones' as list_type,
    CAST(ph AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.Contact.Phones) as ph
WHERE 
    Invoice.Contact.Phones IS NOT NULL

UNION ALL

SELECT
    PaymentID as payment_id,
    'ContactAddresses' as list_type,
    CAST(addr AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.Contact.Addresses) as addr
WHERE
    Invoice.Contact.Addresses IS NOT NULL

UNION ALL

SELECT
    PaymentID as payment_id,
    'ContactGroups' as list_type,
    CAST(cg AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.Contact.ContactGroups) as cg
WHERE 
    Invoice.Contact.ContactGroups IS NOT NULL

UNION ALL

SELECT
    PaymentID as payment_id,
    'InvoicePaymentServices' as list_type,
    CAST(ips AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.InvoicePaymentServices) as ips
WHERE 
    Invoice.InvoicePaymentServices IS NOT NULL

UNION ALL

SELECT
    PaymentID as payment_id,
    'InvoicePayments' as list_type,
    CAST(ip AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.Payments) as ip
WHERE 
    Invoice.Payments IS NOT NULL

UNION ALL

SELECT
    PaymentID as payment_id,
    'InvoiceAddresses' as list_type,
    CAST(ia AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.InvoiceAddresses) as ia
WHERE 
    Invoice.InvoiceAddresses IS NOT NULL

UNION ALL

SELECT
    PaymentID as payment_id,
    'LineItems' as list_type,
    CAST(li AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.LineItems) as li
WHERE 
    Invoice.LineItems IS NOT NULL

UNION ALL

SELECT
    PaymentID as payment_id,
    'Prepayments' as list_type,
    CAST(pp AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.Prepayments) as pp
WHERE 
    Invoice.Prepayments IS NOT NULL

UNION ALL

SELECT
    PaymentID as payment_id,
    'Overpayments' as list_type,
    CAST(op AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.Overpayments) as op
WHERE 
    Invoice.Overpayments IS NOT NULL

UNION ALL

SELECT
    PaymentID as payment_id,
    'CreditNotes' as list_type,
    CAST(cn AS STRING) as list_item,
    CURRENT_TIMESTAMP() as _loaded_at
FROM 
    {{ source('raw', 'xero_payments') }},
UNNEST(Invoice.CreditNotes) as cn
WHERE 
    Invoice.CreditNotes IS NOT NULL