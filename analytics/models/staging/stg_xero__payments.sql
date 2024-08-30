-- models/staging/stg_xero__payments.sql
{{ config(
    materialized='table',
    tags=['staging', 'xero', 'daily']
) }}

SELECT
    PaymentID as payment_id,
    Date as date,
    Amount as amount,
    Reference as reference,
    CurrencyRate as currency_rate,
    PaymentType as payment_type,
    Status as status,
    UpdatedDateUTC as updated_date_utc,
    IsReconciled as is_reconciled,
    HasAccount as has_account,
    HasValidationErrors as has_validation_errors,
    BankAmount as bank_amount,
    Account.AccountID as account_id,
    Account.Code as account_code,
    Invoice.InvoiceID as invoice_id,
    Invoice.InvoiceNumber as invoice_number,
    Invoice.CurrencyCode as invoice_currency_code,
    Invoice.Type as invoice_type,
    Invoice.HasErrors as invoice_has_errors,
    Invoice.IsDiscounted as invoice_is_discounted,
    Invoice.Contact.ContactID as contact_id,
    Invoice.Contact.Name as contact_name,
    Invoice.Contact.HasValidationErrors as contact_has_validation_errors,
    CURRENT_TIMESTAMP() as _loaded_at
FROM {{ source('raw', 'xero_payments') }}