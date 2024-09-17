{{ config(
    tags=['normalized', 'xero', 'contacts']
) }}

WITH contacts_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        JSON_VALUE(data, '$.ContactNumber') AS contact_number,
        JSON_VALUE(data, '$.AccountNumber') AS account_number,
        JSON_VALUE(data, '$.ContactStatus') AS contact_status,
        JSON_VALUE(data, '$.Name') AS name,
        JSON_VALUE(data, '$.FirstName') AS first_name,
        JSON_VALUE(data, '$.LastName') AS last_name,
        JSON_VALUE(data, '$.EmailAddress') AS email_address,
        JSON_VALUE(data, '$.BankAccountDetails') AS bank_account_details,
        JSON_VALUE(data, '$.CompanyNumber') AS company_number,
        JSON_VALUE(data, '$.TaxNumber') AS tax_number,
        JSON_VALUE(data, '$.AccountsReceivableTaxType') AS accounts_receivable_tax_type,
        JSON_VALUE(data, '$.AccountsPayableTaxType') AS accounts_payable_tax_type,
        CAST(JSON_VALUE(data, '$.IsSupplier') AS BOOL) AS is_supplier,
        CAST(JSON_VALUE(data, '$.IsCustomer') AS BOOL) AS is_customer,
        JSON_VALUE(data, '$.DefaultCurrency') AS default_currency,
        TIMESTAMP_MILLIS(
            CAST(
                SAFE.REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc
        -- -- optional fields (only present in single get or specific queries)
        -- JSON_VALUE(data, '$.XeroNetworkKey') AS xero_network_key,
        -- JSON_VALUE(data, '$.MergedToContactID') AS merged_to_contact_id,
        -- JSON_VALUE(data, '$.SalesDefaultAccountCode') AS sales_default_account_code,
        -- JSON_VALUE(data, '$.PurchasesDefaultAccountCode') AS purchases_default_account_code,
        -- JSON_VALUE(data, '$.SalesDefaultLineAmountType') AS sales_default_line_amount_type,
        -- JSON_VALUE(data, '$.PurchasesDefaultLineAmountType') AS purchases_default_line_amount_type,
        -- JSON_VALUE(data, '$.TrackingCategoryName') AS tracking_category_name,
        -- JSON_VALUE(data, '$.TrackingOptionName') AS tracking_option_name,
        -- JSON_VALUE(data, '$.PaymentTerms') AS payment_terms,
        -- JSON_VALUE(data, '$.Website') AS website,
        -- JSON_VALUE(data, '$.BrandingTheme') AS branding_theme,
        -- JSON_VALUE(data, '$.Discount') AS discount,
        -- CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments
    FROM 
        {{ source('raw', 'xero_contacts') }}
)

SELECT
    ingestion_time,
    contact_id,
    contact_number,
    account_number,
    contact_status,
    name,
    first_name,
    last_name,
    email_address,
    bank_account_details,
    company_number,
    tax_number,
    accounts_receivable_tax_type,
    accounts_payable_tax_type,
    is_supplier,
    is_customer,
    default_currency,
    updated_date_utc
    -- xero_network_key,
    -- merged_to_contact_id,
    -- sales_default_account_code,
    -- purchases_default_account_code,
    -- sales_default_line_amount_type,
    -- purchases_default_line_amount_type,
    -- tracking_category_name,
    -- tracking_option_name,
    -- payment_terms,
    -- website,
    -- branding_theme,
    -- discount,
    -- has_attachments
FROM 
    contacts_raw