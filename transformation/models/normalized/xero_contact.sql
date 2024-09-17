{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'contacts', 'extension']
) }}

WITH contacts_extension_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        JSON_VALUE(data, '$.XeroNetworkKey') AS xero_network_key,
        JSON_VALUE(data, '$.MergedToContactID') AS merged_to_contact_id,
        JSON_VALUE(data, '$.SalesDefaultAccountCode') AS sales_default_account_code,
        JSON_VALUE(data, '$.PurchasesDefaultAccountCode') AS purchases_default_account_code,
        JSON_VALUE(data, '$.SalesDefaultLineAmountType') AS sales_default_line_amount_type,
        JSON_VALUE(data, '$.PurchasesDefaultLineAmountType') AS purchases_default_line_amount_type,
        JSON_VALUE(data, '$.TrackingCategoryName') AS tracking_category_name,
        JSON_VALUE(data, '$.TrackingOptionName') AS tracking_option_name,
        JSON_VALUE(data, '$.PaymentTerms') AS payment_terms,
        JSON_VALUE(data, '$.Website') AS website,
        JSON_VALUE(data, '$.BrandingTheme') AS branding_theme,
        CAST(JSON_VALUE(data, '$.Discount') AS FLOAT64) AS discount,
        JSON_QUERY(data, '$.SalesTrackingCategories') AS sales_tracking_categories,
        JSON_QUERY(data, '$.PurchasesTrackingCategories') AS purchases_tracking_categories,
        JSON_QUERY(data, '$.ContactGroups') AS contact_groups,
        JSON_QUERY(data, '$.BatchPayments') AS batch_payments,
        JSON_QUERY(data, '$.Balances') AS balances,
        JSON_QUERY(data, '$.Addresses') AS addresses,
        JSON_QUERY(data, '$.Phones') AS phones
    FROM 
        {{ source('raw', 'xero_contacts') }}
    WHERE 
        JSON_VALUE(data, '$.ContactID') IS NOT NULL
)

SELECT
    ingestion_time,
    contact_id,
    xero_network_key,
    merged_to_contact_id,
    sales_default_account_code,
    purchases_default_account_code,
    sales_default_line_amount_type,
    purchases_default_line_amount_type,
    tracking_category_name,
    tracking_option_name,
    payment_terms,
    website,
    branding_theme,
    discount,
    sales_tracking_categories,
    purchases_tracking_categories,
    contact_groups,
    batch_payments,
    balances,
    addresses,
    phones
FROM 
    contacts_extension_raw