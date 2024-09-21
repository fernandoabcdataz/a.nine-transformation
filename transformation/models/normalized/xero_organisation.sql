{{ config(
    tags=['normalized', 'xero', 'organisation']
) }}

WITH organisation_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.OrganisationID') AS organisation_id,
        -- JSON_VALUE(data, '$.APIKey') AS api_key,
        JSON_VALUE(data, '$.Name') AS name,
        JSON_VALUE(data, '$.LegalName') AS legal_name,
        SAFE_CAST(JSON_VALUE(data, '$.PaysTax') AS BOOL) AS pays_tax,
        JSON_VALUE(data, '$.Version') AS version,
        JSON_VALUE(data, '$.OrganisationType') AS organisation_type,
        JSON_VALUE(data, '$.BaseCurrency') AS base_currency,
        JSON_VALUE(data, '$.CountryCode') AS country_code,
        SAFE_CAST(JSON_VALUE(data, '$.IsDemoCompany') AS BOOL) AS is_demo_company,
        JSON_VALUE(data, '$.OrganisationStatus') AS organisation_status,
        JSON_VALUE(data, '$.RegistrationNumber') AS registration_number,
        JSON_VALUE(data, '$.TaxNumber') AS tax_number,
        SAFE_CAST(JSON_VALUE(data, '$.FinancialYearEndDay') AS INT64) AS financial_year_end_day,
        SAFE_CAST(JSON_VALUE(data, '$.FinancialYearEndMonth') AS INT64) AS financial_year_end_month,
        JSON_VALUE(data, '$.SalesTaxBasis') AS sales_tax_basis,
        JSON_VALUE(data, '$.SalesTaxPeriod') AS sales_tax_period,
        JSON_VALUE(data, '$.DefaultSalesTax') AS default_sales_tax,
        JSON_VALUE(data, '$.DefaultPurchasesTax') AS default_purchases_tax,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.PeriodLockDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS period_lock_date,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.EndOfYearLockDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS end_of_year_lock_date,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.CreatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS created_date_utc,
        JSON_VALUE(data, '$.Timezone') AS timezone,
        JSON_VALUE(data, '$.OrganisationEntityType') AS organisation_entity_type,
        JSON_VALUE(data, '$.ShortCode') AS short_code,
        JSON_VALUE(data, '$.Edition') AS edition,
        JSON_VALUE(data, '$.Class') AS class,
        JSON_VALUE(data, '$.LineOfBusiness') AS line_of_business,
        SAFE_CAST(JSON_VALUE(data, '$.PaymentTerms.Bills.Day') AS INT64) AS payment_terms_bills_day,
        JSON_VALUE(data, '$.PaymentTerms.Bills.Type') AS payment_terms_bills_type,
        SAFE_CAST(JSON_VALUE(data, '$.PaymentTerms.Sales.Day') AS INT64) AS payment_terms_sales_day,
        JSON_VALUE(data, '$.PaymentTerms.Sales.Type') AS payment_terms_sales_type
    FROM 
        {{ source('raw', 'xero_organisation') }}
)

SELECT
    ingestion_time,
    organisation_id,
    name,
    legal_name,
    pays_tax,
    version,
    organisation_type,
    base_currency,
    country_code,
    is_demo_company,
    organisation_status,
    registration_number,
    tax_number,
    financial_year_end_day,
    financial_year_end_month,
    sales_tax_basis,
    sales_tax_period,
    default_sales_tax,
    default_purchases_tax,
    period_lock_date,
    end_of_year_lock_date,
    created_date_utc,
    timezone,
    organisation_entity_type,
    short_code,
    edition,
    class,
    line_of_business,
    payment_terms_bills_day,
    payment_terms_bills_type,
    payment_terms_sales_day,
    payment_terms_sales_type
FROM 
    organisation_raw