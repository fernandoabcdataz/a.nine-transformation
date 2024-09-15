{{ config(
    tags=['normalized', 'xero', 'contacts']
) }}

WITH contacts_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Name') AS name,
        JSON_VALUE(data, '$.ContactStatus') AS contact_status,
        JSON_VALUE(data, '$.EmailAddress') AS email_address,
        JSON_VALUE(data, '$.FirstName') AS first_name,
        JSON_VALUE(data, '$.LastName') AS last_name,
        CAST(JSON_VALUE(data, '$.IsSupplier') AS BOOL) AS is_supplier,
        CAST(JSON_VALUE(data, '$.IsCustomer') AS BOOL) AS is_customer,
        JSON_VALUE(data, '$.DefaultCurrency') AS default_currency
    FROM 
        {{ source('raw', 'xero_contacts') }}
)

SELECT
    ingestion_time,
    contact_id,
    name,
    contact_status,
    email_address,
    first_name,
    last_name,
    is_supplier,
    is_customer,
    default_currency
FROM 
    contacts_raw