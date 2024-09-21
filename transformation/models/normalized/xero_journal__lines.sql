{{ config(
    tags=['normalized', 'xero', 'journalS', 'journal__lines']
) }}

WITH journal_lines_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.JournalID') AS journal_id,
        JSON_VALUE(journal_line., '$.JournalLineID') AS journal_line_id,
        JSON_VALUE(journal_line, '$.AccountID') AS account_id,
        JSON_VALUE(journal_line, '$.AccountCode') AS account_code,
        JSON_VALUE(journal_line, '$.AccountType') AS account_type,
        JSON_VALUE(journal_line, '$.AccountName') AS account_name,
        JSON_VALUE(journal_line, '$.Description') AS description,
        SAFE_CAST(JSON_VALUE(journal_line, '$.NetAmount') AS NUMERIC) AS net_amount,
        SAFE_CAST(JSON_VALUE(journal_line, '$.GrossAmount') AS NUMERIC) AS gross_amount,
        SAFE_CAST(JSON_VALUE(journal_line, '$.TaxAmount') AS NUMERIC) AS tax_amount,
        JSON_VALUE(journal_line, '$.TaxType') AS tax_type,
        JSON_VALUE(journal_line, '$.TaxName') AS tax_name
    FROM 
        {{ source('raw', 'xero_journals') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.JournalLines')) AS journal_line
)

SELECT
    ingestion_time,
    journal_id,
    journal_line_id,
    account_id,
    account_code,
    account_type,
    account_name,
    description,
    net_amount,
    gross_amount,
    tax_amount,
    tax_type,
    tax_name
FROM 
    journal_lines_raw