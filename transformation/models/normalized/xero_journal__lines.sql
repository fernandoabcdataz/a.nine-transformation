{{ config(
    tags=['normalized', 'xero', 'journal_lines']
) }}

WITH journal_lines_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.JournalID') AS journal_id,
        JSON_VALUE(journal_line.value, '$.JournalLineID') AS journal_line_id,
        JSON_VALUE(journal_line.value, '$.AccountID') AS account_id,
        JSON_VALUE(journal_line.value, '$.AccountCode') AS account_code,
        JSON_VALUE(journal_line.value, '$.AccountType') AS account_type,
        JSON_VALUE(journal_line.value, '$.AccountName') AS account_name,
        JSON_VALUE(journal_line.value, '$.Description') AS description,
        CAST(JSON_VALUE(journal_line.value, '$.NetAmount') AS NUMERIC) AS net_amount,
        CAST(JSON_VALUE(journal_line.value, '$.GrossAmount') AS NUMERIC) AS gross_amount,
        CAST(JSON_VALUE(journal_line.value, '$.TaxAmount') AS NUMERIC) AS tax_amount,
        JSON_VALUE(journal_line.value, '$.TaxType') AS tax_type,
        JSON_VALUE(journal_line.value, '$.TaxName') AS tax_name
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