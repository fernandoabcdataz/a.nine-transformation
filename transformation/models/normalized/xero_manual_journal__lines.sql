{{ config(
    tags=['normalized', 'xero', 'manual_journals', 'manual_journal__lines']
) }}

WITH journal_lines_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.ManualJournalID') AS manual_journal_id,
        SAFE_CAST(JSON_VALUE(journal_line, '$.LineAmount') AS NUMERIC) AS line_amount,
        JSON_VALUE(journal_line, '$.AccountCode') AS account_code,
        JSON_VALUE(journal_line, '$.Description') AS description,
        JSON_VALUE(journal_line, '$.TaxType') AS tax_type,
        JSON_VALUE(journal_line, '$.Tracking') AS tracking, -- temporary
        SAFE_CAST(JSON_VALUE(journal_line, '$.TaxAmount') AS NUMERIC) AS tax_amount
    FROM 
        {{ source('raw', 'xero_manual_journals') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.JournalLines')) AS journal_line
)

SELECT
    ingestion_time,
    manual_journal_id,
    line_amount,
    account_code,
    description,
    tax_type,
    tracking,
    tax_amount
FROM 
    journal_lines_raw