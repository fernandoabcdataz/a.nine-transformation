{{ config(
    tags=['normalized', 'xero', 'manual_journal_lines']
) }}

WITH journal_lines_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ManualJournalID') AS manual_journal_id,
        CAST(JSON_VALUE(journal_line.value, '$.LineAmount') AS NUMERIC) AS line_amount,
        JSON_VALUE(journal_line.value, '$.AccountCode') AS account_code,
        JSON_VALUE(journal_line.value, '$.Description') AS description,
        JSON_VALUE(journal_line.value, '$.TaxType') AS tax_type,
        -- Tracking
        CAST(JSON_VALUE(journal_line.value, '$.TaxAmount') AS NUMERIC) AS tax_amount
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
    -- tracking
    tax_amount
FROM 
    journal_lines_raw