{{ config(
    tags=['normalized', 'xero', 'manual_journals']
) }}

WITH manual_journals_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ManualJournalID') AS manual_journal_id,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS journal_date,
        JSON_VALUE(data, '$.LineAmountTypes') AS line_amount_types,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.Narration') AS narration,
        JSON_VALUE(data, '$.Url') AS Url,
        SAFE_CAST(JSON_VALUE(data, '$.ShowOnCashBasisReports') AS BOOL) AS show_on_cash_basis_reports,
        SAFE_CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc
    FROM 
        {{ source('raw', 'xero_manual_journals') }}
)

SELECT
    ingestion_time,
    manual_journal_id,
    journal_date,
    line_amount_types,
    status,
    narration,
    url,
    show_on_cash_basis_reports,
    has_attachments,
    updated_date_utc
FROM 
    manual_journals_raw