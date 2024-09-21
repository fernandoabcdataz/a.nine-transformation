{{ config(
    tags=['normalized', 'xero', 'journals']
) }}

WITH journals_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.JournalID') AS journal_id,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.JournalDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS journal_date,
        SAFE_CAST(JSON_VALUE(data, '$.JournalNumber') AS INT64) AS journal_number,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.CreatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS created_date_utc,
        JSON_VALUE(data, '$.Reference') AS reference,
        JSON_VALUE(data, '$.SourceID') AS source_id,
        JSON_VALUE(data, '$.SourceType') AS source_type
    FROM 
        {{ source('raw', 'xero_journals') }}
)

SELECT
    ingestion_time,
    journal_id,
    journal_date,
    journal_number,
    created_date_utc,
    reference,
    source_id,
    source_type
FROM 
    journals_raw