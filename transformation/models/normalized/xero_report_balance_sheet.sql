{{ config(
    tags=['normalized', 'xero', 'balance_sheet']
) }}

WITH balance_sheet_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(report, '$.ReportID') AS report_id,
        JSON_VALUE(report, '$.ReportName') AS report_name,
        JSON_VALUE(report, '$.ReportType') AS report_type,
        ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(report, '$.ReportTitles'), ', ') AS report_titles,
        JSON_VALUE(report, '$.ReportDate') AS report_date,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(report, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc,
        JSON_VALUE(row, '$.RowType') AS row_type,
        JSON_VALUE(row, '$.Title') AS row_title,
        JSON_VALUE(sub_row, '$.RowType') AS sub_row_type,
        JSON_VALUE(sub_row, '$.Title') AS sub_row_title,
        JSON_VALUE(cell, '$.Value') AS cell_value,
        JSON_VALUE(attribute, '$.Value') AS attribute_value,
        JSON_VALUE(attribute, '$.Id') AS attribute_id
    FROM 
        {{ source('raw', 'xero_balance_sheet_reports') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Reports')) AS report,
        UNNEST(JSON_EXTRACT_ARRAY(report, '$.Rows')) AS row
        LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(row, '$.Rows')) AS sub_row ON TRUE
        LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(row, '$.Cells')) AS cell ON TRUE
        LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(cell, '$.Attributes')) AS attribute ON TRUE
)

SELECT
    ingestion_time,
    report_id,
    report_name,
    report_type,
    report_titles,
    report_date,
    updated_date_utc,
    row_type,
    row_title,
    sub_row_type,
    sub_row_title,
    cell_value,
    attribute_id,
    attribute_value
FROM 
    balance_sheet_raw