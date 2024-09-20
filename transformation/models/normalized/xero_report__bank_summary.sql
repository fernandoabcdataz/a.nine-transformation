{{ config(
    tags=['normalized', 'xero', 'bank_summary']
) }}

WITH report_data AS (
    SELECT
        ingestion_time, -- Assuming ingestion_time is a separate column in your source table
        JSON_VALUE(data, '$.ReportID') AS report_id,
        JSON_VALUE(data, '$.ReportName') AS report_name,
        JSON_VALUE(data, '$.ReportType') AS report_type,
        ARRAY_TO_STRING(
            ARRAY(
                SELECT JSON_VALUE(title)
                FROM UNNEST(JSON_EXTRACT_ARRAY(data, '$.ReportTitles')) AS title
            ),
            ', '
        ) AS report_titles,
        JSON_VALUE(data, '$.ReportDate') AS report_date,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(
                    JSON_VALUE(data, '$.UpdatedDateUTC'),
                    r'/Date\((\d+).*?\)/'
                ) AS INT64
            )
        ) AS updated_date_utc,
        JSON_EXTRACT_ARRAY(data, '$.Rows') AS rows_array
    FROM
        {{ source('raw', 'xero_reports__bank_summary') }}
),

-- Extract top-level rows (e.g., Header, Section)
top_level_rows AS (
    SELECT
        rd.ingestion_time,
        rd.report_id,
        rd.report_name,
        rd.report_type,
        rd.report_titles,
        rd.report_date,
        rd.updated_date_utc,
        JSON_VALUE(row, '$.RowType') AS row_type,
        JSON_VALUE(row, '$.Title') AS row_title,
        JSON_EXTRACT_ARRAY(row, '$.Cells') AS cells,
        JSON_EXTRACT_ARRAY(row, '$.Rows') AS nested_rows
    FROM
        report_data rd,
        UNNEST(rd.rows_array) AS row
),

-- Extract nested rows within 'Section' rows (e.g., Row, SummaryRow)
nested_rows AS (
    SELECT
        tr.ingestion_time,
        tr.report_id,
        tr.report_name,
        tr.report_type,
        tr.report_titles,
        tr.report_date,
        tr.updated_date_utc,
        JSON_VALUE(nested_row, '$.RowType') AS row_type,
        NULL AS row_title, -- Nested rows do not have their own Title
        JSON_EXTRACT_ARRAY(nested_row, '$.Cells') AS cells,
        tr.row_title AS parent_section_title
    FROM
        top_level_rows tr
    CROSS JOIN
        UNNEST(tr.nested_rows) AS nested_row
    WHERE
        tr.row_type = 'Section'
),

combined_rows AS (
    -- Select top-level rows that are not 'Section' or 'Section' with empty 'Rows'
    SELECT
        tr.ingestion_time,
        tr.report_id,
        tr.report_name,
        tr.report_type,
        tr.report_titles,
        tr.report_date,
        tr.updated_date_utc,
        tr.row_type,
        tr.row_title,
        tr.cells
    FROM
        top_level_rows tr
    WHERE
        tr.row_type != 'Section'
        OR (tr.row_type = 'Section' AND (tr.nested_rows IS NULL OR ARRAY_LENGTH(tr.nested_rows) = 0))
    
    UNION ALL
    
    -- Select nested rows and inherit the parent Section's title
    SELECT
        nr.ingestion_time,
        nr.report_id,
        nr.report_name,
        nr.report_type,
        nr.report_titles,
        nr.report_date,
        nr.updated_date_utc,
        nr.row_type,
        nr.parent_section_title AS row_title,
        nr.cells
    FROM
        nested_rows nr
),

final_rows AS (
    SELECT
        cr.ingestion_time,
        cr.report_id,
        cr.report_name,
        cr.report_type,
        cr.report_titles,
        cr.report_date,
        cr.updated_date_utc,
        cr.row_type,
        cr.row_title,
        JSON_VALUE(cell, '$.Value') AS cell_value,
        JSON_VALUE(attribute, '$.Id') AS attribute_id,
        JSON_VALUE(attribute, '$.Value') AS attribute_value
    FROM
        combined_rows cr,
        UNNEST(cr.cells) AS cell
        LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(cell, '$.Attributes')) AS attribute
            ON TRUE
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
    cell_value,
    attribute_id,
    attribute_value
FROM
    final_rows