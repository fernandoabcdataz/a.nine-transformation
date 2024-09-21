{{ config(
    tags=['normalized', 'xero', 'branding_themes']
) }}

WITH branding_themes_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.BrandingThemeID') AS branding_theme_id,
        JSON_VALUE(data, '$.Name') AS name,
        JSON_VALUE(data, '$.LogoUrl') AS logo_url,
        JSON_VALUE(data, '$.Type') AS type,
        SAFE_CAST(JSON_VALUE(data, '$.SortOrder') AS INT64) AS sort_order,
        TIMESTAMP_MILLIS(
            CAST(
                SAFE.REGEXP_EXTRACT(JSON_VALUE(data, '$.CreatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS created_date_utc
    FROM 
        {{ source('raw', 'xero_branding_themes') }}
)

SELECT
    ingestion_time,
    branding_theme_id,
    name,
    logo_url,
    type,
    sort_order,
    created_date_utc
FROM 
    branding_themes_raw