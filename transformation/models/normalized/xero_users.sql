{{ config(
    tags=['normalized', 'xero', 'users']
) }}

WITH users_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.UserID') AS user_id,
        JSON_VALUE(data, '$.EmailAddress') AS email_address,
        JSON_VALUE(data, '$.FirstName') AS first_name,
        JSON_VALUE(data, '$.LastName') AS last_name,
        SAFE_CAST(JSON_VALUE(data, '$.IsSubscriber') AS BOOL) AS is_subscriber,
        JSON_VALUE(data, '$.OrganisationRole') AS organisation_role,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc
    FROM 
        {{ source('raw', 'xero_users') }}
)

SELECT
    ingestion_time,
    user_id,
    email_address,
    first_name,
    is_subscriber,
    last_name,
    organisation_role,
    updated_date_utc
FROM 
    users_raw