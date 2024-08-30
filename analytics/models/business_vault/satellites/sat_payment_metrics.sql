{{ config(materialized='incremental', unique_key='payment_hkey') }}

WITH current_payment_data AS (
    SELECT 
        p.payment_hkey,
        s.payment_type,
        s.status,
        s.amount,
        s.bank_amount,
        s.currency_rate,
        s.valid_from
    FROM
        {{ ref('hub_payment') }} p
    JOIN 
        {{ ref('sat_payment') }} s 
    ON 
        p.payment_hkey = s.payment_hkey
    WHERE s.valid_to = '9999-12-31'::timestamp
)

SELECT
    payment_hkey,
    payment_type,
    status,
    amount,
    bank_amount,
    currency_rate,
    (bank_amount - amount) as payment_difference,
    CASE 
        WHEN status = 'AUTHORISED' THEN 1
        ELSE 0
    END as is_authorised,
    valid_from,
    '9999-12-31'::timestamp as valid_to
FROM 
    current_payment_data
{% if is_incremental() %}
WHERE 
    valid_from > (SELECT MAX(valid_from) FROM {{ this }})
{% endif %}