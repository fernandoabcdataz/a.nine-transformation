{{ config(
    materialized='table',
    tags=['business_vault', 'payment']
) }}

SELECT
    h.payment_hk,
    h.payment_bk,
    s.amount,
    s.date,
    s.status,
    a.account_bk
FROM {{ ref('hub_payment') }} h
JOIN {{ ref('sat_payment_details') }} s ON h.payment_hk = s.payment_hk
JOIN {{ ref('link_payment_account') }} l ON h.payment_hk = l.payment_hk
JOIN {{ ref('hub_account') }} a ON l.account_hk = a.account_hk