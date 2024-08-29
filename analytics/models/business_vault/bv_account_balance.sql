{{ config(
    materialized='table',
    tags=['business_vault', 'account_balance']
) }}

SELECT
    a.account_hk,
    a.account_bk,
    ad.name AS account_name,
    ad.type AS account_type,
    SUM(p.amount) AS total_balance
FROM {{ ref('hub_account') }} a
JOIN {{ ref('sat_account_details') }} ad ON a.account_hk = ad.account_hk
JOIN {{ ref('link_payment_account') }} l ON a.account_hk = l.account_hk
JOIN {{ ref('sat_payment_details') }} p ON l.payment_hk = p.payment_hk
GROUP BY 1, 2, 3, 4