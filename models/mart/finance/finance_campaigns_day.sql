WITH campaigns_data AS (
    SELECT
        date_date
        ,SUM(ads_cost) AS ads_cost
    FROM
        {{ref('int_campaigns_day')}}
    GROUP BY
        date_date
),

finance_data AS (
    SELECT
        date_date
        ,nb_transactions
        ,operational_margin
        ,revenue
        ,average_basket
        ,margin
    FROM
        {{ref('finance_days')}}
)

SELECT
    finance.date_date
    ,campaigns.ads_cost
    ,(finance.operational_margin - campaigns.ads_cost) AS ads_margin
    ,finance.nb_transactions
    ,finance.revenue
    ,finance.average_basket
    ,finance.margin
    ,finance.operational_margin
FROM
    finance_data finance
LEFT JOIN
    campaigns_data campaigns
ON
    finance.date_date = campaigns.date_date
ORDER BY
    finance.date_date DESC