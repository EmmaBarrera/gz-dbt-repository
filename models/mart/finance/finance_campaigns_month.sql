WITH campaigns_data AS (
    SELECT
        DATE_TRUNC(date_date, MONTH) AS months
        ,SUM(ads_cost) AS ads_cost
    FROM
        {{ref('int_campaigns_day')}}
    GROUP BY
        months
),
finance_data AS (
    SELECT
        DATE_TRUNC(date_date, MONTH) AS months
        ,SUM(nb_transactions) AS nb_transactions
        ,SUM(operational_margin) AS operational_margin
        ,SUM(revenue) AS revenue
        ,AVG(average_basket) AS average_basket
        ,SUM(margin) AS margin
    FROM
        {{ref('finance_days')}}
    GROUP BY
        months
)
SELECT
    finance.months
    ,SUM(campaigns.ads_cost) AS ads_cost
    ,ROUND(SUM(finance.operational_margin) - SUM(campaigns.ads_cost),2) AS ads_margin
    ,SUM(finance.nb_transactions) AS nb_transactions
    ,ROUND(SUM(finance.revenue),2) AS revenue
    ,ROUND(AVG(finance.average_basket),2) AS average_basket
    ,ROUND(SUM(finance.margin),2) AS margin
    ,ROUND(SUM(finance.operational_margin),2) AS operational_margin
FROM
    finance_data finance
LEFT JOIN
    campaigns_data campaigns
ON
    finance.months = campaigns.months
GROUP BY
    finance.months
ORDER BY
    finance.months DESC