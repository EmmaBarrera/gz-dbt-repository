WITH daily_data AS (
    SELECT
        date_date
        ,COUNT(orders_id) AS nb_transactions
        ,SUM(revenue) AS revenue
        ,AVG(quantity) AS average_basket
)