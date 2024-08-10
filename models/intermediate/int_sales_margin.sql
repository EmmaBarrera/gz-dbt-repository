WITH sales_data AS (
    SELECT
        orders_id
        ,product_id
        ,revenue
        ,quantity
        ,date_date
    FROM
        {{ref('stg_raw__sales')}}    
),

product_data AS (
    SELECT
        products_id
        ,purchase_price
    FROM
        {{ref('stg_raw__product')}}
),

joined_sales_with_product_data AS (
    SELECT
        sales_data.orders_id
        ,sales_data.product_id
        ,sales_data.revenue
        ,sales_data.quantity
        ,sales_data.date_date
        ,product_data.purchase_price
        ,sales_data.quantity * CAST(product_data.purchase_price AS FLOAT64) AS purchase_cost -- CALCULATE OF PURCHASE COST
    FROM
        sales_data
    LEFT JOIN product_data
    ON sales_data.product_id = product_data.products_id
)

SELECT
    orders_id
    ,product_id
    ,quantity
    ,revenue
    ,purchase_cost
    ,date_date
    ,ROUND(revenue - purchase_cost ,2) AS margin -- CALCULATE OF MARGIN
FROM
    joined_sales_with_product_data
