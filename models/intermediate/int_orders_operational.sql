WITH orders_margin AS (
    SELECT
        orders_id
        ,margin
        ,revenue
        ,quantity
        ,purchase_cost
    FROM
        {{ref("int_orders_margin")}}
),

shipping_data AS (
    SELECT
        orders_id
        ,shipping_fee
        ,logcost
        ,CAST(ship_cost AS FLOAT64) AS ship_cost
    FROM
        {{ref("stg_raw__ship")}}
)

SELECT
    om.orders_id
    ,om.margin
    ,om.revenue
    ,om.quantity
    ,om.purchase_cost
    ,sd.shipping_fee
    ,sd.logcost
    ,sd.ship_cost
    ,ROUND(om.margin + sd.shipping_fee - sd.logcost - sd.ship_cost,2) AS operational_margin
FROM
    orders_margin om
LEFT JOIN
    shipping_data sd
    ON om.orders_id = sd.orders_id
GROUP BY
    om.orders_id

