with 

source as (

    select 
        date_date,
        orders_id,
        pdt_id,
        revenue,
        quantity,
    from {{ source('raw', 'sales') }}

),

renamed as (

    select
        date_date,
        orders_id,
        pdt_id AS product_id,
        revenue,
        quantity
    from source

)

select * from renamed

