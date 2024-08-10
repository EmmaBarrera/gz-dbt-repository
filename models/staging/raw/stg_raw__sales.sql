with 

source as (

    select * from {{ source('raw', 'sales') }}

),

renamed as (

    select
        date_date,
        orders_id,
        pdt_id AS product_id,
        revenue,
        quantity,
        orders_id || '_' || pdt_id AS sales_pk

    from source

)

select * from renamed

