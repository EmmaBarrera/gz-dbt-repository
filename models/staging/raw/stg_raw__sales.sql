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
        orders_id || '_' || product_id AS sale_pk -- Creation of PK from this table 

    from source

)

select * from renamed
