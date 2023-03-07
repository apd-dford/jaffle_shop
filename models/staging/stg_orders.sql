with source as (

    {#-
    Changed from using seed data to the structured streaming delta table
    #}
    select * from {{ source('jaffle_shop_orders_raw') }}

),

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date as order_timestamp,
        status

    from source

)

select * from renamed
