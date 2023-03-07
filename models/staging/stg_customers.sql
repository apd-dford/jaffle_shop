with source as (

    {#-
    Changed from using seed data to the structured streaming delta table
    #}
    select * from {{ source('jaffle_shop', 'jaffle_shop_customers_raw') }}

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from renamed
