with source as (
    
    {#-
    Changed from using seed data to the structured streaming delta table
    #}
    select * from {{ source('jaffle_shop_payments_raw') }}

),

renamed as ( 

    select
        id as payment_id,
        order_id,
        payment_method,

        -- `amount` is currently stored in cents, so we convert it to dollars
        amount / 100 as amount

    from source

)

select * from renamed
