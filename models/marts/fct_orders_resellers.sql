{{ config(materialized='table') }}

with order_lines as (

    select *
    from {{ ref('stg_order_lines_resellers') }}

),

orders as (

    select
        order_nummer,
        order_datum,
        'completed'                      as order_status,
        land_code,
        max(bedrijfsnaam)                as bedrijfsnaam,
        max(email)                       as email,
        sum(net_bedrag)                  as net_bedrag,
        sum(aantal)                      as totaal_aantal
    from order_lines
    group by order_nummer, order_datum, land_code

)

select * from orders
