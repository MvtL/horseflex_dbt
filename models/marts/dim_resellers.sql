{{ config(materialized='table') }}

with orders as (

    select
        bedrijfsnaam,
        email,
        voornaam,
        achternaam,
        straat,
        huisnummer,
        toevoeging,
        postcode,
        stad,
        land_code,
        order_datum,
        row_number() over (
            partition by bedrijfsnaam
            order by order_datum desc
        ) as _rn
    from {{ ref('stg_order_lines_resellers') }}
    where bedrijfsnaam is not null

),

resellers as (

    select
        bedrijfsnaam,
        email,
        voornaam + ' ' + achternaam                               as contactpersoon,
        voornaam,
        achternaam,
        straat,
        huisnummer,
        toevoeging,
        postcode,
        stad,
        land_code
    from orders
    where _rn = 1

)

select *
from resellers
