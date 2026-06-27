{{ config(materialized='table') }}

with klanten as (

    select
        klant_id,
        gebruikersnaam,
        klant_rol,
        voornaam,
        achternaam,
        voornaam + ' ' + achternaam   as klant_naam,
        klant_email,
        klant_telefoon,
        straat,
        stad,
        postcode,
        land_code,
        is_betalende_klant,
        aangemaakt_op,
        gewijzigd_op
    from {{ ref('stg_horseflex_klanten') }}

)

select *
from klanten
