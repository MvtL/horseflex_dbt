with prijslijst as (

    select
        SKU,
        Merk,
        Naam,
        Inhoud,
        Verpakking,
    from {{ ref('stg_prijslijst') }}
)

select *
from prijslijst