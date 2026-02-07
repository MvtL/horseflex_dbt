with Orderlines_resellers as (
  
select
        order_nummer,
        net_verkoopsprijs   as Orderlines_net_verkoopprijs,
        net_bedrag          as Orderlines_net_bedrag,
        Bedrijfsnaam        as Orderlines_Bedrijfsnaam,
        Quantity_ordered    as Orderlines_Quantity_ordered,
        Supplier_item_code  as Orderlines_Supplier_item_code,
    from {{ ref('stg_orders_resellers') }}
)

select * from Orderlines_resellers