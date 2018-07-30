insert into dbo.price_quote(id_materials, id_supplier, quote_date, annual_usage, min_order_quantity, bracket_pricing, quantity, cost)
select distinct
m.id as id_materials
,s.id as id_supplier
,pq.quote_date
,pq.annual_usage
,pq.min_order_quantity
,pq.bracket_pricing
,pq.quantity
,pq.cost
from dbo.stg_price_quote pq
inner join dbo.materials m
	on pq.tube_assembly_id = pq.tube_assembly_id
inner join dbo.supplier s
	on s.supplier = pq.supplier
