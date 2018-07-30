WITH cte 
AS (
select distinct tube_assembly_id,
CASE WHEN component_id = 'NA' then NULL else component_id end component_id,
quantity
from
(select distinct
ta.id as tube_assembly_id,
sm.component_id_1, 
sm.component_id_2, 
sm.component_id_3, 
sm.component_id_4, 
sm.component_id_5, 
sm.component_id_6, 
sm.component_id_7, 
sm.component_id_8,
sm.quantity_1, 
sm.quantity_2, 
sm.quantity_3, 
sm.quantity_4, 
sm.quantity_5, 
sm.quantity_6, 
sm.quantity_7, 
sm.quantity_8
from dbo.stg_materials sm
left join dbo.tube_assembly ta
	on ta.tube_assembly_id = sm.tube_assembly_id

)sub_main
UNPIVOT 
( 
component_id FOR c IN (component_id_1, 
component_id_2, component_id_3, component_id_4, component_id_5, 
component_id_6, component_id_7, component_id_8) 
) comp
UNPIVOT 
( 
quantity FOR q IN (quantity_1, 
quantity_2, quantity_3, quantity_4, quantity_5, 
quantity_6, quantity_7, quantity_8) 
) qtd
WHERE RIGHT(c,1) =  RIGHT(q, 1)
and  quantity <> 'NA'
)
insert into dbo.materials(tube_assembly_id, component_id, quantity)
select cte.tube_assembly_id,
c.id as component_id,
cte.quantity
from cte
inner join dbo.component c
 	on c.component_id = cte.component_id
 	

