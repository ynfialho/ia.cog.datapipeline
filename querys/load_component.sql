INSERT INTO dbo.component(
component_id
,component_type_id
,connection_type_id
,outside_shape
,height_over_tube
,bolt_pattern_long
,bolt_pattern_wide
,groove	
,base_type
,base_diameter
,shoulder_diameter	
,unique_feature	
,orientation	
,weight)
select distinct
CASE WHEN sc.component_id = 'NA' then null else sc.component_id end as component_id
,ct.id as component_type_id
,ctype.id as connection_type_id
,CASE WHEN sc.outside_shape = 'NA' then null else sc.outside_shape end as outside_shape
,CAST(CASE WHEN sc.height_over_tube = 'NA' then NULL else sc.height_over_tube end as numeric(8,1)) as height_over_tube
,CAST(CASE WHEN sc.bolt_pattern_long = 'NA' then NULL else sc.bolt_pattern_long end as numeric(8,1)) as bolt_pattern_long
,CAST(CASE WHEN sc.bolt_pattern_wide = 'NA' then NULL else sc.bolt_pattern_wide end as numeric(8,1)) as bolt_pattern_wide
,CASE WHEN sc.groove = 'NA' then null else sc.groove end as groove	
,CASE WHEN sc.base_type = 'NA' then null else sc.base_type end as base_type
,CAST(CASE WHEN sc.base_diameter = 'NA' then NULL else sc.base_diameter end as numeric(8,1)) as base_diameter
,CAST(CASE WHEN sc.shoulder_diameter = 'NA' then NULL else sc.shoulder_diameter end as numeric(8,1)) as shoulder_diameter
,CASE WHEN sc.unique_feature = 'NA' then null else sc.unique_feature end as unique_feature	
,CASE WHEN sc.orientation = 'NA' then null else sc.orientation end as orientation	
,CAST(CASE WHEN sc.weight = 'NA' then NULL else sc.weight end as numeric(16,4)) as weight
from dbo.stg_component sc 
left join dbo.component_type ct
	on ct.component_type_id = sc.component_type_id
	and ISNULL(ct.component_type, 'NA') = sc.[type]
left join dbo.connection_type ctype
	on ISNULL(ctype.connection_type_id, '9999') = sc.connection_type_id

