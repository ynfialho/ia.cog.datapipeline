insert into dbo.component_type(component_type_id, component_type) 
select distinct component_type_id, CASE WHEN [type] = 'NA' THEN NULL ELSE [type] END as component_type 
from dbo.stg_component