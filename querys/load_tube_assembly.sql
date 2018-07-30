insert into dbo.tube_assembly(tube_assembly_id)
select distinct tube_assembly_id
from dbo.stg_materials