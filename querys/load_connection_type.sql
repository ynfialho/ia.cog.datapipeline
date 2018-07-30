insert into dbo.connection_type
select distinct case when connection_type_id = 'NA' then NULL when connection_type_id = '9999' then NULL
else connection_type_id end connection_type_id
from dbo.stg_component