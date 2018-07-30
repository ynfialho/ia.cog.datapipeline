insert into dbo.supplier(supplier)
select distinct supplier
from dbo.stg_price_quote