Begin;

drop view if exists avg_fire_ppc;
create view avg_fire_ppc as 
	select 
		p.year, 
		p.zipcode, 
		f."Average Fire Risk" as avg_fire_risk,
		p."Average PPC Class" as avg_ppc_class
	from ppc p 
	join fire_data f
		on f.year = p.year and f.zipcode = p.zipcode
		where p.zipcode in ('18','19','20','21','22','23');
COMMIT;