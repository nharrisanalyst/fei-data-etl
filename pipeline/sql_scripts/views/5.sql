BEGIN;

drop view if exists all_avg;
create view all_avg as 
	select year,
			avg_fire_risk,
			avg_ppc_class,
			avg_non_cat_fire_claims,
			avg_non_cat_fire_losses,
			avg_non_cat_smoke_claims,
			avg_non_cat_smoke_losses,
			avg_cat_fire_claims,
			avg_cat_fire_losses,
			avg_cat_smoke_claims,
			avg_cat_smoke_losses
	from avg_fire_ppc f
	join avg_claims_losses c using(year);

COMMIT;