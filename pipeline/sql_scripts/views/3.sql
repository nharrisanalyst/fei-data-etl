BEGIN;

drop view if exists avg_claims_losses;
create view avg_claims_losses as 
	select year, 
		    round(Avg(non_cat_fire_claims),2) as avg_non_cat_fire_claims,
			round(Avg(non_cat_fire_losses),2) as avg_non_cat_fire_losses,
			round(Avg(non_cat_smoke_claims),2) as avg_non_cat_smoke_claims,
			round(Avg(non_cat_smoke_losses),2) as avg_non_cat_smoke_losses,
			round(Avg(cat_fire_claims),2) as avg_cat_fire_claims,
			round(Avg(cat_fire_losses),2) as avg_cat_fire_losses,
			round(Avg(cat_smoke_claims),2) as avg_cat_smoke_claims,
			round(Avg(cat_smoke_losses),2) as avg_cat_smoke_losses
	from fire_pp_claims_and_losses
	group by year;
COMMIT