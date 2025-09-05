BEGIN;

drop MATERIALIZED view if exists fire_pp_claims_and_losses;

create MATERIALIZED view fire_pp_claims_and_losses as
	select p.year, a.zipcode, city, county, 
		f."Average Fire Risk",
		p."Average PPC Class",
		a."Non Cat Cov A Fire Claims" + a."Non Cat Cov C Fire Claims" as non_cat_fire_claims,
		a."Non Cat Cov A Fire Losses" + a."Non Cat Cov C Fire Losses" as non_cat_fire_losses,
		a."Non Cat Cov A Smoke Claims" + a."Non Cat Cov C Smoke Claims" as non_cat_smoke_claims,
		a."Non Cat Cov A Smoke Losses" + a."Non Cat Cov C Smoke Losses" as non_cat_smoke_losses,
		a."Cat Cov A Fire Claims" + a."Cat Cov C Fire Claims" as cat_fire_claims,
		a."Cat Cov A Fire Losses" + a."Cat Cov C Fire Losses" as cat_fire_losses,
		a."Cat Cov A Smoke Claims" + a."Cat Cov C Smoke Claims" as cat_smoke_claims,
		a."Cat Cov A Smoke Losses" + a."Cat Cov C Smoke Losses" as cat_smoke_losses
	from all_companies a
	join ppc p 
		on a.year = p.year and a.zipcode = p.zipcode
	join fire_data f 
		on f.year = p.year and f.zipcode = p.zipcode
	join zipcodes z 
		on z.zipcode = a.zipcode
	join cities using(city_id)
	join counties using(county_id)
	where a.zipcode> 30;

COMMIT