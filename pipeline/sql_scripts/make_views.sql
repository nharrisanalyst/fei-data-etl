BEGIN;

drop MATERIALIZED view if exists fire_pp_claims_and_losses;

create MATERIALIZED view fire_pp_claims_and_losses as
	select p.year, a.zipcode, city, county, 
		f."Average Fire Risk",
		p."Average PPC Class",
		hz."FHSZ_avg_rating" as fhsz_ranking,
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
	join fhsz_data hz
		on hz."ZCTA5CE20" = a.zipcode
	join zipcodes z 
		on z.zipcode = a.zipcode
	join cities using(city_id)
	join counties using(county_id)
	where a.zipcode> 30;


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


BEGIN;

Drop view if EXISTS fhsz_zipcodes;

create  view fhsz_zipcodes as
	Select "GEOID20" as real_zipcode
	from fhsz_data;

COMMIT;

BEGIN;
Drop view if EXISTS zipcodes_exclude;
create view zipcodes_exclude as
	select real_zipcode 
	from fhsz_zipcodes
	where real_zipcode not in (
	select distinct(zipcode)
	from fire_data);
COMMIT;

BEGIN;

Drop view if EXISTS real_zipcodes;

CREATE VIEW real_zipcodes as
	select real_zipcode as real_zipcodes 
	from fhsz_zipcodes
	WHERE real_zipcode not in (
		SELECT real_zipcode 
		from zipcodes_exclude);

Commit;


