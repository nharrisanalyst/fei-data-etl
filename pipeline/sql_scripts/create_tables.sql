Begin; 
    
    drop table if exists zipcodes CASCADE;
    drop table if exists cities CASCADE;
    drop table if exists counties CASCADE;

    -- drop all views 
    drop view if exists all_avg;
    drop view if exists avg_claims_losses;
    drop view if exists avg_fire_ppc;
    drop MATERIALIZED view if exists fire_pp_claims_and_losses;
  

    create table counties(
        county_id Serial PRIMARY KEY,
        county VARCHAR (100) UNIQUE NOT NULL
    );

    create table cities(
        city_id Serial PRIMARY KEY,
        city VARCHAR (100) UNIQUE NOT NULL,
        county_id INT,
        CONSTRAINT fk_county FOREIGN KEY(county_id) REFERENCES counties(county_id)
    );

    create table zipcodes(
        zipcode BIGINT NOT NULL PRIMARY KEY,
        city_id INT,
        CONSTRAINT fk_city FOREIGN KEY(city_id) REFERENCES cities(city_id)
    );

COMMIT;
