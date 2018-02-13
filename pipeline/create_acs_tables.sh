#!/bin/bash

for year in $(seq 2009 2015); do
    echo "----"
    echo $year
    table="acs${year}_5yr"

    echo """drop table if exists lookup_acs.$table;""" | psql
    
    echo """
    create table lookup_acs.$table as
    (select
	'12/31/$year'::date as knowledge_date,
	geoid,
	b01003001 as total_population,
	b02009001 as total_black,
	b05012002 as native_born, b05012003 as foreign_born,
	b07003004 as mobility_same_house, b07003007 as mobility_moved_same_county,
	b07003010 as mobility_moved_diff_county_same_state, b07003013 as mobility_moved_diff_state,
	b07003016 as mobility_moved_abroad,
	b09010002 as with_ssisnap,
	b17022002 as ratio_lt_1_3, b17022022 as ratio_1_3_to_1_49,
	b17022042 as ratio_1_5_to_1_84,	b17022062 as ratio_gte_1_85,
	b19001002 as income_lt_10k, b19001003 as income_10_to_15k,
	b19001004 as income_15_to_20k, b19001005 as income_20_to_25k,
	b19001006 as income_25_to_30k, b19001007 as income_30_to_35k,
	b19001008 as income_35_to_40k, b19001009 as income_40_to_45k,
	b19001010 as income_45_to_50k, b19001011 as income_50_to_60k,
	b19001012 as income_60_to_75k, b19001013 as income_75_to_100k,
	b19001014 as income_100_to_125k, b19001015 as income_125_to_150k,
	b19001016 as income_150_to_200k, b19001017 as income_gt_200k,
	b08013001 as travel_time_work,
	b08015001 as num_vehicles_to_work,
	b08303002/(b08303001+1) as travel_lt_5,
	(b08303003+b08303004+b08303005+b08303006+b08303007)/(b08303001+1) as travel_5_to_30,
	(b08303008+b08303009+b08303010+b08303011)/(b08303001+1) as travel_30_to_60,
	(b08303012+b08303013)/(b08303001+1) as travel_gt_60,
	b12007001 as age_at_first_marriage_male,
	b12007002 as age_at_first_marriage_female,
	(b15002003+b15002020)/(b15002001+1) as no_schooling, (b15002004+b15002021)/(b15002001+1) as elementary_school,
	(b15002005+b15002006+b15002022+b15002023)/(b15002001+1) as middle_school,
	(b15002007+b15002008+b15002009+b15002010+b15002011+b15002024+b15002025+b15002026+b15002027+b15002028)/(b15002001+1) as high_school,
	(b15002012+b15002029)/(b15002001+1) as lt_1yr_college, (b15002013+b15002030)/(b15002001+1) as some_college,
	(b15002014+b15002031)/(b15002001+1) as associates, (b15002015+b15002032)/(b15002001+1) as bachelors,
	(b15002016+b15002033)/(b15002001+1) as masters, (b15002017+b15002034)/(b15002001+1) as prof_degree,
	(b15002018+b15002035)/(b15002001+1) as phd
     from $table.b01003 -- total population
     join $table.b02009 using (geoid) -- black population
     join $table.b05012 using (geoid) -- nativity
     join $table.b07003 using (geoid) -- geographic mobility
     join $table.b09010 using (geoid) -- SSI/Food stamps/SNAP/etc
     join $table.b17022 using (geoid) -- ratio of income to poverty level
     join $table.b19001 using (geoid) -- household income
     join $table.b08013 using (geoid) -- aggregate travel time to work
     join $table.b08015 using (geoid) -- number of vehicles to travel to work
     join $table.b08303 using (geoid) -- travel time to work
     join $table.b12007 using (geoid) -- med age at first marriage
     join $table.b15002 using (geoid) -- educational attainment
    );""" | psql
done


echo """drop table if exists lookup_acs.all_years;""" | psql
echo """
create table lookup_acs.all_years as
     (select * from lookup_acs.acs2009_5yr
     union 
     select * from lookup_acs.acs2010_5yr
     union 
     select * from lookup_acs.acs2011_5yr
     union 
     select * from lookup_acs.acs2012_5yr
     union 
     select * from lookup_acs.acs2013_5yr
     union 
     select * from lookup_acs.acs2014_5yr
     union 
     select * from lookup_acs.acs2015_5yr
     );
""" | psql
