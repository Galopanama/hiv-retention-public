/*
Query to create previous
Infectious Diseases appointment
count.
*/
DROP TABLE IF EXISTS
    features.previous_id_appt_count;

CREATE TABLE features.previous_id_appt_count AS(

select
	mrn as entity_id,
	start_date,
	row_number() OVER (partition BY mrn ORDER BY start_date) - 1 as previous_appt_count
from staging.encounter_diagnoses
where enc_eio_o=1
	and id_provider=1
	and attending_service in ('Infectious Diseases', 'Ped Infectious Diseases',
  								'Internal Medicine', 'Hematology/Oncology'));
