/*
Create the states table for triage.
*/
DROP TABLE IF EXISTS
    states;

CREATE TABLE states AS(

  with earliest_appts as (
		select
			mrn,
			min(start_date) as earliest_appt_date
		from staging.appt_status
		where
			enc_eio_o = 1 and
			id_provider = 1 and
			attending_service in ('Infectious Diseases', 'Ped Infectious Diseases',
									'Internal Medicine', 'Hematology/Oncology') and
			encounter_type in ('Appointment', 'Office Visit', 'Hospital Encounter',
								'Nurse-Only visit', 'Procedure') and
			appt_status in ('Completed', 'Canceled', 'No Show',
							'Left without seen', 'Arrived') and
			(extract(year from start_date) between 2008 and 2016) and
			(extract(year from end_date) between 2008 and 2016)
	  	group by mrn)

	select
		mrn as entity_id,
		case
			when ((dob + interval '18 year')::date > earliest_appt_date)
				then (dob + interval '18 year')
				else earliest_appt_date
		end as start_time,
		case
			when date_of_death is null
			   then '2020-01-01'::date
			else date_of_death
		end as end_time,
		'active'::text as state
	from staging.cohort_diagnoses
	left join earliest_appts using (mrn));
