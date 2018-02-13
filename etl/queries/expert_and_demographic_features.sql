/*
Queries to create tables for expert features:
missed ID appointments,
missed appointments,
diagnosis with substance use,
positive toxicology test,
viral loads
cd4 counts.
*/

CREATE SCHEMA IF NOT EXISTS features;

/*
Table for diagnosis with substance use feature. All diagnoses
have been classified as related to substance use or not.
We are building a feature that indicates if a date was associated
with a substance use related diagnosis.
*/
DROP TABLE IF EXISTS
    features.diagnosis_w_substance_use;

CREATE TABLE features.diagnosis_w_substance_use AS(
  select
        mrn as entity_id,
        start_date as date_col,
        max(substance_use) as diagnosis_w_substance_use
  from staging.diagnosis_diagnoses
  join staging.encounter_diagnoses
          using (mrn, bill_num)
  where
    start_date between '1/1/2008' and '8/31/2016'
  group by mrn, date_col);


/*
Table for positive TOXICOLOGY SCREEN, URINE tests feature. A screening
can be used for detection of multiple drugs; thus, there can be one
positive test among 10 tests. As soon as one test was positive,
we count the day as a day with a positive test result.
*/
DROP TABLE IF EXISTS
    features.positive_substance_test;

CREATE TABLE features.positive_substance_test AS(
  select
        mrn as entity_id,
        result_time as date_col,
        max((ord_value ilike 'positive')::int) as test_positive /*there can be multiple tests per day.*/
  from staging.lab_diagnoses
  where
    proc_name ilike 'toxicology screen, urine' and
    result_time between '1/1/2008' and '8/31/2016'
  group by 1,2);


/*
Table for missed ID appointments feature. Individuals can have multiple
ID appointments per day. If at least one ID appointment was completed, we
classify the day as a day without missed ID appointments.
*/
DROP TABLE IF EXISTS
    features.missed_id_appt_table;

CREATE TABLE features.missed_id_appt_table AS(
  select
  	mrn as entity_id,
  	start_date as date_col,
  	case
  		when max((lower(appt_status) = 'completed')::int)=1 then 0
      when max((lower(appt_status) = 'canceled')::int)=1 then 0
  		else max((lower(appt_status) in ('no show', 'left without seen'))::int)
  	end as missed_id_appt
  from staging.appt_status
  where
  	lower(attending_service) in ('infectious diseases', 'ped infectious diseases', 'internal medicine', 'hematology/oncology') and
    lower(encounter_type) in ('appointment', 'office visit', 'hospital encounter', 'nurse-only visit', 'procedure') and
    enc_eio_o = 1 and
  	id_provider = 1 and
    appt_status is not null and
    start_date between '1/1/2008' and '8/31/2016'
  group by 1,2);


/*
Table for missed appointments feature. Individuals can have multiple
appointments per day. If at least one appointment was MISSED, we
classify the day as a day with a missed appointment.
There are multiple times when an individual has multiple entries (completed, canceled, etc)
for what seems to be the same appointment. In order to deal with this, we group on unique
characteristics of an appointment first. If there is one COMPLETED status within
an appointment 'group' we count the appointment as completed.
We are not filtering on attending_service, but are keeping the
other filters for enc_eio_o, encounter_type, and appt_status to be consistent
with ID and non-ID tables.
Also, 'appt_status is not null' is kept because we do not know if
appointment was kept by patient or not. This can be seen as a conservative
approach to the definition of missing an appointment.
*/
DROP TABLE IF EXISTS
    features.missed_appt_table;

CREATE TABLE features.missed_appt_table AS(
  with aggregation_by_appt as (
    select
          mrn,
          attending_name,
          attending_service,
          start_date as date_col,
          end_date,
          case
            when max((lower(appt_status) = 'completed')::int)=1 then 0
            when max((lower(appt_status) = 'canceled')::int)=1 then 0
            else max((lower(appt_status) in ('no show', 'left without seen'))::int)
          end as missed_appt
      from staging.appt_status
      where
        appt_status is not null
        and start_date between '1/1/2008' and '8/31/2016'
      group by 1,2,3,4,5)
  select
        mrn as entity_id,
        date_col,
        case
          when sum(missed_appt) > 0 then 1
          else 0
        end as missed_appt_binary
  from aggregation_by_appt
  group by 1,2);


/*
Queries to create table for demographic features:
gender, age, race
*/
DROP TABLE IF EXISTS
    features.demographic_features;

CREATE TABLE features.demographic_features AS(
  select
  	mrn as entity_id,
    '2008-01-01'::date as knowledge_date_column,
    /* Triage needs knowledge date for a feature; setting to beginning of study.
    States table takes care of only including individuals when they are active.
    In the future, we could set the knowledge_date based on an individuals
    first encounter.*/
  	dob,
  	sex,
  	race
  from staging.cohort_diagnoses);


/*
Table for viral loads.
*/
DROP TABLE IF EXISTS
    features.viral_loads;

CREATE TABLE features.viral_loads AS(
  select
      mrn as entity_id,
      result_vl_date as date_col,
      vl_result
  from
      staging.viral_loads
  where
    result_vl_date is not null
    and result_vl_date between '1/1/2008' and '8/31/2016');

/*
Table for CD4 counts.
*/
DROP TABLE IF EXISTS
    features.cd4_counts;

CREATE TABLE features.cd4_counts AS(
  select
    mrn as entity_id,
    result_time,
    cast(ord_value as float) as cd4_count
  from
    staging.lab_diagnoses
  where
    component_name ilike 'ABSOLUTE CD4'
    and ord_value <> 'REQUEST CREDITED'
    and result_time is not null
    and result_time between '1/1/2008' and '8/31/2016');


/*
Table for missed non-ID appointments feature. Individuals can have multiple
non-ID appointments per day. If at least one non-ID appointment was MISSED, we
classify the day as a day with a missed non-ID appointment.
There are multiple times when an individual has multiple entries (completed, canceled, etc)
for what seems to be the same appointment. In order to deal with this, we group on unique
characteristics of an appointment first. If there is one COMPLETED status within
an appointment 'group' we count the appointment as completed.

Note that we are not including encounter_type as part of the unique
characteristics of an appointment. Therefore, if an attending completes an appointment
within the same service, we count the appointment as completed - even if there was
another appointment of different type (e.g., hospital encounter) that was not completed.

OPEN QUESTION: Should we only include appointments with enc_eio_o = 1 as these
reflect out patient appointments and therefore, appointments that truly can be kept
versus hospital encounters?

We are currently not building this table; filtering for non-id appts can lead to not obvious results.
Filtering out ID appts can obscure that individual kept or canceled ID appointment, but did not show up
for related non-ID appt. With our current logic, keeping or canceling the related appointment (same attending,
attending_service, etc.) means that the other appointment was not missed. We currently don't have a way
to account for that; therefore, we are not including that feature.

DROP TABLE IF EXISTS
    features.missed_non_id_appt_table;

CREATE TABLE features.missed_non_id_appt_table AS(
  with aggregation_by_non_id_appt as (
      select
          mrn,
          attending_name,
          attending_service,
          start_date as date_col,
          end_date,
          case
            when max((lower(appt_status) = 'completed')::int)=1 then 0
            when max((lower(appt_status) = 'canceled')::int)=1 then 0
            else max((lower(appt_status) in ('no show', 'left without seen'))::int)
          end as missed_non_id_appt
      from staging.appt_status
      where
        not (
          lower(attending_service) in ('infectious diseases', 'ped infectious diseases', 'internal medicine', 'hematology/oncology') and
          lower(encounter_type) in ('appointment', 'office visit', 'hospital encounter', 'nurse-only visit', 'procedure') and
          enc_eio_o = 1 and
          id_provider = 1
          )
        and appt_status is not null
      group by 1,2,3,4,5)
  select
        mrn,
        date_col,
        case
          when sum(missed_non_id_appt) > 0 then 1
          else 0
        end as missed_non_id_appt
  from aggregation_by_non_id_appt
  group by 1,2);
*/
