/*
The staging schema is for clean tables.
*/

CREATE SCHEMA IF NOT EXISTS
       staging;

/*
Cleaning the final_mrns table. We are going to use it
to narrow down the list of MRNs that are part of the
cohort.
*/
DROP TABLE IF EXISTS
   staging.final_mrns CASCADE;

CREATE TABLE staging.final_mrns AS(
 SELECT DISTINCT
 	mrn::int,
  to_date(enroll_date, 'MM/DD/YY') as enroll_date,
  yearfirstvisit::int as yearfirstvisit,
  to_date(firstvisitdt, 'MM/DD/YY') as firstvisitdt
 FROM raw.final_mrns
 WHERE
 	n_pop=1);

ALTER TABLE staging.final_mrns
 ADD PRIMARY KEY (mrn);

/*
#####################
#####################
#####################
*/
/*
Clean the cohort table to get
the proper dates and keep MRNs from modeling population.
Removing duplicates after dropping column pharmacy_name.
*/
DROP TABLE IF EXISTS
    staging.cohort_diagnoses CASCADE;

CREATE TABLE staging.cohort_diagnoses AS(
  SELECT DISTINCT
  	mrn,
  	to_date(dob, 'MM/DD/YYYY') as dob,
  	to_date(date_of_death, 'MM/DD/YYYY') as date_of_death,
  	sex,
  	race,
  	ethnicity,
  	address_line1,
  	address_line2,
  	city,
  	postal_code
  FROM raw.cohort_diagnoses
  JOIN staging.final_mrns AS cohort_table
  	USING (mrn));

ALTER TABLE staging.cohort_diagnoses
 ADD PRIMARY KEY (mrn),
 ALTER COLUMN mrn TYPE INT,
 ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.final_mrns (mrn);

/*
#####################
#####################
#####################
*/

/*
Clean the encounter table to get
the proper dates. Removing duplicates after
dropping column index_enc.
*/
DROP TABLE IF EXISTS
     staging.encounter_diagnoses;

CREATE TABLE staging.encounter_diagnoses AS(
  SELECT DISTINCT
      mrn,
      bill_num,
      (enc_eio LIKE '%I%')::int AS enc_eio_i,
  		(enc_eio LIKE '%O%')::int AS enc_eio_o,
  		(enc_eio LIKE '%E%')::int AS enc_eio_e,
  		(enc_eio IS NULL)::int AS enc_eio_null,
      start_date,
      end_date,
      trim(attending_name) AS attending_name,
      trim(attending_service) AS attending_service,
      id_provider,
      fin_class
  FROM
      raw.encounter_diagnoses
  LEFT JOIN
  	(SELECT DISTINCT attending_name, attending_service, id_provider
  		FROM raw.appt_status) AS distinct_provider
  		USING (attending_name, attending_service)
  WHERE
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses)
    AND start_date::date between '1/1/2008' and '8/31/2016'
    AND end_date::date between '1/1/2008' and '8/31/2016');

ALTER TABLE staging.encounter_diagnoses
 ALTER COLUMN mrn TYPE INT,
ALTER COLUMN bill_num TYPE INT,
ALTER COLUMN start_date TYPE DATE
  USING to_date(start_date, 'MM/DD/YYYY'),
ALTER COLUMN end_date TYPE DATE
  USING to_date(end_date, 'MM/DD/YYYY'),
ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS encounter_mrn;
CREATE INDEX encounter_mrn ON staging.encounter_diagnoses (mrn);

DROP INDEX IF EXISTS encounter_id_provider;
CREATE INDEX encounter_id_provider ON staging.encounter_diagnoses (id_provider);

DROP INDEX IF EXISTS encounter_enc_eio_o;
CREATE INDEX encounter_enc_eio_o ON staging.encounter_diagnoses (enc_eio_o);

DROP INDEX IF EXISTS encounter_attending_service;
CREATE INDEX encounter_attending_service ON staging.encounter_diagnoses (attending_service);

DROP INDEX IF EXISTS encounter_start_date;
CREATE INDEX encounter_start_date ON staging.encounter_diagnoses (start_date);

/*
#####################
#####################
#####################
*/

/*
Clean the viral loads table to get
proper dates.
*/
DROP TABLE IF EXISTS
     staging.viral_loads;

CREATE TABLE staging.viral_loads AS(
  SELECT
    mrn,
    seq_vl,
    bill_num,
    to_date(result_vl_date, 'MM/DD/YY') AS result_vl_date,
    to_timestamp(result_vl_time, 'HH:MI AM, PM')::TIME AS result_vl_time,
    proc_name,
    component_name,
    vl_result
  FROM
    raw.viral_loads
  WHERE
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses)
    AND result_vl_date::date between '1/1/2008' and '8/31/2016');

ALTER TABLE staging.viral_loads
 ALTER COLUMN mrn TYPE INT,
ALTER COLUMN seq_vl TYPE INT,
ALTER COLUMN bill_num TYPE INT,
ALTER COLUMN vl_result TYPE REAL,
ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS viral_loads_mrn;
CREATE INDEX viral_loads_mrn ON staging.viral_loads (mrn);

DROP INDEX IF EXISTS viral_loads_result_vl_date;
CREATE INDEX viral_loads_result_vl_date ON staging.viral_loads (result_vl_date);

/*
#####################
#####################
#####################
*/


/*
Clean the appointment status table to get
the proper dates. Removing duplicates after
dropping column index_enc.
*/
DROP TABLE IF EXISTS
     staging.appt_status;

CREATE TABLE staging.appt_status AS(
  SELECT DISTINCT
      mrn,
      bill_num::bigint,
      (enc_eio LIKE '%I%')::int AS enc_eio_i,
  		(enc_eio LIKE '%O%')::int AS enc_eio_o,
  		(enc_eio LIKE '%E%')::int AS enc_eio_e,
  		(enc_eio IS NULL)::int AS enc_eio_null,
      start_date,
      end_date,
      trim(attending_name) AS attending_name,
      id_provider,
      trim(attending_service) AS attending_service,
      encounter_type,
      appt_status,
      fin_class
  FROM
      raw.appt_status
  WHERE
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses)
    AND start_date::date between '1/1/2008' and '8/31/2016'
    AND end_date::date between '1/1/2008' and '8/31/2016');


ALTER TABLE staging.appt_status
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE BIGINT,
  ALTER COLUMN start_date TYPE DATE
    USING to_date(start_date, 'MM/DD/YYYY'),
  ALTER COLUMN end_date TYPE DATE
    USING to_date(end_date, 'MM/DD/YYYY');

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS appt_status_mrn;
CREATE INDEX appt_status_mrn ON staging.appt_status (mrn);

DROP INDEX IF EXISTS appt_status_encounter_type;
CREATE INDEX appt_status_encounter_type ON staging.appt_status (encounter_type);

DROP INDEX IF EXISTS appt_status_appt_status;
CREATE INDEX appt_status_appt_status ON staging.appt_status (appt_status);

DROP INDEX IF EXISTS appt_status_start_date;
CREATE INDEX appt_status_start_date ON staging.appt_status (start_date);

/*
#####################
#####################
#####################
*/


/*
Creating a table for pharmacy_name that we
dropped from cohort because it lead to duplicate
entries.
*/
DROP TABLE IF EXISTS
     staging.cohort_pharmacy_diagnoses;

CREATE TABLE staging.cohort_pharmacy_diagnoses AS(
  SELECT DISTINCT
    mrn,
    pharmacy_name
  FROM
      raw.cohort_diagnoses
  WHERE
    pharmacy_name IS NOT NULL AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.cohort_diagnoses
  ALTER COLUMN mrn TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);


/*
#####################
#####################
#####################
*/


/*
Clean the diagnosis table.
TO-DO:
  1) 3 entries have both ICD9 and ICD10 codes. Do we want to keep them?
*/
DROP TABLE IF EXISTS
     staging.diagnosis_diagnoses;

CREATE TABLE staging.diagnosis_diagnoses AS(
  SELECT *
  FROM
      raw.diagnosis_diagnoses
  WHERE
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses) AND
    (icd9_dx IS NOT NULL OR icd10_dx IS NOT NULL));

ALTER TABLE staging.diagnosis_diagnoses
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS diagnosis_diagnoses_mrn;
CREATE INDEX diagnosis_diagnoses_mrn ON staging.diagnosis_diagnoses (mrn);

DROP INDEX IF EXISTS diagnosis_diagnoses_bill_num;
CREATE INDEX diagnosis_diagnoses_bill_num ON staging.diagnosis_diagnoses (bill_num);

/*
#####################
#####################
#####################
*/


/*
Clean the dx history table.
*/
DROP TABLE IF EXISTS
     staging.dx_history;

CREATE TABLE staging.dx_history AS(
  SELECT *
  FROM
      raw.dx_history
  WHERE
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.dx_history
  ALTER COLUMN mrn TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS dx_history_mrn;
CREATE INDEX dx_history_mrn ON staging.dx_history (mrn);

/*
#####################
#####################
#####################
*/


/*
Cleaning the lab diagnoses table to get proper dates.
To-Do: Current approach only keeps date, but not hour associated
with result_time. Is that okay for us?
*/
DROP TABLE IF EXISTS
     staging.lab_diagnoses;

CREATE TABLE staging.lab_diagnoses AS(
  SELECT *
  FROM
      raw.lab_diagnoses
  WHERE
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses)
    AND result_time::date between '1/1/2008' and '8/31/2016');

ALTER TABLE staging.lab_diagnoses
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN result_time TYPE DATE
    USING to_date(result_time, 'MM/DD/YY HH24:MI'),
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS lab_diagnoses_mrn;
CREATE INDEX lab_diagnoses_mrn ON staging.lab_diagnoses (mrn);

DROP INDEX IF EXISTS lab_diagnoses_bill_num;
CREATE INDEX lab_diagnoses_bill_num ON staging.lab_diagnoses (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the medication diagnoses table to get proper dates.
To-Do: There are rows with missing start dates. Keep them?
*/
DROP TABLE IF EXISTS
     staging.medication_diagnoses;

CREATE TABLE staging.medication_diagnoses AS(
  SELECT *
  FROM
      raw.medication_diagnoses
  WHERE
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses)
    AND start_date::date between '1/1/2008' and '8/31/2016'
    AND end_date::date between '1/1/2008' and '8/31/2016');

ALTER TABLE staging.medication_diagnoses
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN start_date TYPE DATE
    USING to_date(start_date, 'MM/DD/YYYY'),
  ALTER COLUMN end_date TYPE DATE
    USING to_date(end_date, 'MM/DD/YYYY'),
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS medication_diagnoses_mrn;
CREATE INDEX medication_diagnoses_mrn ON staging.medication_diagnoses (mrn);

DROP INDEX IF EXISTS medication_diagnoses_bill_num;
CREATE INDEX medication_diagnoses_bill_num ON staging.medication_diagnoses (bill_num);

DROP INDEX IF EXISTS medication_diagnoses_start_date;
CREATE INDEX medication_diagnoses_start_date ON staging.medication_diagnoses (start_date);

/*
#####################
#####################
#####################
*/


/*
Cleaning the problem diagnoses table to get proper dates.
To-Do: Acknowledge that there are rows with missing bill_nums. In
  order to use data, we should NOT join on both mrn and bill_num.
  Instead, use noted_date.
*/
DROP TABLE IF EXISTS
     staging.problem_diagnoses;

CREATE TABLE staging.problem_diagnoses AS(
  SELECT *
  FROM
      raw.problem_diagnoses
  WHERE
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses)
    AND noted_date::date between '1/1/2008' and '8/31/2016'
    AND resolved_date::date between '1/1/2008' and '8/31/2016');

ALTER TABLE staging.problem_diagnoses
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN noted_date TYPE DATE
    USING to_date(noted_date, 'MM/DD/YYYY'),
  ALTER COLUMN resolved_date TYPE DATE
    USING to_date(resolved_date, 'MM/DD/YYYY'),
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS problem_diagnoses_mrn;
CREATE INDEX problem_diagnoses_mrn ON staging.problem_diagnoses (mrn);

DROP INDEX IF EXISTS problem_diagnoses_bill_num;
CREATE INDEX problem_diagnoses_bill_num ON staging.problem_diagnoses (bill_num);

DROP INDEX IF EXISTS problem_diagnoses_noted_date;
CREATE INDEX problem_diagnoses_noted_date ON staging.problem_diagnoses (noted_date);

/*
#####################
#####################
#####################
*/


/*
Cleaning the nadir_cd4s table to get proper dates.
*/
DROP TABLE IF EXISTS
     staging.nadir_cd4s;

CREATE TABLE staging.nadir_cd4s AS(
  SELECT
    mrn,
    seq_cd4,
    bill_num,
    result_cd4_date,
    proc_name,
    cd4_result
  FROM
      raw.nadir_cd4s
  WHERE
  (cd4_result ~ '^[0-9\.]+$')
  AND mrn IN (SELECT mrn FROM staging.cohort_diagnoses)
  AND result_cd4_date::date between '1/1/2008' and '8/31/2016');

ALTER TABLE staging.nadir_cd4s
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN cd4_result TYPE double precision
    USING cd4_result::double precision,
  ALTER COLUMN result_cd4_date TYPE DATE
    USING to_date(result_cd4_date, 'MM/DD/YYYY'),
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS nadir_cd4s_mrn;
CREATE INDEX nadir_cd4s_mrn ON staging.nadir_cd4s (mrn);

DROP INDEX IF EXISTS nadir_cd4s_bill_num;
CREATE INDEX nadir_cd4s_bill_num ON staging.nadir_cd4s (bill_num);

DROP INDEX IF EXISTS nadir_cd4s_result_cd4_date;
CREATE INDEX nadir_cd4s_result_cd4_date ON staging.nadir_cd4s (result_cd4_date);

/*
#####################
#####################
#####################
*/


/*
Cleaning the social_diagnoses table.
*/
DROP TABLE IF EXISTS
     staging.social_diagnoses;

CREATE TABLE staging.social_diagnoses AS(
  SELECT *
  FROM
      raw.social_diagnoses
  WHERE
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.social_diagnoses
  ALTER COLUMN mrn TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS social_diagnoses_mrn;
CREATE INDEX social_diagnoses_mrn ON staging.social_diagnoses (mrn);

/*
#####################
#####################
#####################
*/


/*
Cleaning the gis_final table. Dropping columns
that are already present in other tables.
*/
DROP TABLE IF EXISTS
     staging.gis_final;

CREATE TABLE staging.gis_final AS(
  SELECT
    mrn,
    address_1,
    address_2,
    city,
    zipcode,
    pharmacy_name AS pharmacy_name_1,
    pharm_mail_in AS pharm_mail_in_1,
    pharmacy_name_2,
    pharm_mail_in_2,
    pharmacy_name_3,
    pharm_mail_in_3,
    pharmacy_name_4,
    pharm_mail_in_4
  FROM
      raw.gis_final
  WHERE
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.gis_final
  ALTER COLUMN mrn TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS gis_final_mrn;
CREATE INDEX gis_final_mrn ON staging.gis_final (mrn);

/*
#####################
#####################
#####################
*/


/*
Cleaning the ancillarydata_regex_casesensitive_nostem_neg table.
*/
DROP TABLE IF EXISTS
     staging.ancillarydata_regex_casesensitive_nostem_neg;

CREATE TABLE staging.ancillarydata_regex_casesensitive_nostem_neg AS(
  SELECT *
  FROM
      raw.ancillarydata_regex_casesensitive_nostem_neg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.ancillarydata_regex_casesensitive_nostem_neg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS ancillarydata_regex_casesensitive_nostem_neg_mrn;
CREATE INDEX ancillarydata_regex_casesensitive_nostem_neg_mrn
  ON staging.ancillarydata_regex_casesensitive_nostem_neg (mrn);

DROP INDEX IF EXISTS ancillarydata_regex_casesensitive_nostem_neg_bill_num;
CREATE INDEX ancillarydata_regex_casesensitive_nostem_neg_bill_num
  ON staging.ancillarydata_regex_casesensitive_nostem_neg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the ancillarydata_regex_casesensitive_nostem_noneg table.
*/
DROP TABLE IF EXISTS
     staging.ancillarydata_regex_casesensitive_nostem_noneg;

CREATE TABLE staging.ancillarydata_regex_casesensitive_nostem_noneg AS(
  SELECT *
  FROM
      raw.ancillarydata_regex_casesensitive_nostem_noneg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.ancillarydata_regex_casesensitive_nostem_noneg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS ancillarydata_regex_casesensitive_nostem_noneg_mrn;
CREATE INDEX ancillarydata_regex_casesensitive_nostem_noneg_mrn
  ON staging.ancillarydata_regex_casesensitive_nostem_noneg (mrn);

DROP INDEX IF EXISTS ancillarydata_regex_casesensitive_nostem_noneg_bill_num;
CREATE INDEX ancillarydata_regex_casesensitive_nostem_noneg_bill_num
  ON staging.ancillarydata_regex_casesensitive_nostem_noneg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the ancillarydata_regex_casesensitive_stem_neg table.
*/
DROP TABLE IF EXISTS
     staging.ancillarydata_regex_casesensitive_stem_neg;

CREATE TABLE staging.ancillarydata_regex_casesensitive_stem_neg AS(
  SELECT *
  FROM
      raw.ancillarydata_regex_casesensitive_stem_neg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.ancillarydata_regex_casesensitive_stem_neg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS ancillarydata_regex_casesensitive_stem_neg_mrn;
CREATE INDEX ancillarydata_regex_casesensitive_stem_neg_mrn
  ON staging.ancillarydata_regex_casesensitive_stem_neg (mrn);

DROP INDEX IF EXISTS ancillarydata_regex_casesensitive_stem_neg_bill_num;
CREATE INDEX ancillarydata_regex_casesensitive_stem_neg_bill_num
  ON staging.ancillarydata_regex_casesensitive_stem_neg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the ancillarydata_regex_casesensitive_stem_noneg table.
*/
DROP TABLE IF EXISTS
     staging.ancillarydata_regex_casesensitive_stem_noneg;

CREATE TABLE staging.ancillarydata_regex_casesensitive_stem_noneg AS(
  SELECT *
  FROM
      raw.ancillarydata_regex_casesensitive_stem_noneg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.ancillarydata_regex_casesensitive_stem_noneg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS ancillarydata_regex_casesensitive_stem_noneg_mrn;
CREATE INDEX ancillarydata_regex_casesensitive_stem_noneg_mrn
  ON staging.ancillarydata_regex_casesensitive_stem_noneg (mrn);

DROP INDEX IF EXISTS ancillarydata_regex_casesensitive_stem_noneg_bill_num;
CREATE INDEX ancillarydata_regex_casesensitive_stem_noneg_bill_num
  ON staging.ancillarydata_regex_casesensitive_stem_noneg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the ancillarydata_regex_nocasesensitive_nostem_neg table.
*/
DROP TABLE IF EXISTS
     staging.ancillarydata_regex_nocasesensitive_nostem_neg;

CREATE TABLE staging.ancillarydata_regex_nocasesensitive_nostem_neg AS(
  SELECT *
  FROM
      raw.ancillarydata_regex_nocasesensitive_nostem_neg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.ancillarydata_regex_nocasesensitive_nostem_neg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS ancillarydata_regex_nocasesensitive_nostem_neg_mrn;
CREATE INDEX ancillarydata_regex_nocasesensitive_nostem_neg_mrn
  ON staging.ancillarydata_regex_nocasesensitive_nostem_neg (mrn);

DROP INDEX IF EXISTS ancillarydata_regex_nocasesensitive_nostem_neg_bill_num;
CREATE INDEX ancillarydata_regex_nocasesensitive_nostem_neg_bill_num
  ON staging.ancillarydata_regex_nocasesensitive_nostem_neg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the ancillarydata_regex_nocasesensitive_nostem_noneg table.
*/
DROP TABLE IF EXISTS
     staging.ancillarydata_regex_nocasesensitive_nostem_noneg;

CREATE TABLE staging.ancillarydata_regex_nocasesensitive_nostem_noneg AS(
  SELECT *
  FROM
      raw.ancillarydata_regex_nocasesensitive_nostem_noneg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.ancillarydata_regex_nocasesensitive_nostem_noneg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS ancillarydata_regex_nocasesensitive_nostem_noneg_mrn;
CREATE INDEX ancillarydata_regex_nocasesensitive_nostem_noneg_mrn
  ON staging.ancillarydata_regex_nocasesensitive_nostem_noneg (mrn);

DROP INDEX IF EXISTS ancillarydata_regex_nocasesensitive_nostem_noneg_bill_num;
CREATE INDEX ancillarydata_regex_nocasesensitive_nostem_noneg_bill_num
  ON staging.ancillarydata_regex_nocasesensitive_nostem_noneg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the ancillarydata_regex_nocasesensitive_stem_neg table.
*/
DROP TABLE IF EXISTS
     staging.ancillarydata_regex_nocasesensitive_stem_neg;

CREATE TABLE staging.ancillarydata_regex_nocasesensitive_stem_neg AS(
  SELECT *
  FROM
      raw.ancillarydata_regex_nocasesensitive_stem_neg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.ancillarydata_regex_nocasesensitive_stem_neg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS ancillarydata_regex_nocasesensitive_stem_neg_mrn;
CREATE INDEX ancillarydata_regex_nocasesensitive_stem_neg_mrn
  ON staging.ancillarydata_regex_nocasesensitive_stem_neg (mrn);

DROP INDEX IF EXISTS ancillarydata_regex_nocasesensitive_stem_neg_bill_num;
CREATE INDEX ancillarydata_regex_nocasesensitive_stem_neg_bill_num
  ON staging.ancillarydata_regex_nocasesensitive_stem_neg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the ancillarydata_regex_nocasesensitive_stem_noneg table.
*/
DROP TABLE IF EXISTS
     staging.ancillarydata_regex_nocasesensitive_stem_noneg;

CREATE TABLE staging.ancillarydata_regex_nocasesensitive_stem_noneg AS(
  SELECT *
  FROM
      raw.ancillarydata_regex_nocasesensitive_stem_noneg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.ancillarydata_regex_nocasesensitive_stem_noneg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS ancillarydata_regex_nocasesensitive_stem_noneg_mrn;
CREATE INDEX ancillarydata_regex_nocasesensitive_stem_noneg_mrn
  ON staging.ancillarydata_regex_nocasesensitive_stem_noneg (mrn);

DROP INDEX IF EXISTS ancillarydata_regex_nocasesensitive_stem_noneg_bill_num;
CREATE INDEX ancillarydata_regex_nocasesensitive_stem_noneg_bill_num
  ON staging.ancillarydata_regex_nocasesensitive_stem_noneg (bill_num);


/*
#####################
#####################
#####################
*/


/*
Cleaning the infectiousdata_regex_casesensitive_nostem_neg table.
*/
DROP TABLE IF EXISTS
     staging.infectiousdata_regex_casesensitive_nostem_neg;

CREATE TABLE staging.infectiousdata_regex_casesensitive_nostem_neg AS(
  SELECT *
  FROM
      raw.infectiousdata_regex_casesensitive_nostem_neg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.infectiousdata_regex_casesensitive_nostem_neg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS infectiousdata_regex_casesensitive_nostem_neg_mrn;
CREATE INDEX infectiousdata_regex_casesensitive_nostem_neg_mrn
  ON staging.infectiousdata_regex_casesensitive_nostem_neg (mrn);

DROP INDEX IF EXISTS infectiousdata_regex_casesensitive_nostem_neg_bill_num;
CREATE INDEX infectiousdata_regex_casesensitive_nostem_neg_bill_num
  ON staging.infectiousdata_regex_casesensitive_nostem_neg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the infectiousdata_regex_casesensitive_nostem_noneg table.
*/
DROP TABLE IF EXISTS
     staging.infectiousdata_regex_casesensitive_nostem_noneg;

CREATE TABLE staging.infectiousdata_regex_casesensitive_nostem_noneg AS(
  SELECT *
  FROM
      raw.infectiousdata_regex_casesensitive_nostem_noneg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.infectiousdata_regex_casesensitive_nostem_noneg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS infectiousdata_regex_casesensitive_nostem_noneg_mrn;
CREATE INDEX infectiousdata_regex_casesensitive_nostem_noneg_mrn
  ON staging.infectiousdata_regex_casesensitive_nostem_noneg (mrn);

DROP INDEX IF EXISTS infectiousdata_regex_casesensitive_nostem_noneg_bill_num;
CREATE INDEX infectiousdata_regex_casesensitive_nostem_noneg_bill_num
  ON staging.infectiousdata_regex_casesensitive_nostem_noneg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the infectiousdata_regex_casesensitive_stem_neg table.
*/
DROP TABLE IF EXISTS
     staging.infectiousdata_regex_casesensitive_stem_neg;

CREATE TABLE staging.infectiousdata_regex_casesensitive_stem_neg AS(
  SELECT *
  FROM
      raw.infectiousdata_regex_casesensitive_stem_neg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.infectiousdata_regex_casesensitive_stem_neg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS infectiousdata_regex_casesensitive_stem_neg_mrn;
CREATE INDEX infectiousdata_regex_casesensitive_stem_neg_mrn
  ON staging.infectiousdata_regex_casesensitive_stem_neg (mrn);

DROP INDEX IF EXISTS infectiousdata_regex_casesensitive_stem_neg_bill_num;
CREATE INDEX infectiousdata_regex_casesensitive_stem_neg_bill_num
  ON staging.infectiousdata_regex_casesensitive_stem_neg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the infectiousdata_regex_casesensitive_stem_noneg table.
*/
DROP TABLE IF EXISTS
     staging.infectiousdata_regex_casesensitive_stem_noneg;

CREATE TABLE staging.infectiousdata_regex_casesensitive_stem_noneg AS(
  SELECT *
  FROM
      raw.infectiousdata_regex_casesensitive_stem_noneg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.infectiousdata_regex_casesensitive_stem_noneg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS infectiousdata_regex_casesensitive_stem_noneg_mrn;
CREATE INDEX infectiousdata_regex_casesensitive_stem_noneg_mrn
  ON staging.infectiousdata_regex_casesensitive_stem_noneg (mrn);

DROP INDEX IF EXISTS infectiousdata_regex_casesensitive_stem_noneg_bill_num;
CREATE INDEX infectiousdata_regex_casesensitive_stem_noneg_bill_num
  ON staging.infectiousdata_regex_casesensitive_stem_noneg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the infectiousdata_regex_nocasesensitive_nostem_neg table.
*/
DROP TABLE IF EXISTS
     staging.infectiousdata_regex_nocasesensitive_nostem_neg;

CREATE TABLE staging.infectiousdata_regex_nocasesensitive_nostem_neg AS(
  SELECT *
  FROM
      raw.infectiousdata_regex_nocasesensitive_nostem_neg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.infectiousdata_regex_nocasesensitive_nostem_neg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS infectiousdata_regex_nocasesensitive_nostem_neg_mrn;
CREATE INDEX infectiousdata_regex_nocasesensitive_nostem_neg_mrn
  ON staging.infectiousdata_regex_nocasesensitive_nostem_neg (mrn);

DROP INDEX IF EXISTS infectiousdata_regex_nocasesensitive_nostem_neg_bill_num;
CREATE INDEX infectiousdata_regex_nocasesensitive_nostem_neg_bill_num
  ON staging.infectiousdata_regex_nocasesensitive_nostem_neg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the infectiousdata_regex_nocasesensitive_nostem_noneg table.
*/
DROP TABLE IF EXISTS
     staging.infectiousdata_regex_nocasesensitive_nostem_noneg;

CREATE TABLE staging.infectiousdata_regex_nocasesensitive_nostem_noneg AS(
  SELECT *
  FROM
      raw.infectiousdata_regex_nocasesensitive_nostem_noneg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.infectiousdata_regex_nocasesensitive_nostem_noneg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS infectiousdata_regex_nocasesensitive_nostem_noneg_mrn;
CREATE INDEX infectiousdata_regex_nocasesensitive_nostem_noneg_mrn
  ON staging.infectiousdata_regex_nocasesensitive_nostem_noneg (mrn);

DROP INDEX IF EXISTS infectiousdata_regex_nocasesensitive_nostem_noneg_bill_num;
CREATE INDEX infectiousdata_regex_nocasesensitive_nostem_noneg_bill_num
  ON staging.infectiousdata_regex_nocasesensitive_nostem_noneg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the infectiousdata_regex_nocasesensitive_stem_neg table.
*/
DROP TABLE IF EXISTS
     staging.infectiousdata_regex_nocasesensitive_stem_neg;

CREATE TABLE staging.infectiousdata_regex_nocasesensitive_stem_neg AS(
  SELECT *
  FROM
      raw.infectiousdata_regex_nocasesensitive_stem_neg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.infectiousdata_regex_nocasesensitive_stem_neg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS infectiousdata_regex_nocasesensitive_stem_neg_mrn;
CREATE INDEX infectiousdata_regex_nocasesensitive_stem_neg_mrn
  ON staging.infectiousdata_regex_nocasesensitive_stem_neg (mrn);

DROP INDEX IF EXISTS infectiousdata_regex_nocasesensitive_stem_neg_bill_num;
CREATE INDEX infectiousdata_regex_nocasesensitive_stem_neg_bill_num
  ON staging.infectiousdata_regex_nocasesensitive_stem_neg (bill_num);

/*
#####################
#####################
#####################
*/


/*
Cleaning the infectiousdata_regex_nocasesensitive_stem_noneg table.
*/
DROP TABLE IF EXISTS
     staging.infectiousdata_regex_nocasesensitive_stem_noneg;

CREATE TABLE staging.infectiousdata_regex_nocasesensitive_stem_noneg AS(
  SELECT *
  FROM
      raw.infectiousdata_regex_nocasesensitive_stem_noneg
  WHERE
    note_type != 'Scanned Progress Notes' AND
    mrn IN (SELECT mrn FROM staging.cohort_diagnoses));

ALTER TABLE staging.infectiousdata_regex_nocasesensitive_stem_noneg
  ALTER COLUMN mrn TYPE INT,
  ALTER COLUMN bill_num TYPE INT,
  ALTER COLUMN note_id TYPE INT,
  ADD CONSTRAINT cohort_mrn FOREIGN KEY (mrn) REFERENCES staging.cohort_diagnoses (mrn);

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS infectiousdata_regex_nocasesensitive_stem_noneg_mrn;
CREATE INDEX infectiousdata_regex_nocasesensitive_stem_noneg_mrn
  ON staging.infectiousdata_regex_nocasesensitive_stem_noneg (mrn);

DROP INDEX IF EXISTS infectiousdata_regex_nocasesensitive_stem_noneg_bill_num;
CREATE INDEX infectiousdata_regex_nocasesensitive_stem_noneg_bill_num
  ON staging.infectiousdata_regex_nocasesensitive_stem_noneg (bill_num);


/*
Cleaning the rw_mrns table. We will use
it as a
*/
DROP TABLE IF EXISTS
     staging.rw_mrns;

CREATE TABLE staging.rw_mrns AS(
  SELECT
    mrn,
    first_name,
    last_name,
    to_date(dob, 'MM/DD/YYYY') as dob
  FROM
      raw.rw_mrns);

ALTER TABLE staging.rw_mrns
  ALTER COLUMN mrn TYPE INT;

/*
Adding indexes on important columns.
*/
DROP INDEX IF EXISTS rw_mrns_mrn;
CREATE INDEX rw_mrns_mrn
  ON staging.rw_mrns (mrn);

/*
#####################
#####################
#####################
*/
