-- Medications
drop table if exists features_cs.meds;
create table features_cs.meds as
       (select distinct entity_id,
               treatment_start_date as date_col,
	       treatment_type_id as medication
        from events_ucm.events
        where event_type='treatment'
	      and treatment_start_date is not null
	);
create index on features_cs.meds(entity_id);
create index on features_cs.meds(date_col);



drop table if exists features_cs.meds_art;
create table features_cs.meds_art as
       (select distinct entity_id,
               treatment_start_date as date_col,
               treatment_type_id as medication
        from events_ucm.events
        where event_type='treatment'
	      and treatment_start_date is not null
	      and art::bool
        );
create index on features_cs.meds_art(entity_id);
create index on features_cs.meds_art(date_col);

drop table if exists features_cs.meds_psych;
create table features_cs.meds_psych as
       (select distinct entity_id,
               treatment_start_date as date_col,
               treatment_type_id as medication
        from events_ucm.events
        where event_type='treatment'
	      and treatment_start_date is not null
	      and psychiatric_medication::bool
        );
create index on features_cs.meds_psych(entity_id);
create index on features_cs.meds_psych(date_col);

drop table if exists features_cs.meds_opioid;
create table features_cs.meds_opioid as
       (select distinct entity_id,
               treatment_start_date as date_col,
               treatment_type_id as medication
        from events_ucm.events
        where event_type='treatment'
	      and treatment_start_date is not null
	      and opioid::bool
        );
create index on features_cs.meds_opioid(entity_id);
create index on features_cs.meds_opioid(date_col);

drop table if exists features_cs.meds_oi_prophylaxis;
create table features_cs.meds_oi_prophylaxis as
       (select distinct entity_id,
               treatment_start_date as date_col,
               treatment_type_id as medication
        from events_ucm.events
        where event_type='treatment'
	      and treatment_start_date is not null
              and oi_prophylaxis::bool
        );
create index on features_cs.meds_oi_prophylaxis(entity_id);
create index on features_cs.meds_oi_prophylaxis(date_col);
