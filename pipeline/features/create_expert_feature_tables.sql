
/* First Appt */
drop table if exists features_cs.first_appt;
create table features_cs.first_appt as
    (select entity_id,
            min(visit_date) as date_col,
            0 as first_appt_flag
     from events_ucm.events
     where event_type = 'visit'
     group by 1
     union
     select entity_id,
            (min(visit_date)-'1day'::interval)::date as date_col,
            1 as first_appt_flag
     from events_ucm.events
     where event_type = 'visit'
     group by 1);

create index on features_cs.first_appt(entity_id);
create index on features_cs.first_appt(date_col);

-- missed Infectious Disease Appt
drop table if exists features_cs.missed_id_appt;
create table features_cs.missed_id_appt as
    (select entity_id,
	    visit_date as date_col,
            case when visit_status_id = 2 then 1
                 when visit_status_id = 6 then 1
                 else 0
	    end as missed_id_appt
     from events_ucm.events
     where id_provider_flag=1
            and event_type = 'visit'
     union
     select entity_id,
           (min(visit_date) - '1day'::interval)::date as date_col,
           0 as missed_id_appt
     from events_ucm.events
     where id_provider_flag = 1
     and event_type = 'visit'
     group by 1);   
create index on features_cs.missed_id_appt(entity_id);
create index on features_cs.missed_id_appt(date_col);

-- missed appt (any)
drop table if exists features_cs.missed_appt;
create table features_cs.missed_appt as
    (select entity_id,
	    visit_date as date_col,
            case when visit_status_id = 2 then 1
                 when visit_status_id = 6 then 1
                 else 0
	    end as missed_appt
     from events_ucm.events
     where event_type = 'visit'
     union
     select entity_id,
           (min(visit_date) - '1day'::interval)::date as date_col,
           0 as missed_id_appt
     from events_ucm.events
     where event_type = 'visit'
     group by 1);   
create index on features_cs.missed_appt(entity_id);
create index on features_cs.missed_appt(date_col);

-- diagnosis with substance use
drop table if exists features_cs.substance_use;
create table features_cs.substance_use as
    (select entity_id, visit_date as date_col, substance_use
     from events_ucm.events
     where event_type = 'visit'
     union
     select entity_id,
            (min(visit_date) - '1day'::interval)::date as date_col,
            0 as substance_use
     from events_ucm.events
     where event_type = 'visit'
     group by 1);
create index on features_cs.substance_use(entity_id);
create index on features_cs.substance_use(date_col);


drop table features_cs.positive_substance;
create table features_cs.positive_substance as
       (
	select entity_id,
	       lab_result_date as date_col,
	       case when lab_test_value ilike '%positive%' then 1 else 0 end as tox_pos
        from events_ucm.events
        left join lookup_ucm.test_types
             on lab_test_type_cd = test_type_id
        where event_type = 'lab'
              and test_type ilike '%toxicology%'
	union
	select entity_id,
            (min(visit_date) - '1day'::interval)::date as date_col,
            0 as tox_pos
     	from events_ucm.events
     	where event_type = 'visit'
	group by 1	       
       );

drop table if exists features_cs.expert;
create table features_cs.expert as
       (
       select *
       from features_cs.missed_id_appt
       full outer join  features_cs.missed_appt using (entity_id, date_col)
       full outer join features_cs.substance_use using (entity_id, date_col)
       full outer join features_cs.positive_substance using (entity_id, date_col)
       );
create index on features_cs.expert(entity_id);
create index on features_cs.expert(date_col);
