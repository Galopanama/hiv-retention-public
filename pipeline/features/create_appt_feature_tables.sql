drop table if exists features_cs.id_appt;
create table features_cs.id_appt as
       (
	select distinct entity_id,
	       visit_date as date_col,
	       case when status in ('completed', 'arrived', 'y') then 1 else 0 end as completed,
	       1 as scheduled,
	       case when status = 'canceled' then 1 else 0 end as cancelled,
	       case when status = 'no show' then 1 else 0 end as noshow,
	       (
                visit_date - LAG(visit_date) OVER (
                 PARTITION BY entity_id ORDER BY visit_date)
               )::INT as days_between_appts
        from events_ucm.events
	join lookup_ucm.status_codes
             on status_id = visit_status_id
	where event_type = 'visit'
	      and id_provider_flag=1	
       );
create index on features_cs.id_appt(entity_id);
create index on features_cs.id_appt(date_col);


drop table if exists features_cs.appt;
create table features_cs.appt as
       (
	select distinct entity_id,
	       visit_date as date_col,
	       case when status in ('completed', 'arrived', 'y') then 1 else 0 end as completed,
	       1 as scheduled,
	       case when status = 'canceled' then 1 else 0 end as cancelled,
	       case when status = 'no show' then 1 else 0 end as noshow,
	       (
                visit_date - LAG(visit_date) OVER (
                 PARTITION BY entity_id ORDER BY visit_date)
               )::INT as days_between_appts
        from events_ucm.events
	join lookup_ucm.status_codes
             on status_id = visit_status_id
	where event_type = 'visit'
       );
create index on features_cs.appt(entity_id);
create index on features_cs.appt(date_col);
