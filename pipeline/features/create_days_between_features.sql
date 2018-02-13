drop table if exists features_cs.days_between_id_appts;
create table features_cs.days_between_id_appts as (
SELECT entity_id,
      visit_date -1 as date_col, -- we need to offset by 1 day to include day of appointment as well
      visit_date - LAG(visit_date) OVER (
      PARTITION BY entity_id ORDER BY visit_date) as days_between_appts
    FROM (
      SELECT DISTINCT entity_id,
                      visit_date
              from events_ucm.events
              WHERE event_type='visit'
        			AND visit_date IS NOT null
        			AND id_provider_flag=1
        			AND visit_status_id in (2,4,8)) as x);
CREATE INDEX days_between_id_appts_idx ON features_cs.days_between_id_appts (entity_id, date_col);
