"""
Create label outcomes table
Create cohort table for each time period
"""
# coding: utf-8

import sys
import yaml
import sqlalchemy
import logging
import pandas as pd

from sqlalchemy.sql import text

logging.basicConfig(filename='events_table.log')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


def get_db_conn(postgres_config):
    with open(postgres_config, 'r') as f:
        config = yaml.load(f)
    dbtype = 'postgres'

    user = config['user']
    host = config['host']
    port = config['port']
    db = config['database']
    passcode = config['password']
    url = '{}://{}:{}@{}:{}/{}'.format(dbtype,
                                       user,
                                       passcode,
                                       host,
                                       port,
                                       db)
    return sqlalchemy.create_engine(url, echo=False)


def create_events_table(table_name=None):
    """
    Create events table with name table_name.
    In:
        - table_name: (str) optional, default is events
    """
    if not table_name:
        table_name = 'events'

    engine = get_db_conn('/group/dsapp-lab/luigi.yaml')
    connection = engine.connect()

    drop_query = text("drop table if exists public.{};"
                      .format(table_name)).execution_options(autocommit=True)
    connection.execute(drop_query)

    create_query = text("""
                    CREATE TABLE IF NOT EXISTS public.{} (
                        entity_id integer,
                        outcome_date date,
                        outcome boolean);
                    """.format(table_name)).execution_options(autocommit=True)

    connection.execute(create_query, table_name=table_name)

    query = text("""
                with observed_status as (
                                    select
                                          mrn,
                                          case
                                            when max(start_date) - min(start_date) >= 90
                                                then false --is adherent; gets turned into 0
                                            else true -- not adherent; gets turned into 1
                                          end as flag
                                    from staging.encounter_diagnoses
                                    where id_provider = 1 and
                                            enc_eio_o = 1 and
                                            attending_service in ('Infectious Diseases', 'Ped Infectious Diseases',
                                                                    'Internal Medicine', 'Hematology/Oncology') and
                                            start_date between :as_of_date and :as_of_date + '12 months'::interval
                                    group by mrn)

                insert into public.{}
                (entity_id, outcome_date, outcome)
                select mrn as entity_id,
                        :as_of_date as outcome_date,
                        case when flag = true
                                then true
                            when flag is null
                                then true
                            else false
                        end as outcome

                from staging.cohort_diagnoses
                left join observed_status using (mrn);
                """.format(table_name)).execution_options(autocommit=True)

    for year in range(2008, 2017):
        for month in range(1, 13):
            for day in range(1, 32):
                try:
                    connection.execute(query, as_of_date=pd.datetime(year, month, day, 0, 0))
                except:
                    continue


def create_outcomes_table(table_name=None):
    """
    Create outcomes table with name table_name.
    Outcomes table has the label (not-adherent(T) or adherent(F))
        for every mrn for every date range in the study
    In:
        - table_name: (str) optional, default is outcomes
    Still todo:
        - add the count of visits in the interval as a parameter
          (default now is 2)
    """
    if not table_name:
        table_name = 'outcomes'
    valid_appt_gap = 90  # days
    # This is the gap between appointments for them to count towards adherence
    prediction_horizon_time = 1
    prediction_horizon_unit = 'year'
    engine = get_db_conn('/group/dsapp-lab/luigi.yaml')
    connection = engine.connect()
    drop_query = text("drop table if exists public.{};"
                      .format(table_name)).execution_options(autocommit=True)
    connection.execute(drop_query)
    create_query = text("""
                        CREATE TABLE IF NOT EXISTS public.{} (
                            entity_id integer,
                            outcome_start_date date,
                            outcome_end_date date,
                            outcome boolean);
                    """.format(table_name)).execution_options(autocommit=True)
    connection.execute(create_query, table_name=table_name)
    query = text("""
        with observed_status as (
            select
                mrn,
                case
                    when max(start_date) - min(start_date) >= :valid_appt_gap
                    then false --is adherent; gets turned into 0
                    else true -- not adherent; gets turned into 1
                    end as flag
            from staging.encounter_diagnoses
            where id_provider = 1
                and enc_eio_o = 1 -- outpatient visit
                and attending_service in ('Infectious Diseases',
                    'Ped Infectious Diseases', 'Internal Medicine',
                    'Hematology/Oncology')
                and start_date between :as_of_date
                    and :as_of_date
                        + cast(:prediction_horizon_time||' '||
                            :prediction_horizon_unit as interval)
                        - '1 day'::interval
            group by mrn ),
        min_start_date as (
            select
                mrn,
                min(start_date) as min_start_date
                from staging.encounter_diagnoses
                where id_provider = 1
                    and enc_eio_o = 1
                    and attending_service in ('Infectious Diseases',
                        'Ped Infectious Diseases', 'Internal Medicine',
                        'Hematology/Oncology')
                group by mrn )
        insert into public.{}
            (entity_id, outcome_start_date, outcome_end_date, outcome)
        select mrn as entity_id,
            :as_of_date as outcome_start_date,
            :as_of_date
                + cast(:prediction_horizon_time||' '||
                    :prediction_horizon_unit as interval)
                - '1 day'::interval as outcome_end_date,
            case
                when flag = true
                    then true
                when flag is null
                    then true
                else false
            end as outcome
        from staging.cohort_diagnoses
        left join observed_status using (mrn)
        left join min_start_date x using (mrn);
        """.format(table_name)).execution_options(autocommit=True)
    for d in pd.date_range('1-1-2008', '12-31-2016'):
        connection.execute(query,
                           as_of_date=pd.datetime(d.year,
                                                  d.month, d.day, 0, 0),
                           valid_appt_gap=valid_appt_gap,
                           prediction_horizon_time=prediction_horizon_time,
                           prediction_horizon_unit=prediction_horizon_unit)


def create_cohort_table(table_name=None):
    """
    Create cohort table with name table_name.
    Cohort table has the whether an mrn is in the cohort or not for
        every date range in the study
    In:
        - table_name: (str) optional, default is cohort
    """
    if not table_name:
        table_name = 'cohort'
    engine = get_db_conn('/group/dsapp-lab/luigi.yaml')
    connection = engine.connect()
    drop_query = text("drop table if exists public.{};"
                      .format(table_name)).execution_options(autocommit=True)
    connection.execute(drop_query)
    query = """
            create temp table last_appt as
            select distinct on (mrn)
                mrn, start_date as last_appt_date
            from staging.encounter_diagnoses
            where mrn in (select distinct(mrn) from public.events)
                and attending_service in ('Hematology/Oncology',
                    'Infectious Diseases', 'Ped Infectious Disease',
                    'Internal Medicine')
            order by 1, 2 desc;
    """
    connection.execute(query)
    query = """
            create temp table first_appt as
            select distinct on (mrn)
                mrn, start_date as first_appt_date
            from staging.appt_status
            where mrn in (select distinct(mrn) from public.events)
                and attending_service in ('Hematology/Oncology',
                    'Infectious Diseases', 'Ped Infectious Disease',
                    'Internal Medicine')
            order by 1, 2 asc;
    """
    connection.execute(query)
    # every mrn, every date, in or out of cohort
    create_query = """
    create table cohort as
        with appts as (
            select x.mrn, first_appt_date, last_appt_date
            from last_appt x
            join first_appt y on x.mrn=y.mrn
        )
        select mrn, first_appt_date, last_appt_date,
            outcome_start_date, outcome_end_date,
            outcome_end_date - last_appt_date as days_since_last_appt,
            (outcome_start_date - last_appt_date) < 730
                AND (outcome_start_date - first_appt_date) >= 0 as in_cohort
        from appts a
        join outcomes y
        on a.mrn = y.entity_id;
    """
    connection.execute(create_query)


if __name__ == '__main__':
    """
    There are two options:
        option a) only create events table as used by Triage
        option b) create outcomes and cohort table for modeling outside of Triage

    To use option a, use:
    python label_maker.py 'triage_events' table_name

    To use option b, use:
    python label_maker.py 'some_other_string' outcome_table_name cohort_table_name

    If options are used without passing table names, the functions still run, but tables
    are created with default names.
    """
    if sys.argv[1] == 'triage_events':
        if len(sys.argv) == 3:
            create_events_table(sys.argv[2])
        else:
            create_events_table()
    else:
        if len(sys.argv) == 4:
            create_outcomes_table(sys.argv[2])
            create_cohort_table(sys.argv[3])
        else:
            create_outcomes_table()
            create_cohort_table()
