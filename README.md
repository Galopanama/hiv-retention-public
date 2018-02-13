# HIV Retention

## Description

Repo for HIV Retention DSaPP project (Public code).

## From raw to modeling

To create the tables needed for modeling, execute:

```
python etl/cli.py etl/queries/clean_data.sql  etl/queries/create_states_table.sql  etl/queries/expert_and_demographic_features.sql
```

Run all the code in DEV_load_cdph_common_schema.ipynb

cli.py accesses etl/inventory.yaml to find out which CSVs have to get used for the tables in raw.
etl/queries/clean_data.sql cleans the raw tables and moves them to staging.
etl/queries/create_states_table.sql creates the states table that is used by Triage to know when an individual should be included in the modeling process.
etl/queries/expert_and_demographic_features.sql creates two feature tables.

In order to create the events table, execute:

```
python etl/label_maker.py
```

The events table gets used by Triage to create the labels.
Once the tables are created, you can run a Triage experiment:

```
pipeline/Triage_Run.py pipeline/parameter_config.yaml
```

The experiment_config tells Triage which features and models to use. When you pass the true argument, the experiment adds the regex features.

## Analysis of results

Triage stores its results in the DB. Some common queries for the analysis of model performance can be found in queries/comp.sql and queries/performance_eval.sql. 

## Contributors

- Hannes Koenig (koenigh@uchicago.edu)
- Avishek Raman Kumar (avishekkumar@uchicago.edu)
- Arthi Ramachandran (aramachandran1@medicine.bsd.uchicago.edu)
- Christina Sung (csung1@uchicago.edu)
- Joseph Thomas Walsh (jtwalsh@uchicago.edu)