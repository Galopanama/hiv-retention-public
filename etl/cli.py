#!/usr/bin/env python
"""
Basic Configuration for Loading CSV files
"""
import datetime
import os
import re
import sys

import click
import pandas as pd
import yaml
from six.moves.configparser import ConfigParser
from config import PostgresConfig


def cli(files):
    """
    Run your pipeline
    In:
        - files: (list) of paths to sql queries
    """
    config = 'luigi.yaml'
    inventory = 'inventory.yaml'

    load_command(config,
                 inventory,
                 schema='raw',
                 resume=True,
                 verbose=True)

    for file in files:
        execute_sql(config, file)


def execute_sql(config, path_to_sql):
    """
    Executes sql statements found in path_to_sql.
    In:
        config: config file
        path_to_sql: file path to sql queries
    """
    if config.endswith('.yaml') or config.endswith('.yml'):
        with open(config, 'r') as f:
            config = yaml.load(f)
            postgres_config = PostgresConfig(**config['postgres'])
    else:
        raise ValueError("--config must be either a yaml file")

    with open(path_to_sql, 'r') as query_file:
        queries=query_file.read().replace('\n', '')

    postgres_config.execute_in_psql(queries)


def basic_cleaning(to_clean):
    """
    Cleanes numbers, replace spaces with underscores
    and removes brackets and parenthesis.

    Parameter
    ---------
    to_clean: str
       string to clean

    Return
    ------
    cleaned: str
       cleaned string
    """
    cleaned = to_clean.strip()
    cleaned = re.findall(r'^\d*(.*)$', cleaned)[0]
    cleaned = re.sub(r'[\s.]+', '_', cleaned) #spaces turned into underscores
    cleaned = re.sub(r'[\[\]\(\)]', '', cleaned) #brackets and parenthesis replaced
    cleaned = re.sub(r'[-]','_',cleaned)
    cleaned = re.sub(r':','_',cleaned)
    cleaned = re.sub(r'%','pct',cleaned)
    cleaned = re.sub(r'&','and',cleaned)
    cleaned = cleaned.lower()
    return cleaned


def load_command(config, inventory, schema, resume, verbose):
    """Load data from csvs into the postgres database.

	config: config file

    Also does some basic cleaning (e.g., lower casing names and removing spaces).
    You need to specify a config file, e.g.::

        [postgres]
        host=<SOME HOST>
        port=<SOME PORT>
        user=<SOME USER>
        database=<SOME DATABASE>
        password=<SOME PASSWORD>
        inventor: config_yml

    and an inventory of files to upload. The inventory has the format::

        inventory:
          - file_name: file_to_upload.csv
            table_name: what_to_name_the_table
            column_map:
              "original column name":
                name: new_column_name
                type: date|some_python_type

    All options *except* the `file_name` are optional, and if not specified
    will be filled in with defaults. E.g., by default, table names and columns
    get cleaned into lower_snake_case and the type is whatever pandas guesses
    it is after 100 rows. (Note in particular that pandas usually doesn't detect
    dates very well from csvs.)

    WARNING: If you run this, it will drop all the tables that already exist
    in the schema with the given names **unless** you call with `--resume`.

	schema = 'raw'
	resume = True
	verbose = False
    """
    if config.endswith('.yaml') or config.endswith('.yml'):
        with open(config, 'r') as f:
            config = yaml.load(f)
            postgres_config = PostgresConfig(**config['postgres'])
    else:
        raise ValueError("--config must be either a yaml file")

    with open(inventory, 'r') as f:
        inventory = yaml.load(f)
    inventory = inventory['inventory']

    if verbose:
        print("Found {} tables to upload".format(len(inventory)))
        print("Verifying existence of inputs.")



    do_not_exist = [desc['file_name'] for desc in inventory
                    if not os.path.isfile(desc['file_name'])]
    if do_not_exist:
        click.echo("The following files in the inventory don't exist: "
                   "{}".format(',\n'.join(do_not_exist)))
        click.echo("Please fix and try again.")
        sys.exit(1)

    postgres_config = PostgresConfig(**config['postgres'])

    for file_description in inventory:
        file_name = file_description['file_name']
        basic_file_name = os.path.basename(file_name).rsplit('.', 1)[0]
        table_name = file_description.get('table_name') or basic_cleaning(basic_file_name)

        if resume and postgres_config.does_table_exist(schema=schema, table=table_name):
            click.echo("Table {}.{} exists. Skipping.".format(schema, table_name))
            continue

        column_map = file_description.get('column_map', {})
        df_data = pd.read_csv(file_name,
                              nrows=10000,
                              na_values=["NULL"])

        # Redo types
        make_big_int = {}
        for column_name, value in column_map.items():
            new_type = value['type']
            if new_type == 'date':
                df_data[column_name] = df_data[column_name].apply(pd.Timestamp)
            elif new_type.startswith('date'):
                fmt_str = re.findall(r'^date\((.*)\)$', new_type)[0]
                df_data[column_name] = df_data[column_name].apply(
                    lambda x: datetime.datetime.strftime(x, fmt_str))
            else:
                df_data[column_name] = df_data[column_name].astype(new_type)
        click.echo("Column map " + str(column_map))
        renamed_columns = {}


        for column_name in df_data.columns:
            if column_name in column_map and 'name' in column_map[column_name]:
                renamed_columns[column_name] = column_map[column_name]['name']
            else:
                renamed_columns[column_name] = basic_cleaning(column_name)



        #df_data_data = df_data.reset_index()
        #index_column_name = df_data.columns[0]
        #df_data_data = df_data_data.rename(columns={index_column_name: 'id'})
        df_data.rename(columns=renamed_columns,
                       inplace=True)
        print(df_data.head())


        sql_statement = pd.io.sql.get_schema(df_data, 'REPLACE ME')
        sql_statement = sql_statement.replace(
            '"REPLACE ME"', '"{}"."{}"'.format(schema, table_name))
        
        #sql_statement = sql_statement.replace('"id" INTEGER', '"id" SERIAL PRIMARY KEY')

        if verbose:
            click.echo("Creating table with statement:")
            click.echo(sql_statement)

        postgres_config.create_and_drop(table=table_name, create_statement=sql_statement,
                                        schema=schema)
        copy_statement = (
            "\copy \"{schema}\".\"{table_name}\" ({column_names}) FROM '{file_name}' "
            "WITH CSV HEADER NULL ''".format(
                schema=schema,
                table_name=table_name,
                column_names=','.join(df_data.columns[:]),
                file_name=file_name))
        postgres_config.execute_in_psql(copy_statement)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("""
                \n\nHave you considered adding queries for the
                staging tables, first features table, and the states
                table?\n\n
                """)
    if len(sys.argv) > 0:
        files = sys.argv[1:]
    else:
        files = None
    cli(files)
