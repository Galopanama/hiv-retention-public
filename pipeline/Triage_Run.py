
#!/usr/bin/env python
import sys
import os
import yaml
import logging
import sqlalchemy
from sqlalchemy.pool import NullPool

from catwalk.storage import FSModelStorageEngine
from triage.experiments import SingleThreadedExperiment

from architect.label_generators import BinaryLabelGenerator


class HIVLabelGenerator(BinaryLabelGenerator):
    """
    We are using this class to get a custom label definition.
    We will create an events table that is actually a labels table.
    """
    def __init__(self, events_table, db_engine):
        self.events_table = events_table
        self.db_engine = db_engine

    def generate(
        self,
        start_date,
        label_window,
        labels_table,
    ):
        query = """insert into {labels_table} (
            select
                {events_table}.entity_id,
                '{start_date}'::date as as_of_date,
                '{label_window}'::interval as label_window,
                'outcome' as label_name,
                'binary' as label_type,
                bool_or(outcome::bool)::int as label
            from {events_table}
            where '{start_date}' = outcome_date
            group by 1, 2, 3, 4, 5
        )""".format(
            events_table=self.events_table,
            labels_table=labels_table,
            start_date=start_date,
            label_window=label_window,
        )
        logging.debug('Running label generation query: %s', query)
        self.db_engine.execute(query)
        return labels_table

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
    print(url)
    return sqlalchemy.create_engine(url, poolclass=NullPool)



if __name__ == "__main__":
    config_file = sys.argv[1]
    #add_regex_features = sys.argv[2].lower() == 'true'
    #print("add regex features: ", str(add_regex_features))
    print(config_file)

    features_directory = 'features'
    try:
        # load main experiment config
        with open(config_file) as f:
            experiment_config = yaml.load(f)

        # load feature configs and update experiment config with their contents
        all_feature_aggregations = []
        for filename in os.listdir('{}/'.format(features_directory)):
            if filename.endswith(".yaml"):
                with open('{}/{}'.format(features_directory, filename)) as f:
                    feature_aggregations = yaml.load(f)
                    for aggregation in feature_aggregations['feature_aggregations']:
                        all_feature_aggregations.append(aggregation)
        experiment_config['feature_aggregations'] = all_feature_aggregations

    except yaml.parser.ParserError as e:
        print('{} config cannot be parsed.'.format(config_file))
        print(e)
        exit(1)
    except IOError:
        print('There is no file named', config_file)
        exit(1)
    except Exception as e:
        print('Cannot open file')
        print('Exception: ', e)
        exit(1)


    print(os.getcwd())
    print(os.path.dirname("~"))
    engine = get_db_conn('./luigi.yaml')

    print("Experiment config:\n", experiment_config)
    experiment = SingleThreadedExperiment(config=experiment_config,
                                          db_engine=engine,
                                          model_storage_class=FSModelStorageEngine,
                                          project_path='/group/dsapp-lab/triage_files/',
                                          label_generator_class=HIVLabelGenerator, replace=False)


    logging.basicConfig(level=logging.INFO)
    experiment.run()
