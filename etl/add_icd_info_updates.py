import yaml
import json
import joblib
import pydotplus

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib
from sqlalchemy.sql import text

from sklearn import tree

from sqlalchemy import create_engine

postgres_config = '/group/dsapp-lab/luigi.yaml'

def get_db_conn(postgres_config):
    """
    This is really bad code
    """
    with open(postgres_config, 'r') as f:
        config = yaml.load(f)
    dbtype = 'postgres'
    
    #previously was: user = config['postgres']['user']
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
    conn = create_engine(url)
    return conn

def query_db(query, conn, params=None):
    """
    Queries DB and returns pandas df.
    """
    if params:
        return pd.read_sql(query, conn, params=params)
    else:        
        return pd.read_sql(query, conn)

engine = get_db_conn(postgres_config)
connection = engine.connect()


df_data = pd.read_csv("/group/ridgway-lab/HIV retention/to verify/icd9_toaddextrainfo_wsubstance.csv", na_values=["NULL"])
df_data.columns = map(str.lower, df_data.columns)
df_data.to_sql("icd9_info", engine, schema = 'lookup_ucm', if_exists='replace', index=False)
df_data = pd.read_csv("/group/ridgway-lab/HIV retention/to verify/icd10_toaddextrainfo_wsubstance.csv", na_values=["NULL"])
df_data.columns = map(str.lower, df_data.columns)
df_data.to_sql("icd10_info", engine, schema = 'lookup_ucm', if_exists='replace', index=False)
