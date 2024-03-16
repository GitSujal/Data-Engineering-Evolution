import os
import sqlalchemy
import yaml
import pandas as pd


config = yaml.safe_load(open('config.yml'))
# create a new db engine
server = config['db_credentials']['server']
database = config['db_credentials']['database']
user = config['db_credentials']['user']
password = os.getenv('DP101_PASSWORD')

sql_engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
                                  connect_args={'connect_timeout': 30})


# Read job_processed table from the database
df = pd.read_sql('SELECT * FROM jobs_processed', sql_engine)

# Print the first 5 rows of the dataframe
print(df.head())

