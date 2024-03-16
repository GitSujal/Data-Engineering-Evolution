import logging
import argparse
import pandas as pd
import yaml
import sqlalchemy
import os
from seek_scraper import SeekScraper
from data_preparation import main as data_prep_main

# set up logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(filename='seek_scraper.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


config = yaml.safe_load(open('config.yml'))
# create a new db engine
server = config['db_credentials']['server']
database = config['db_credentials']['database']
user = config['db_credentials']['user']
password = os.getenv('DP101_PASSWORD')

sql_engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
                                  connect_args={'connect_timeout': 30})


def scrape_data(job: str, location: str, jobs_to_scrape: int, debug: bool=False, 
                save_local:bool = False,
                save_sql:bool = False, sql_engine: sqlalchemy.engine.base.Engine = None
                ) -> pd.DataFrame:
    """
    scrape data from seek
    :param job: job title to scrape
    :param location: location to scrape
    :param jobs_to_scrape: number of jobs to scrape
    :param debug: debug mode
    :return: dataframe with scraped data 
    """
    ss = SeekScraper(job=job, location=location, jobs_to_scrape=jobs_to_scrape, debug=debug, sql_engine=sql_engine)
    logger.info(f'Starting to scrape {jobs_to_scrape} jobs for {job} in {location}')
    data = ss(save_local=save_local, save_sql=save_sql)
    # print success message with job count  job and location
    print(f'Successfully scraped {jobs_to_scrape} jobs for {job} in {location}')
    logger.info(f'Successfully scraped {jobs_to_scrape} jobs for {job} in {location}')
    
    # call the data preparation function
    data_prep_main()

    return data



if __name__ == '__main__':
    # parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-j','--job', type=str, default='data-analyst', help='Job title to scrape (default: data-analyst)')
    parser.add_argument('-l','--location', type=str, default='perth', help='Location to scrape (default: perth)')
    parser.add_argument('-n','--num_jobs_to_scrape', type=int, default=100, help='Number of jobs to scrape (default: 100)')
    parser.add_argument('--debug', type=bool, default=False, help='Debug mode (default: False)')
    args = parser.parse_args()

    # call the scrape data function
    scrape_data(job=args.job, location=args.location, jobs_to_scrape=args.num_jobs_to_scrape,
                 debug=args.debug)

