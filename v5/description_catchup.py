import os
import sqlalchemy
import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging

# set up a log file
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

logging.info("Starting the description catcheere....")

# create a new db engine
server = 'dp101server.database.windows.net'
database = 'dp101database'
user = 'dp101admin'
password = os.getenv('DP101_PASSWORD')

sql_engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
                                connect_args={'connect_timeout': 30})


def catch_up_descriptions(sql_engine):
    # catchup with job description
    sql = """select jobid from jobs where jobid not in (select jobid from jobs_descriptions);"""

    job_ids = pd.read_sql(sql, sql_engine)
    logging.info(f"Found {job_ids.shape[0]} jobs without description")

    if job_ids.shape[0] > 200:
        job_ids = job_ids.sample(200)['jobid'].to_list()
    else:
        job_ids = job_ids['jobid'].to_list()

    all_job_desc = pd.DataFrame()
    for job_id in job_ids:
        # print(f"Getting job {job_id} description")
        seek_url = f"https://www.seek.com.au/job/{job_id}?ref=search-standalone&type=standout"
        response = requests.get(seek_url)
        page_content = BeautifulSoup(response.content, 'html.parser')
        job_desc = page_content.find('div', attrs={'data-automation': 'jobAdDetails'})
        if job_desc:
            job_desc = job_desc.text
            job_desc_df = pd.DataFrame({'jobid': [job_id], 'jobDescription': [job_desc]})
            all_job_desc = pd.concat([all_job_desc, job_desc_df], ignore_index=True)
        else:
            print(f"Job {job_id} not found")
            pass

    if all_job_desc.shape[0] > 0:
        all_job_desc.to_sql('jobs_descriptions', sql_engine, if_exists='append', index=False)
        print(f"Added {all_job_desc.shape[0]} job descriptions to the database")
        logging.info(f"Added {all_job_desc.shape[0]} job descriptions to the database")
    
    return all_job_desc


if __name__=="__main__":
    catch_up_descriptions(sql_engine)