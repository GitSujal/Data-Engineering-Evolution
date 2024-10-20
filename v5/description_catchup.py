# import os
# import sqlalchemy
import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
import argparse
from data_preparation import process_data
import duckdb
from scraper import write_df_to_duckdb

# set up a log file
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

logging.info("Starting the description catchup....")


def find_jobids_to_scrape(con, batch_size=200)-> list:
    # catchup with job description
    sql = """select jobId from jobs where jobid not in (select jobid from jobs_descriptions) order by jobid desc;"""

    job_ids = con.execute(sql).fetchdf()
    logging.info(f"Found {job_ids.shape[0]} jobs without description")

    if job_ids.shape[0] > batch_size:
        job_ids = job_ids.sample(batch_size)['jobId'].to_list()
    else:
        job_ids = job_ids['jobId'].to_list()
    
    return job_ids


def scrape_descriptions(job_ids: list)->pd.DataFrame:
    
    all_job_desc = pd.DataFrame()
    for job_id in job_ids:
        # print(f"Getting job {job_id} description")
        seek_url = f"https://www.seek.com.au/job/{job_id}?ref=search-standalone&type=standout"
        response = requests.get(seek_url)
        page_content = BeautifulSoup(response.content, 'html.parser')
        job_desc = page_content.find('div', attrs={'data-automation': 'jobAdDetails'})
        if job_desc:
            job_desc = job_desc.text
            # convert the job_desc to proper text format use utf-8 encoding
            job_desc = job_desc.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
            job_desc_df = pd.DataFrame({'jobId': [job_id], 'jobDescription': [job_desc]})
            all_job_desc = pd.concat([all_job_desc, job_desc_df], ignore_index=True)
        else:
            print(f"Job {job_id} not found")
            pass
    return all_job_desc

if __name__=="__main__":

    parser = argparse.ArgumentParser(description="Catch up with job descriptions")
    parser.add_argument("--catchup", help="Catch up with job descriptions", action="store_true")
    parser.add_argument("--process", help="Process the data", action="store_true")
    parser.add_argument("-b", "--batch", help="The batch size for the job descriptions", type=int, default=200)
    args = parser.parse_args()

    # DB file path
    db_file = 'jobs.db'

    # job descriptions table name
    jobs_descriptions_table_name = "jobs_descriptions"
    jobs_descriptions_table_pk_columns = ["jobId"]
    with duckdb.connect(db_file, read_only=True) as con:
        # catch up with job descriptions
        job_ids = find_jobids_to_scrape(con, args.batch)
    if len(job_ids) > 0:
        job_descriptions = scrape_descriptions(job_ids)
        if job_descriptions.shape[0] > 0:
            logging.info(f"Need to write {job_descriptions.shape[0]} job descriptions to the database")
            with duckdb.connect(db_file, read_only=False) as con:
                # write the job descriptions to the database
                write_df_to_duckdb(job_descriptions, jobs_descriptions_table_name, con, jobs_descriptions_table_pk_columns)
                logging.info(f"Added {job_descriptions.shape[0]} job descriptions to the database")
            # process the data
            if args.process:
                with duckdb.connect(db_file, read_only=False) as con:
                    logging.info("Processing the data")
                    process_data(con)
        else:
            logging.info("No new job descriptions found")