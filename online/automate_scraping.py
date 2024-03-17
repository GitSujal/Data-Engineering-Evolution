import time
from datetime import datetime
from scrape import scrape_data
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import sqlalchemy
import yaml
from data_preparation import process_data


config = yaml.safe_load(open('config.yml'))
# create a new db engine
server = config['db_credentials']['server']
database = config['db_credentials']['database']
user = config['db_credentials']['user']
password = os.getenv('DP101_PASSWORD')

sql_engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
                                  connect_args={'connect_timeout': 30})

# Define the jobs to scrape
jobs_to_scrape = [
    "data analyst",
    "data scientist",
    "data engineer",
    "business analyst",
    "business intelligence",
    "machine learning",
    "artificial intelligence",
    "reporting analyst",
    "BI analyst",
    "BI developer",
    "BI consultant",
    "BI engineer",
    "Data and Analytics",
    "Data and Analytics Consultant"
]

# Define the locations
locations  = [
    "Australia"
]

# Define a DF to store the results
results = pd.DataFrame(columns=["job", "location", "jobsCount", "scrapeDate", "pk_hash"])

# Find the total jobs avaialble for each job title

for job in jobs_to_scrape:
    for location in locations:
        target_url = f"https://www.seek.com.au/{job}-jobs/in-all-{location}"
        response = requests.get(target_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            jobs = soup.find("span", {"data-automation": "totalJobsCount"})
            jobs_count = int(jobs.text.replace(",", ""))
            new_row = {"job": job, "location": location, "jobsCount": jobs_count, "scrapeDate": datetime.now().date()}
            # create a hash of job, location and scrapeDate
            new_row_hash = hash(f"{job}{location}{datetime.now().date()}")
            new_row["pk_hash"] = new_row_hash

            new_df = pd.DataFrame([new_row])
            results = pd.concat([results, new_df], ignore_index=True)
        else:
            print(f"Failed to get the data for {job} in {location}")

def save_to_db(df, table_name, engine):
    existing_records = pd.read_sql(f"SELECT pk_hash FROM {table_name}", engine)
    # de-duplicate the data based on job, location and scrapeDate
    df = df.drop_duplicates(subset=["job", "location", "scrapeDate"])
    # remove the records that are already in the database
    df = df[~df["pk_hash"].isin(existing_records["pk_hash"])]
    # save the results to the database
    df.to_sql(table_name, engine, if_exists="append", index=False)

# save the results to the database
save_to_db(df=results, table_name="jobs_trends", engine=sql_engine)

# Scrape the data for each job
for index, row in results.iterrows():
    job = row["job"]
    location = row["location"]
    jobs_to_scrape = row["jobsCount"]
    batch_size = 200
    # calculate the number of batches
    num_batches = jobs_to_scrape // batch_size + 1
    start_page = 1
    print(f"Scraping {job} in {location} - {jobs_to_scrape} jobs")
    for i in range(num_batches):
        start = i * batch_size
        end = (i + 1) * batch_size
        start_page = i*10 + 1  
        print(f"Batch {i+1} - Start: {start} End: {end} Start Page: {start_page}")
        data = scrape_data(job=job, 
                    location=location, 
                    jobs_to_scrape=batch_size, 
                    save_local=False, 
                    save_sql=True, 
                    sql_engine=sql_engine, 
                    sleep_time=0.2, 
                    start_page=start_page)
        if len(data) == 0:
            print(f"No more data for {job} in {location}")
            break
        time.sleep(5)
    print(f"Finished scraping {job} in {location} total jobs: {jobs_to_scrape}")
    print("Starting the data preparation job")
    process_data(sql_engine)
    print("Waiting for 30 seconds before starting the next job")
    time.sleep(30)

print("All jobs are finished")
        