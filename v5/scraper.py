from seek_scraper import search_keyword, search_related_keywords
import pandas as pd
import os
import sqlalchemy
from data_preparation import process_data


# Define the jobs to scrape
jobs_to_scrape = [
    "data analyst",
    "data scientist",
    "data engineer"
]

# Define the locations
locations  = [
    "Sydney",
    "Melbourne",
    "Brisbane",
    "Perth",
    "Adelaide",
    "Canberra",
    "Australia"
]

def get_trends_all(jobs_to_scrape, locations):

    # Define a DF to store the results
    all_trends_df = pd.DataFrame()

    # Find the total jobs avaialble for each job title
    for job in jobs_to_scrape:
        for location in locations:
            trends_df = search_related_keywords(job, location)
            all_trends_df = pd.concat([all_trends_df, trends_df], ignore_index=True)
            # de-duplicate the data based on keywords, location and searchDate
            all_trends_df = all_trends_df.drop_duplicates(subset=["keywords", "location", "searchDate"], keep="last")

    return all_trends_df

def get_jobs_all(jobs_to_scrape, locations):

    # Define a DF to store the results
    all_jobs_df = pd.DataFrame()

    # Find the total jobs avaialble for each job title
    for job in jobs_to_scrape:
        for location in locations:
            print(f"Getting the data for {job} in {location}")
            jobs_df = search_keyword(job, location)
            print(f"Got {jobs_df.shape[0]} jobs for {job} in {location}")
            print("Moving to the next location....")
            all_jobs_df = pd.concat([all_jobs_df, jobs_df], ignore_index=True)
            # de-duplicate the data based on keywords, location and searchDate
            all_jobs_df = all_jobs_df.drop_duplicates(subset=["jobId"], keep="last")
        print("Moving to the next job....")
    return all_jobs_df

def write_trends_to_db(df, table_name, sql_engine):
    # Write the data to the database
    df.to_sql(table_name, sql_engine, if_exists='append', index=False)

def write_jobs_to_db(df, table_name, sql_engine, primary_key='jobId'):
    # get the existing data from the database
    existing_df = pd.read_sql(f'SELECT {primary_key} FROM {table_name}', sql_engine)
    # find the new records
    new_records = df[~df[primary_key].isin(existing_df[primary_key])]
    # write the new records to the database
    new_records.to_sql(table_name, sql_engine, if_exists='append', index=False)


if __name__=="__main__":
    # create a new db engine
    server = 'dp101server.database.windows.net'
    database = 'dp101database'
    user = 'dp101admin'
    password = os.getenv('DP101_PASSWORD')

    sql_engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
                                    connect_args={'connect_timeout': 30})

    all_trends_df = get_trends_all(jobs_to_scrape, locations)
    if all_trends_df.shape[0] > 0:
        write_trends_to_db(all_trends_df, "jobs_trends", sql_engine)
        print("Trends data written to the database")
    else:
        print("No trends data to write to the database")
    
    all_jobs_df = get_jobs_all(jobs_to_scrape, locations)
    if all_jobs_df.shape[0] > 0:
        write_jobs_to_db(all_jobs_df, "jobs", sql_engine)
        print("Jobs data written to the database")
    else:
        print("No jobs data to write to the database")
    print("Processing the data....")
    process_data(sql_engine)
    print("Data processing complete")