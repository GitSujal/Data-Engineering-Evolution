from seek_scraper import search_keyword, search_related_keywords
import pandas as pd
from pandas.errors import DatabaseError
from duckdb import CatalogException
import logging
import duckdb
import duckdb

# set up a log file
logging.basicConfig(filename='logs/scraper.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

logging.info("Starting the scraper....")

# Define the jobs to scrape
# jobs_to_scrape = [
#     "data analyst",
#     "data scientist",
#     "data engineer"
# ]

# Related titles
related_titles = {
    "Data Analyst": [
                "Data Analyst",
                "Business Intelligence Analyst",
                "Power BI Specialist",
                "Data Specialist",
                "Marketing Analyst",
                "Analytics Consultant",
                "Healthcare Data Analyst",
                "Insights Analyst",
                "Reporting Analyst",
                "Data Quality Analyst",
                "HR Data Analyst",
                "Test Analyst"
    ],
    "Data Engineer": [
                "Data Engineer",
                "Database Administrator",
                "Analytics Engineer",
                "Data Warehouse Developer",
                "Cloud Engineer",
                "Data Architect",
                "Database Developer",
                "Data Specialist",
                "ETL Developer",
                "Consultant",
                "Big Data Engineer",
                "Data Platform Engineer",
                "Data Integration Specialist",
                "DataOps Engineer",
                "ML Engineer",
    ],
    "Data Scientist": [
                "Data Scientist",
                "Machine Learning Engineer",
                "Statistical Modelling Expert",
                "Simulation Analyst",
                "Predictive Analytics Consultant",
                "Data Model Developer",
                "AI Engineer"
    ]
}

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
    for job in related_titles.keys():
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
    for key, value in related_titles.items():
        for job in value:
            for location in locations:
                logging.info(f"Getting the data for {job} in {location}")
                print(f"Getting the data for {job} in {location}")
                jobs_df = search_keyword(job, location)
                logging.info(f"Got {jobs_df.shape[0]} jobs for {job} in {location}")
                print(f"Got {jobs_df.shape[0]} jobs for {job} in {location}")
                print("Moving to the next location....")
                all_jobs_df = pd.concat([all_jobs_df, jobs_df], ignore_index=True)
                # de-duplicate the data based on keywords, location and searchDate
                all_jobs_df = all_jobs_df.drop_duplicates(subset=["jobId"], keep="last")
            print("Moving to the next job....")

    # for job in jobs_to_scrape:
    #     for location in locations:
    #         logging.info(f"Getting the data for {job} in {location}")
    #         print(f"Getting the data for {job} in {location}")
    #         jobs_df = search_keyword(job, location)
    #         logging.info(f"Got {jobs_df.shape[0]} jobs for {job} in {location}")
    #         print(f"Got {jobs_df.shape[0]} jobs for {job} in {location}")
    #         print("Moving to the next location....")
    #         all_jobs_df = pd.concat([all_jobs_df, jobs_df], ignore_index=True)
    #         # de-duplicate the data based on keywords, location and searchDate
    #         all_jobs_df = all_jobs_df.drop_duplicates(subset=["jobId"], keep="last")
    #     print("Moving to the next job....")
    return all_jobs_df

def write_df_to_duckdb(df, table_name, con, pk_columns):
    # get the existing data from the database
    try:
        existing_df = con.table(table_name).to_df()
    except (CatalogException, DatabaseError) as e:
        if 'does not exist' in str(e) or 'failed on sql' in str(e):
            existing_df = pd.DataFrame()
        else:
            raise e
    # append the new records to the existing data
    all_df = pd.concat([existing_df, df], ignore_index=True)
    # de-duplicate the data based on pk_columns
    all_df = all_df.drop_duplicates(subset=pk_columns, keep="last")
    # write the new records to the database
    con.register('all_df', all_df)
    con.execute(f"CREATE or REPLACE TABLE {table_name} AS SELECT * FROM all_df")

    # log the number of records written to the database
    logging.info(f"{all_df.shape[0]} records written to the database out of which {df.shape[0]} are new records")

if __name__=="__main__":

    # DB file path
    db_file = 'jobs.db'

    trends_table_name = "jobs_trends"
    trends_table_pk_columns = ["keywords", "location", "searchDate"]
    all_trends_df = get_trends_all(related_titles, locations)
    with duckdb.connect(db_file, read_only=False) as con:
        write_df_to_duckdb(all_trends_df, trends_table_name, con, trends_table_pk_columns)
        logging.info("Trends data written to the database")
        print("Trends data written to the database")

    jobs_table_name = "jobs"
    jobs_table_pk_columns = ["jobId"]
    all_jobs_df = get_jobs_all(related_titles, locations)
    with duckdb.connect(db_file, read_only=False) as con:
        write_df_to_duckdb(all_jobs_df, jobs_table_name, con, jobs_table_pk_columns)
        logging.info("Jobs data written to the database")
        print("Jobs data written to the database")