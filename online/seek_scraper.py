import pandas as pd
import requests
from bs4 import BeautifulSoup
from time import sleep
import re
from datetime import datetime
import os
import warnings
import json
import logging
warnings.simplefilter(action='ignore', category=FutureWarning)


# Class to scrape data from seek.com.au
class SeekScraper:
    
    seek_base_url = 'https://www.seek.com.au'
    
    job_attributes= [
        "country" ,
        "advertiserName" ,
        "jobId",
        "jobTitle" ,
        "jobStatus", 
        "jobListingType"  ,
        "jobPostedTime" ,
        "jobArea" , 
        "jobLocation" , 
        "jobClassification" ,
        "jobSubClassification" ,
        "jobWorkType",
        "jobSalary" 
    ]
    
    search_attributes = [
        "searchKeywords",
        "searchLocation",
        "searchDate" 
    ]
    # Constructor
    def __init__(self, url:str=None, 
                    job:str = None, 
                    location:str = None, 
                    jobs_to_scrape: int = 100,
                    debug:bool = False,
                    sleep_time:float = 0.1,
                    data_folder: str = 'data',
                    sql_engine = None,
                    start_page: int = 1
                    ):
        if url is None and (job is None or location is None):
            logging.error('Either url or job and location must be provided')
            raise ValueError('Either url or job and location must be provided')
        if url is not None:
            if self.seek_base_url not in url:
                self.url = f'{self.seek_base_url}{url}'
            else:
                self.url = url
        else:
            self.job = job
            self.location = location
            self.url = f'{self.seek_base_url}/{job}-jobs/in-{location}'
        self.jobs_to_scrape = jobs_to_scrape
        self.scrape_count = 0
        self.debug = debug
        self.sleep_time = sleep_time
        self.columns = self.search_attributes +  self.job_attributes + ['jobDescription']
        self.data = pd.DataFrame(columns= self.columns)
        self.data_folder = data_folder
        self.start_page = start_page
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
        self.local_file = f'{self.data_folder}/seek_scraper_raw_data.csv'
        self.index_file = f'{self.data_folder}/seek_scraper_all_index.csv'

        self.sql_engine = sql_engine
        
    def make_request(self, url:str):
        if self.debug:
            print(f'Making request to {url}')
        try: 
            response = requests.get(url)
            if response.status_code != 200:
                raise ValueError(f'Error making request to {url}')
        except ValueError as e:
            print(f"{e} skipping and continuing")
            return None
        return response

    def default_if_none(self, value):
        if value is None:
            return 'Unknown'
        return value.text

    def metadata_parser(self, metadata):
        content = re.findall(r'=\ (.*);', metadata)
        # replace undefined with null
        content = content[0].replace('undefined','null')
        content = json.loads(content)
        return content

    def parse_search_result(self, response, available_indices: list = [], job_ids_to_scrape: list = []):
        result = BeautifulSoup(response.text, 'html.parser')
        metadata = result.find_all("script", attrs={'data-automation': 'server-state'})
        for data in metadata:
            lines = data.text.splitlines()
            for line in lines:
                if "SK_DL" in line:
                    sk_dl = self.metadata_parser(line)
                    # if self.debug:
                    #     print(sk_dl)
                if "SEEK_REDUX_DATA" in line:
                    seek_redux_data = self.metadata_parser(line)
                    # if self.debug:
                    #     print(seek_redux_data)
        self.total_jobs_found = int(sk_dl['jobsCount'])
        if self.total_jobs_found < self.jobs_to_scrape:
            logging.info(f'Only {self.total_jobs_found} jobs found, reducing jobs to scrape to {self.total_jobs_found}')
            print(f'Only {self.total_jobs_found} jobs found, reducing jobs to scrape to {self.total_jobs_found}')
            self.jobs_to_scrape = self.total_jobs_found
        for jobid in seek_redux_data['results']['jobIds']:
            jobid = int(jobid)
            if jobid not in available_indices:
                # found new job 
                job_ids_to_scrape.append(jobid)
            else:            
                if self.debug:
                    if self.debug:
                        logging.info(f'Job {jobid} already scraped')
                    print(f'Job {jobid} already scraped')
            self.scrape_count += 1

        return job_ids_to_scrape
                        
    def parse_job_description(self, response):
        result = BeautifulSoup(response.text, 'html.parser')
        # find attribute search result  
        job_description = result.find(attrs={'data-automation':'jobAdDetails'})
        job_type = result.find(attrs={'data-automation':'job-detail-work-type'})
        job_description = self.default_if_none(job_description)
        job_type = self.default_if_none(job_type)
        # remove all non-alphanumeric characters
        if job_description is not None:
            job_description = re.sub('\W+',' ', job_description).lower()
        return job_description, job_type
    
    def parse_job_attributes(self, response):
        result = BeautifulSoup(response.text, 'html.parser')
        metadata = result.find_all("script", attrs={'data-automation': 'server-state'})
        for data in metadata:
            lines = data.text.splitlines()
            for line in lines:
                if "SK_DL" in line:
                    sk_dl = self.metadata_parser(line)
                    if self.debug:
                        print(sk_dl)
        return sk_dl

    def save_to_csv(self):
        if self.debug:
            logging.info(f'Saving data to {self.local_file}')
            print(f'Saving data to {self.local_file}')

        # if the csv exists append it based on job id
        if os.path.exists(self.local_file):
            # read the csv into a dataframe
            df = pd.read_csv(self.local_file)
            # find the new rows
            new_rows = self.data[~self.data['jobId'].isin(df['jobId'])]
            # append the new rows to the dataframe using concat
            df = pd.concat([df, new_rows], ignore_index=True)
            # save the dataframe to csv
            df.to_csv(self.local_file, index=False)
        else:
            # save the dataframe to csv
            self.data.to_csv(self.local_file, index=False)
        if self.debug:
            logging.info('Data saved to csv %s', self.local_file)
            print('Data saved')
    
    def save_to_sql(self):
        # de-duping the data based on jobId
        self.data = self.data.drop_duplicates(subset=['jobId'], keep='first')
        if self.debug:
            logging.info(f'Saving data to sql')
            print(f'Saving data to sql')
        # Read existing data from sql
        if self.sql_engine is not None:
            existing_df = pd.read_sql("SELECT jobId FROM dbo.jobs", self.sql_engine)
            # find the new rows
            new_rows = self.data[~self.data['jobId'].isin(existing_df['jobId'])]
            # append the new rows to the dataframe using concat
            # required columns are all columns except jobDescription
            req_columns = [col for col in new_rows.columns if col != 'jobDescription']
            # save the new rows to sql
            new_rows[req_columns].to_sql('jobs', self.sql_engine, if_exists='append', index=False)
            if self.debug:
                logging.info('Data saved to sql')
                print('Data saved to sql')
            # load the job description to a separate table
            job_description_df = new_rows[['jobId', 'jobDescription']]
            # save the job description to sql
            job_description_df.to_sql('jobs_descriptions', self.sql_engine, if_exists='append', index=False)

    def save_all_indexes_sql(self, new_indexes):
        if self.debug:
            logging.info(f'Saving all indexes to sql')
            print(f'Saving all indexes to sql')
        # open the index file as dataframe and append the new indexes
        if self.sql_engine is not None:
            existing_df = pd.read_sql("SELECT jobId FROM dbo.jobs_indexes", self.sql_engine)
            # concat the new indexes
            new_df = pd.DataFrame.from_dict({'jobId': new_indexes,
                                             "scrapeDate": datetime.now(),
                                             })
            # de-duping the data based on jobId
            new_df = new_df.drop_duplicates(subset=['jobId'], keep='first')
            # new rows
            new_rows = new_df[~new_df['jobId'].isin(existing_df['jobId'])]
            # write the new rows to sql
            new_rows.to_sql('jobs_indexes', self.sql_engine, if_exists='append', index=False)

            if self.debug:
                logging.info('All indexes saved to sql')
                print('All indexes saved to sql')
            
        if self.debug:
            logging.info('All indexes saved to sql')
            print('All indexes saved to sql')
    
    def save_all_indexes(self, new_indexes):
        if self.debug:
            logging.info(f'Saving all indexes to {self.index_file}')
            print(f'Saving all indexes to {self.index_file}')
        # open the index file as dataframe and append the new indexes
        if os.path.exists(self.index_file):
            index_df = pd.read_csv(self.index_file)
            # concat the new indexes
            new_df = pd.DataFrame.from_dict({'jobId': new_indexes,
                                             "scrapeDate": datetime.now(),
                                             })

            # append the new rows to the dataframe using concat
            indexes = pd.concat([index_df, new_df], ignore_index=True)
            # remove duplicates
            indexes = indexes.drop_duplicates(subset=['jobId'], keep='first')
            # save the indexes to csv
            indexes.to_csv(self.index_file, index=False)
        else:
            indexes = pd.DataFrame.from_dict({'jobId': new_indexes,
                                             "scrapeDate": datetime.now(),
                                             })
            indexes.to_csv(self.index_file, index=False)
        if self.debug:
            logging.info('All indexes saved to %s', self.index_file)
            print('All indexes saved')

    def get_available_indexes(self):

        if self.sql_engine is None:
            if self.debug:
                logging.info(f'Getting available indexes from {self.index_file}')
                print(f'Getting available indexes from {self.index_file}')
            if os.path.exists(self.index_file):
                index_df = pd.read_csv(self.index_file)
                return index_df['jobId'].astype(int).tolist()
            else:
                return []
        else:
            try:
                if self.debug:
                    logging.info(f'Getting available indexes from sql')
                    print(f'Getting available indexes from sql')
                index_df_sql = "SELECT jobId FROM dbo.jobs_indexes"
                index_df = pd.read_sql(index_df_sql, self.sql_engine)
                return index_df['jobId'].astype(int).tolist()
            except Exception as e:
                logging.info(f'Error getting indexes from sql {e}')
                print(f'Error getting indexes from sql {e}')
                return []
    
    def set_batch_size(self, jobs_to_scrape):
        if jobs_to_scrape > 50:
            self.batch_size = 50
        else:
            self.batch_size = jobs_to_scrape
        if self.debug:
            logging.info(f'Batch size set to {self.batch_size}')
            print(f'Batch size set to {self.batch_size}')

    def __call__(self, save_local:bool = True, save_sql:bool = False):
        if self.debug:
            logging.info(f'Calling SeekScraper with url: {self.url}')
            print(f'Calling SeekScraper with url: {self.url}')
        available_indexes = self.get_available_indexes()
        job_ids_to_scrape = []
        page_numnber = self.start_page

        while self.scrape_count < self.jobs_to_scrape:
            response = self.make_request(f'{self.url}?page={page_numnber}')
            if response is not None:
                job_ids_to_scrape =  self.parse_search_result(response, available_indexes, job_ids_to_scrape=job_ids_to_scrape)
            sleep(self.sleep_time)
            page_numnber += 1

        # cut off any excess if any 
        job_ids_to_scrape = job_ids_to_scrape[:self.jobs_to_scrape]
        scraping_required_count = len(job_ids_to_scrape)

        if scraping_required_count>0:
            if self.debug:
                logging.info(f'{scraping_required_count} jobs to scrape')
                print(f'{scraping_required_count} jobs to scrape')
            # do batching based on jobs_to_scrape size
            self.set_batch_size(scraping_required_count)
            jobs_scraped = []
            # scrape the jobs
            for i in range(0, scraping_required_count, self.batch_size):
                batch = job_ids_to_scrape[i:i + self.batch_size]
                batch_scrape_count = 0
                # empty the df before each batch to keep the memory usage low
                self.data = pd.DataFrame(columns=self.columns)
                for job in batch:
                    if job not in jobs_scraped:
                        response = self.make_request(f'{self.seek_base_url}/job/{str(job)}')
                        if response is not None:
                            job_description, job_type = self.parse_job_description(response)
                            job_metadata = self.parse_job_attributes(response)
                            # only keeps the keys that are in the job_attributes list
                            job_metadata = {k: [v] for k, v in job_metadata.items() if k in self.job_attributes}
                            # if key not in job_metadata add it with value "Unknown"
                            for key in self.job_attributes:
                                if key not in job_metadata:
                                    job_metadata[key] = ["Unknown"]
                            job_metadata['jobDescription'] = [job_description]
                            job_metadata["searchKeywords"] = [self.job]
                            job_metadata["searchLocation"] = [self.location]
                            job_metadata["searchDate"] = [datetime.now()]
                            job_metadata["jobWorkType"] = [job_type]
                            job_metadata_df = pd.DataFrame.from_dict(job_metadata)
                            # if self.debug:
                            #     print(job_metadata_df)
                            self.data = pd.concat([self.data, job_metadata_df], ignore_index=True)
                            batch_scrape_count += 1
                            jobs_scraped.append(job)
                sleep(self.sleep_time)
                if self.debug:
                    print(self.data)
                if save_local:
                    self.save_to_csv()
                    self.save_all_indexes(batch)
                if self.sql_engine is not None and save_sql:
                   self.save_to_sql()
                   self.save_all_indexes_sql(batch)
                print(f'Scraped {batch_scrape_count} jobs batch number {i//self.batch_size + 1}')
                logging.info(f'Scraped {batch_scrape_count} jobs batch number {i//self.batch_size + 1}')
            
        return self.data