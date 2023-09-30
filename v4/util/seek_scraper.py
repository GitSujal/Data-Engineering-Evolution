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

# set up logging
logging.basicConfig(filename='seek_scraper.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
                    log_level = logging.INFO, 
                    sleep_time:float = 0.2,
                    data_folder: str = 'data',
                    index_folder: str = 'index'
                    ):
        if url is None and (job is None or location is None):
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
        self.debug = log_level == logging.DEBUG
        if self.debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(log_level)
        self.sleep_time = sleep_time
        self.columns = self.search_attributes +  self.job_attributes + ['jobDescription']
        self.data = pd.DataFrame(columns= self.columns)
        self.data_folder = data_folder
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
        self.local_file = f'{self.data_folder}/seek_scraper_raw_data.csv'
        self.index_file = f'{self.data_folder}/seek_scraper_all_index.csv'
        
    def make_request(self, url:str):
        logger.info(f'Making request to {url}')
        try: 
            response = requests.get(url)
            if response.status_code != 200:
                logger.error(f'Error making request to {url}')
                raise ValueError(f'Error making request to {url}')
        except ValueError as e:
            logger.error(f"Error message: {e}")
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
                    logger.debug(str(sk_dl))
                if "SEEK_REDUX_DATA" in line:
                    seek_redux_data = self.metadata_parser(line)
                    logger.debug(str(seek_redux_data))
        self.total_jobs_found = int(sk_dl['searchTotalCount'])
        if self.total_jobs_found < self.jobs_to_scrape:
            logger.info(f'Only {self.total_jobs_found} jobs found, reducing jobs to scrape to {self.total_jobs_found}')
            self.jobs_to_scrape = self.total_jobs_found
        for job in seek_redux_data['results']['jobIds']:
            if job not in available_indices:
                # found new job 
                job_ids_to_scrape.append(job)
            else:            
                logger.info(f'Job {job} already scraped')
            self.scrape_count += 1
        return job_ids_to_scrape
                        
    def parse_job_description(self, response):
        result = BeautifulSoup(response.text, 'html.parser')
        # find attribute search result  
        job_description = result.find(attrs={'data-automation':'jobAdDetails'})
        job_description = self.default_if_none(job_description)
        # remove all non-alphanumeric characters
        if job_description is not None:
            job_description = re.sub('\W+',' ', job_description).lower()
        return job_description
    
    def parse_job_attributes(self, response):
        result = BeautifulSoup(response.text, 'html.parser')
        metadata = result.find_all("script", attrs={'data-automation': 'server-state'})
        for data in metadata:
            lines = data.text.splitlines()
            for line in lines:
                if "SK_DL" in line:
                    sk_dl = self.metadata_parser(line)
                    logger.debug(str(sk_dl))
        return sk_dl

    def save_to_csv(self):
        logger.info(f'Saving data to {self.local_file}')

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
        logger.info(f'Saved data to {self.local_file}')
    
    def save_all_indexes(self, new_indexes):
        logger.info(f'Saving all indexes to {self.index_file}')
        # open the index file as dataframe and append the new indexes
        if os.path.exists(self.index_file):
            index_df = pd.read_csv(self.index_file)
            # concat the new indexes
            new_df = pd.DataFrame.from_dict({'jobID': new_indexes})

            indexes = pd.concat([index_df, new_df], ignore_index=True)
            # remove duplicates
            indexes = indexes.drop_duplicates(subset=['jobID'], keep='first')
            # save the indexes to csv
            indexes.to_csv(self.index_file, index=False)
        else:
            indexes = pd.DataFrame.from_dict({'jobID': new_indexes})
            indexes.to_csv(self.index_file, index=False)
        logger.info(f'Saved all indexes to {self.index_file}')

    def get_available_indexes(self):
        logger.info(f'Getting available indexes from {self.index_file}')
        if os.path.exists(self.index_file):
            index_df = pd.read_csv(self.index_file)
            return index_df['jobID'].astype(str).tolist()
        else:
            return []

    def set_batch_size(self, jobs_to_scrape):
        if jobs_to_scrape > 100:
            self.batch_size = 100
        else:
            self.batch_size = jobs_to_scrape

    def __call__(self, save_local = True):
        logger.info(f'Calling SeekScraper with url: {self.url}')
        available_indexes = self.get_available_indexes()
        job_ids_to_scrape = []
        page_numnber = 1

        while self.scrape_count < self.jobs_to_scrape:
            response = self.make_request(f'{self.url}?page={page_numnber}')
            if response is not None:
                job_ids_to_scrape =  self.parse_search_result(response, available_indexes, job_ids_to_scrape=job_ids_to_scrape)
            sleep(self.sleep_time)
            page_numnber += 1

        # cut off any excess if any 
        job_ids_to_scrape = job_ids_to_scrape[:self.jobs_to_scrape]
        scraping_reqired_count = len(job_ids_to_scrape)

        if scraping_reqired_count>0:
            logger.info(f'{scraping_reqired_count} jobs to scrape')
            # do batching based on jobs_to_scrape size
            self.set_batch_size(scraping_reqired_count)
            # scrape the jobs
            for i in range(0, scraping_reqired_count, self.batch_size):
                batch = job_ids_to_scrape[i:i + self.batch_size]
                batch_scrape_count = 0
                # empty the df before each batch to keep the memory usage low
                self.data = pd.DataFrame(columns=self.columns)
                for job in batch:
                    response = self.make_request(f'{self.seek_base_url}/job/{job}')
                    if response is not None:
                        job_description = self.parse_job_description(response)
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
                        job_metadata["searchDate"] = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                        job_metadata_df = pd.DataFrame.from_dict(job_metadata)
                        # if self.debug:
                        #     print(job_metadata_df)
                        self.data = pd.concat([self.data, job_metadata_df], ignore_index=True)
                        batch_scrape_count += 1
                sleep(self.sleep_time)
                if self.debug:
                    print(self.data)
                if save_local:
                    self.save_to_csv()
                    self.save_all_indexes(batch)
                logger.info(f'Scraped {batch_scrape_count} jobs batch number {i//self.batch_size + 1}')
        # log top 5 rows from self.data as info
        logger.info(f'Scraped {self.scrape_count} jobs')
        logger.info(f'Head of data: {self.data.head()}')
        return self.data