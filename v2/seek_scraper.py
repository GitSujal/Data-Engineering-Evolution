import pandas as pd
import requests
from bs4 import BeautifulSoup
from time import sleep
import re
from datetime import datetime
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


# Class to scrape data from seek.com.au
class SeekScraper:
    
    seek_base_url = 'https://www.seek.com.au'
    
    column_to_seek_attribute_mapping = {
        'search results': 'searchResults',
        'jobs lists': 'normalJob',
        'job title': 'jobTitle',
        'company': 'jobCompany',
        'location': 'jobLocation',
        'job classification': 'jobClassification',
        'work type': 'job-detail-work-type',
        'job description': 'jobAdDetails'
    }
    # Constructor
    def __init__(self, url:str=None, 
                    job:str = None, 
                    location:str = None, 
                    pages_to_scrape:int = 1,
                    skip_pages:int = 1,
                    debug:bool = False,
                    sleep_time:float = 0.2,
                    columns:list = ['Search Term', 'Job ID', 'Job Title', 'Company', 'Location', 
                                    'Job Classification', 'Job Description', 'Work Type', 'Scrape Date'],
                    data_folder: str = 'data'
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
        self.pages_to_scrape = pages_to_scrape
        self.skip_pages = skip_pages
        self.debug = debug
        self.sleep_time = sleep_time
        self.columns = columns
        self.data = pd.DataFrame(columns= self.columns)
        self.data_folder = data_folder
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
        self.local_file = f'{self.data_folder}/seek_scraper_raw_data.csv'
        self.index_file = f'{self.data_folder}/seek_scraper_all_index.csv'
        
    def make_request(self, url:str):
        if self.debug:
            print(f'Making request to {url}')
        try: 
            response = requests.get(url)
            if response.status_code != 200:
                raise ValueError(f'Error making request to {url}')
        except ValueError as e:
            print("{e} skipping and continuing")
            return None
        return response

    def default_if_none(self, value):
        if value is None:
            return 'Unknown'
        return value.text


    def parse_job_data(self, response, available_indices: list = []):
        result = BeautifulSoup(response.text, 'html.parser')
        # find attribute search result  

        search_results = result.find(attrs={'data-automation':self.column_to_seek_attribute_mapping["search results"]})
        # find attributes with job title within search result
        jobs = search_results.find_all(attrs={'data-automation':self.column_to_seek_attribute_mapping["jobs lists"]})
        
        if self.debug:
            print(f'Found {len(jobs)} jobs')
        if len(jobs) > 0:
            for job in jobs:
                temp_data = {}  
                if self.debug:
                    print('Parsing job data')
                temp_data = pd.DataFrame(columns= self.columns)
                temp_data["Search Term"] = [self.job.replace('-',' ').capitalize()]   
                
                # find the job title
                job_title = job.find(attrs={'data-automation':self.column_to_seek_attribute_mapping["job title"]})
                temp_data["Job Title"] = [self.default_if_none(job_title)]
                if self.debug:
                    print(f'Job Title: {self.default_if_none(job_title)}')
                
                # find the job link
                job_link = job_title['href']
                if self.debug:
                    print(f'Job Link: {self.seek_base_url}{job_link}')
                
                # job id is the part between /job/ and ? in the job link
                job_id = job_link.split('/job/')[1].split('?')[0]
                if job_id not in available_indices:
                    if self.debug:
                        print(f'Found new job with id {job_id}')
                    temp_data["Job ID"] = [job_id]
                    if self.debug:
                        print(f'Job ID: {job_id}')

                    # find the company name
                    company = job.find(attrs={'data-automation':self.column_to_seek_attribute_mapping["company"]})
                    temp_data["Company"] = [self.default_if_none(company)]
                    if self.debug:
                        print(f'Company: {self.default_if_none(company)}')

                    # find the location
                    location = job.find(attrs={'data-automation':self.column_to_seek_attribute_mapping["location"]})
                    temp_data["Location"] = [self.default_if_none(location)]
                    if self.debug:
                        print(f'Location: {self.default_if_none(location)}')

                    # job classification
                    job_classification = job.find(attrs={'data-automation':self.column_to_seek_attribute_mapping["job classification"]})
                    temp_data["Job Classification"] = [self.default_if_none(job_classification)]
                    if self.debug:
                        print(f'Job Classification: {self.default_if_none(job_classification)}')

                    if job_link is not None:
                        job_description_url = f'{self.seek_base_url}{job_link}'
                        job_description_response = self.make_request(job_description_url)
                        if job_description_response is not None:
                            job_description, work_type = self.parse_job_description(job_description_response)
                            temp_data['Job Description'] = [job_description]
                            temp_data['Work Type'] = [self.default_if_none(work_type)]
                        else:
                            temp_data['Job Description'] = ['Unknown']
                            temp_data["Work Type"] = ['Unknown']
                    else:
                        temp_data['Job Description'] = ['Unknown']
                        temp_data["Work Type"] = ['Unknown']

                    temp_data["Scrape Date"] = [datetime.now().strftime("%d/%m/%Y %H:%M:%S")]

                    if self.debug:
                        print(temp_data)
                    
                    if self.data.shape[0] == 0:
                        if self.debug:
                            print('Adding first row')
                        self.data = pd.DataFrame(temp_data)
                    else:
                        if self.debug:
                            print('Adding row')
                        # append the dfs using concat
                        temp_data_df = pd.DataFrame(temp_data)
                        self.data = pd.concat([self.data, temp_data_df], ignore_index=True)

                else:
                    if self.debug:
                        print(f'Job ID {job_id} already exists in data')
                if self.debug:
                    print('------------------------')
                    break
                        
    def parse_job_description(self, response):
        result = BeautifulSoup(response.text, 'html.parser')
        # find attribute search result  
        job_description = result.find(attrs={'data-automation':self.column_to_seek_attribute_mapping["job description"]})
        work_type = result.find(attrs={'data-automation':self.column_to_seek_attribute_mapping["work type"]})
        job_description = self.default_if_none(job_description)
        # remove all non-alphanumeric characters
        if job_description is not None:
            job_description = re.sub('\W+',' ', job_description).lower()
        return job_description, work_type
    
    def save_to_csv(self):
        if self.debug:
            print(f'Saving data to {self.local_file}')

        # if the csv exists append it based on job id
        if os.path.exists(self.local_file):
            # read the csv into a dataframe
            df = pd.read_csv(self.local_file)
            # find the new rows
            new_rows = self.data[~self.data['Job ID'].isin(df['Job ID'])]
            # append the new rows to the dataframe using concat
            df = pd.concat([df, new_rows], ignore_index=True)
            # save the dataframe to csv
            df.to_csv(self.local_file, index=False)
        else:
            # save the dataframe to csv
            self.data.to_csv(self.local_file, index=False)
        if self.debug:
            print('Data saved')
    
    def save_all_indexes(self):
        if self.debug:
            print(f'Saving all indexes to {self.index_file}')
        # open the index file as dataframe and append the new indexes
        if os.path.exists(self.index_file):
            index_df = pd.read_csv(self.index_file)
            # concat the new indexes
            indexes = pd.concat([index_df, self.data[['Job ID', 'Scrape Date']]], ignore_index=True)
            # remove duplicates
            indexes = indexes.drop_duplicates(subset=['Job ID'], keep='first')
            # save the indexes to csv
            indexes.to_csv(self.index_file, index=False)
        else:
            indexes = self.data[['Job ID', 'Scrape Date']]
            indexes.to_csv(self.index_file, index=False)
        if self.debug:
            print('All indexes saved')

    def get_available_indexes(self):
        if self.debug:
            print(f'Getting available indexes from {self.index_file}')
        if os.path.exists(self.index_file):
            index_df = pd.read_csv(self.index_file)
            return index_df['Job ID'].astype(str).tolist()
        else:
            return []

    def __call__(self, save_local = True):
        if self.debug:
            print(f'Calling SeekScraper with url: {self.url}')
        available_indexes = self.get_available_indexes()

        for page in range(self.skip_pages, self.pages_to_scrape+self.skip_pages):
            if self.debug:
                print(f'Scraping page {page}')
            if page == 1:
                response = self.make_request(self.url)
            else:
                response = self.make_request(f'{self.url}?page={page}')
            if response is not None:
                self.parse_job_data(response, available_indexes)
            sleep(self.sleep_time)
        if save_local:
            self.save_to_csv()
            self.save_all_indexes()
        return self.data