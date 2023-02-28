import pandas as pd
import requests
from bs4 import BeautifulSoup
from time import sleep
import re
import json


# Class to scrape data from seek.com.au
class SeekScraper:
    seek_base_url = 'https://www.seek.com.au'
    
    default_qualifications = {
    "degree": "Degree",
    "bachelor": "Bachelor",
    "master": "Master",
    "doctor": "Doctor",
    "undergrad": "Undergrad",
    "phd": "PhD",
    "postgrad": "Postgrad",
    "postgraduate": "Postgraduate",
    "diploma": "Diploma",
    "cert": "Cert",
    "certificate": "Certificate",
    "certification": "Certification",
    "graduate": "Graduate",
    "undergraduate": "Undergraduate"
    }

    default_programming_languages = {"sql" : "SQL", "python" : "Python", "r" : "R", "c":"C",
                            "c#":"C#","javascript" : "JavaScript","js":"JS","java":"Java",
                            "scala":"Scala","sas" : "SAS", "matlab": "MATLAB","c++" : "C++", 
                            "c/c++" : "C / C++","perl" : "Perl","go" : "Go","typescript" : "TypeScript", 
                            "bash":"Bash", "html" : "HTML","css" : "CSS", "php" : "PHP", "powershell" : "Powershell",
                            "rust" : "Rust", "kotlin" : "Kotlin", "ruby" : "Ruby", "dart" : "Dart","assembly" :"Assembly",
                              "swift" : "Swift", "vba" : "VBA", "lua" : "Lua", "groovy" : "Groovy", "delphi" : "Delphi", 
                            "objective-c" : "Objective-C", "haskell" : "Haskell", "elixir" : "Elixir", "julia" : "Julia", 
                            "clojure": "Clojure", "solidity" : "Solidity", "lisp" : "Lisp", "f#":"F#", "fortran" : "Fortran",
                            "erlang" : "Erlang", "apl" : "APL", "cobol" : "COBOL", "ocaml": "OCaml", "crystal":"Crystal",
                            "javascript/typescript" : "JavaScript / TypeScript", "golang":"Golang","nosql": "NoSQL","mongodb" : "MongoDB",
                            "t-sql" :"Transact-SQL","no-sql" : "No-SQL", "visual_basic" : "Visual Basic",
                            "pascal":"Pascal","mongo" : "Mongo", "pl/sql" : "PL/SQL", "sass" :"Sass", "vb.net" : "VB.NET", "mssql" : "MSSQL"
                            }

    default_skill_keywords = {"airflow": "Airflow", "alteryx": "Alteryx", "asp.net": "ASP.NET", 
                      "atlassian": "Atlassian", "excel": "Excel", "power_bi": "Power BI", 
                      "tableau": "Tableau", "srss": "SRSS", "word": "Word", "unix": "Unix", 
                      "vue": "Vue", "jquery": "jQuery", "linux/unix": "Linux / Unix", "seaborn": "Seaborn", 
                      "microstrategy": "MicroStrategy", "spss": "SPSS", "visio": "Visio", "gdpr": "GDPR", 
                      "ssrs": "SSRS", "spreadsheet": "Spreadsheet", "aws": "AWS", "hadoop": "Hadoop", "ssis": "SSIS", 
                      "linux": "Linux", "sap": "SAP", "powerpoint": "PowerPoint", "sharepoint": "SharePoint", "redshift": "Redshift", 
                      "snowflake": "Snowflake", "qlik": "Qlik", "cognos": "Cognos", "pandas": "Pandas", "spark": "Spark", 
                      "outlook": "Outlook", "oracle": "Oracle", "architect": "Architect", "architecture": "Architecture", 
                      "catalogue": "Catalogue", "catalog": "Catalog", "lineage": "Lineage"
                    }


    # Constructor
    def __init__(self, url:str=None, 
                    job:str = None, 
                    location:str = None, 
                    pages_to_scrape:int = 1,
                    debug:bool = False,
                    sleep_time:float = 0.2,
                    skill_keywords:list = None,
                    programming_languages:list = None,
                    qualifications:list = None,
                    incldue_raw_description:bool = False):
        if url is None and (job is None or location is None):
            raise ValueError('Either url or job and location must be provided')
        if url is not None:
            if self.seek_base_url not in url:
                self.url = f'{self.seek_base_url}{url}'
            else:
                self.url = url
        else:
            self.url = f'{self.seek_base_url}/{job}-jobs/in-{location}'
        self.pages_to_scrape = pages_to_scrape
        self.debug = debug
        self.include_raw_description = incldue_raw_description
        self.sleep_time = sleep_time
        if self.include_raw_description:
            self.data = pd.DataFrame(columns=['Job Title', 'Company', 'Location', 
                                              'Job Classification', 'Job Link', 'Skills', 
                                              'Qualifications', 'Programming Languages', 'Job Description'])
        else:
            self.data = pd.DataFrame(columns=['Job Title', 'Company', 'Location', 
                                              'Job Classification', 'Job Link', 'Skills', 
                                              'Qualifications', 'Programming Languages'])
        if skill_keywords:
            self.skill_keywords = skill_keywords
        else:
            self.skill_keywords = list(self.default_skill_keywords.keys())
        if programming_languages:
            self.programming_languages = programming_languages
        else:
            self.programming_languages = list(self.default_programming_languages.keys())
        if qualifications:
            self.qualifications = qualifications
        else:
            self.qualifications = list(self.default_qualifications.keys())
    
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

    def parse_job_data(self, response):
        result = BeautifulSoup(response.text, 'html.parser')
        # find attribute search result  
        search_results = result.find(attrs={'data-automation':'searchResults'})
        # find attributes with job title within search result
        jobs = search_results.find_all(attrs={'data-automation':'normalJob'})
        if self.debug:
            print(f'Found {len(jobs)} jobs')
        if len(jobs) > 0:
            for job in jobs:
                # find the job title
                job_title = job.find(attrs={'data-automation':'jobTitle'})
                # find the job link
                job_link = job_title['href']
                job_title = self.default_if_none(job_title)
                # find the company name
                company = job.find(attrs={'data-automation':'jobCompany'})
                company = self.default_if_none(company)
                # find the location
                location = job.find(attrs={'data-automation':'jobLocation'})
                location = self.default_if_none(location)
                # job classification
                job_classification = job.find(attrs={'data-automation':'jobClassification'})
                job_classification = self.default_if_none(job_classification)
                if job_link is not None:
                    job_description_url = f'{self.seek_base_url}{job_link}'
                    job_description_response = self.make_request(job_description_url)
                    if job_description_response is not None:
                        job_description = self.parse_job_description(job_description_response)
                        parsed_jd = self.process_description(job_description)
                    else:
                        job_description = 'Unknown'
                        parsed_jd = {"skills":'Unknown', "qualifications":'Unknown', "programming_languages":'Unknown'}
                else:
                    job_description = 'Unknown'
                    parsed_jd = {"skills":'Unknown', "qualifications":'Unknown', "programming_languages":'Unknown'}
                if self.debug:
                    print(f'Job title: {job_title}')
                    print(f'Company: {company}')
                    print(f'Location: {location}')
                    print(f'Job classification: {job_classification}')
                    print(f'Job link: {job_link}')
                    print(f'Job description: {job_description}')
                    print(f'Parsed job description: {parsed_jd}')
                
                if self.data.shape[0] == 0:
                    if self.debug:
                        print('Adding first row')
                    if self.include_raw_description:
                        self.data.loc[0] = [job_title, company, location, 
                                        job_classification, job_link,
                                        parsed_jd['skills'], parsed_jd['qualifications'],
                                        parsed_jd['programming_languages'], job_description]
                    else:
                        self.data.loc[0] = [job_title, company, location, 
                                        job_classification, job_link,
                                        parsed_jd['skills'], parsed_jd['qualifications'],
                                        parsed_jd['programming_languages']]
                else:
                    if self.debug:
                        print('Adding row')
                    if self.include_raw_description:
                        self.data.loc[self.data.shape[0]] = [job_title, company, location, 
                                        job_classification, job_link,
                                        parsed_jd['skills'], parsed_jd['qualifications'],
                                        parsed_jd['programming_languages'], job_description]
                    else:
                        self.data.loc[self.data.shape[0]] = [job_title, company, location, 
                                        job_classification, job_link,
                                        parsed_jd['skills'], parsed_jd['qualifications'],
                                        parsed_jd['programming_languages']]
        
    def parse_job_description(self, response):
        result = BeautifulSoup(response.text, 'html.parser')
        # find attribute search result  
        job_description = result.find(attrs={'data-automation':'jobAdDetails'})
        job_description = self.default_if_none(job_description)
        # remove all non-alphanumeric characters
        if job_description is not None:
            job_description = re.sub('\W+',' ', job_description).lower()
        return job_description

    def process_description(self, description):
        if self.debug:
            print(f'Processing description: {description}')
        jd_skills = []
        jd_qualifications = []
        jd_programming_languages = []
        for word in description.split():
            if word in self.programming_languages:
                    jd_programming_languages.append(word)
            if word in self.qualifications:
                    jd_qualifications.append(word)
            if word in self.skill_keywords:
                    jd_skills.append(word)
        if len(jd_skills) > 0:
            jd_skills = ' '.join(set(jd_skills))
        else:
            jd_skills = ''
        if len(jd_qualifications) > 0:
            jd_qualifications = ' '.join(set(jd_qualifications))
        else:
            jd_qualifications = ''
        if len(jd_programming_languages) > 0:
            jd_programming_languages = ' '.join(set(jd_programming_languages))
        else:
            jd_programming_languages = ''
        
        return_dict = {"skills":jd_skills, "qualifications":jd_qualifications, "programming_languages":jd_programming_languages}
        return return_dict
    
    def __call__(self):
        if self.debug:
            print(f'Calling SeekScraper with url: {self.url}')
        for page in range(1, self.pages_to_scrape+1):
            if self.debug:
                print(f'Scraping page {page}')
            if page == 1:
                response = self.make_request(self.url)
            else:
                response = self.make_request(f'{self.url}?page={page}')
            if response is not None:
                self.parse_job_data(response)
            sleep(self.sleep_time)
        return self.data