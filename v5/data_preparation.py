from description_processor import keywords_finder
import json
import pandas as pd
import os
import re
import duckdb
from scraper import write_df_to_duckdb


def prepare_data(raw_df: pd.DataFrame, keyword_dict_file: list) -> 0:
    """ 
    Prepare data for modeling. Given the raw data frame and a list of keyword dictionary files, 
    this function will create a new data file with the keyword columns added to the raw data file.
    it will use the keywords_finder function from description_processor.py to find the keywords in the Job Description column.
    it will then save the new data file to the output_file path.
    
    Args:
    raw_df: pd.DataFrame: The raw data frame
    keyword_dict_file: list: A list of keyword dictionary files

    Returns:
    data: pd.DataFrame: The new data frame with the keyword columns added
        
    """
    data = raw_df
    
    print(f'Starting the keyword search for {len(data)} jobs. It may take a while...')
    for keyword_dict in keyword_dict_file:
        with open(keyword_dict) as f:
            keywords_mapping = json.load(f)
            # get the file name only without the extension
            keyword_column_name = os.path.basename(keyword_dict).split('.')[0].replace('_keywords', '').capitalize()
            data[keyword_column_name] = data['jobDescription'].apply(
                lambda x: keywords_finder(x, keywords_mapping))

    # drop the Job Description column
    data.drop('jobDescription', axis=1, inplace=True)

    # for aesthetic purposes, remove - from seach term and capitalize the first letter
    # replace null with empty string
    data['searchKeywords'] = data['searchKeywords'].fillna('Unknown')
    data['searchKeywords'] = data['searchKeywords'].apply(
        lambda x: x.replace('-', ' ').capitalize())

    # move job id to the first column
    data = data[['jobId'] + [col for col in data.columns if col != 'jobId']]


    # convert scrape date to date only
    data['searchDate'] = pd.to_datetime(data['searchDate']).dt.date
    
    # convert salary to min, max, per annum and per hour
    print(f'Processing salary...')
    # replace null with empty string
    data['jobSalary'] = data['jobSalary'].fillna('Unknown')
    sal_processed = data['jobSalary'].apply(salary_processor)
    sal_df = pd.DataFrame(sal_processed)
    data = pd.concat([data, sal_df], axis=1)
    
    # convert job post date to date only
    data['jobPostedTime'] = pd.to_datetime(data['jobPostedTime'])

    # apply jobworktype processor
    data['jobWorkType'] = data['jobWorkType'].apply(job_work_type_processor)

    # print success message
    print(f'Data preparation completed successfully!')

    return data

def salary_processor(salary_text: str, debug: bool = False):
    """
    Process the salary column. To find the min, max, per annum and per hour salary.
    """
    salary_dict = {
        'min': 0,
        'max': 0,
        'per_annum': 0,
        'per_hour': 0,
        'per_day': 0,
        'jobSal_formatted': salary_text
    }
    
    if salary_text == '' or salary_text == 'Unknown':
        return pd.Series(salary_dict.values(), index=salary_dict.keys())
    else:
        # replace 'to' with '-' to make it easier to split
        salary_text = salary_text.replace('to', '-').lower()
        
        # get rid of comma in salary
        salary_text = salary_text.replace(',', '').replace(', ', '')
        # replace all spaces between digits with nothing
        salary_text = re.sub(r'(\d)\s+(\d)', r'\1\2', salary_text)

        # replace 'per hour' with 'per hr' to make it easier to split using regex
        salary_text = salary_text.replace('per hour', 'per hr')\
            .replace('p. h.', 'per hr').replace('p.h.', 'per hr').replace('p/h', 'per hr').replace('ph', 'per hr')\
            .replace('p/h', 'per hr').replace('/hour', 'per hr').replace('/ hour', 'per hr').replace('/hr', 'per hr').replace('/h', 'per hr')\
            .replace('perhour', 'per hr').replace('an hour', 'per hr')
        
        # replace 'per day' with 'per dy' to make it easier to split using regex
        salary_text = salary_text.replace('per day', 'per dy').replace('perday', 'per dy')\
            .replace('p. d.', 'per dy').replace('p.d.', 'per dy').replace('p/d', 'per dy').replace('per day', 'per dy')\
            .replace('a day', 'per dy').replace('daily', 'per dy').replace('/day', 'per dy')\
            .replace('pd', 'per dy').replace('dr', 'per dy').replace('day rate', 'per dy').replace('dayrate', 'per dy')
        
        # replace 'per fortnight' with 'per fn' to make it easier to split using regex
        salary_text = salary_text.replace('per fortnight', 'per fn').replace('perfortnight', 'per fn')\
            .replace('p. fn.', 'per fn').replace('p.fn.', 'per fn').replace('p/fn', 'per fn')\
            .replace('2 weeks', 'per fn').replace('2weeks', 'per fn').replace('2-weeks', 'per fn')\
            .replace('a fortnight', 'per fn').replace('a f.n.', 'per fn').replace('a fn', 'per fn')\
            .replace('2-weeks', 'per fn')
        
        # replace 'per week' with 'per w' to make it easier to split using regex
        salary_text = salary_text.replace('per week', 'per w').replace('perweek', 'per w')\
            .replace('p. wk.', 'per w').replace('p.wk.', 'per w').replace('p/wk', 'per w')\
            .replace('a week', 'per w').replace('a w.', 'per w').replace('a wk', 'per w')
        
        # repalce 'per annum' with 'per yr' to make it easier to split using regex
        salary_text = salary_text.replace('per annum', 'per yr').replace('perannum', 'per yr')\
            .replace('p. yr.', 'per yr').replace('p.yr.', 'per yr').replace('p/yr', 'per yr')\
            .replace('per year', 'per yr').replace('peryear', 'per yr').replace('per year', 'per yr')\
            .replace('p.a.', 'per yr').replace('pa', 'per yr').replace('p.a', 'per yr')

        # # replace k with 000 to make it easier to split
        salary_text = salary_text.replace('k', '000')
        min_salary = 0
        max_salary = 0


        if '-' in salary_text:
            sals = salary_text.split('-')
            if len(sals) > 2:
                # when there are too many numbers use ratio to find the two closest numbers and use them as min and max
                all_sals = re.findall(r'\$\s?(\d+)', salary_text)
                all_sal_num = [int(sal) for sal in all_sals if int(sal) > 20]
                # find the two closest numbers form the list using ratio
                for i,val in enumerate(all_sal_num):
                    for j in range(i+1, len(all_sal_num)):
                        if (all_sal_num[i] / all_sal_num[j]) < 2.0 and (all_sal_num[i] / all_sal_num[j]) > 0.5:
                            min_salary = all_sal_num[i]
                            max_salary = all_sal_num[j]
                            break
            elif len(sals) == 1:
                # when there is only one number, use it as min and max
                min_salary_txt = re.findall(r'\$\s?(\d+)', salary_text)
                if len(min_salary_txt) > 0:
                    min_salary = int(min_salary_txt[0])
                    if min_salary < 20:
                        min_salary = 0
                    max_salary = min_salary

            else:
                min_salary_prt, max_salary_prt = salary_text.split('-')
                # use regex to find number following the $ sign
                min_salary_txt = re.findall(r'\$\s?(\d+)', min_salary_prt)
                if len(min_salary_txt) > 0:
                    min_salary = int(min_salary_txt[0])
                    if min_salary < 20:
                        min_salary = 0
                # use regex to find number following the $ sign
                max_salary_txt = re.findall(r'\$?\s?(\d+)', max_salary_prt)
                if len(max_salary_txt) > 0:
                    max_salary = int(max_salary_txt[0])
                    if max_salary < 20:
                        max_salary = 0
        else:
            min_salary_txt = re.findall(r'\$\s?(\d+)', salary_text)
            if len(min_salary_txt) > 0:
                min_salary = int(min_salary_txt[0])
                max_salary = min_salary
        if debug:
            print(f'salary_text: {salary_text}')
            print(f'min_salary: {min_salary}, max_salary: {max_salary}')

        scale  = {"day": 260, "week": 52,  "year": 1, "fortnight": 26, "hour": 1976, "thousand": 0.001,'k': 1000}
        scalecap = {"day": 4000, "week": 20000, "year": 1000000, "fortnight": 30000, "hour": 400}

        if min_salary != 0 and max_salary != 0:
          avg_salary = (min_salary + max_salary) // 2
        elif min_salary != 0 and max_salary == 0:
            avg_salary = min_salary
        elif min_salary == 0 and max_salary != 0:
            avg_salary = max_salary
        else:
            avg_salary = 0
        
        required_scale = 'year'
        # find the scale of the salary
        if 'per yr' not in salary_text and avg_salary < scalecap['year']:
            if 'per hr' in salary_text and avg_salary < scalecap['hour']:
                required_scale = 'hour'
            elif 'per dy' in salary_text and avg_salary < scalecap['day']:
                required_scale = 'day'
            elif 'per wk' in salary_text and avg_salary < scalecap['week']:
                required_scale = 'week'
            elif 'per fn' in salary_text and avg_salary < scalecap['fortnight']:
                required_scale = 'fortnight'
            else:
                if avg_salary < scalecap['hour']:
                    required_scale = 'hour'      
                elif avg_salary < scalecap['day'] and avg_salary > scalecap['hour']:
                    required_scale = 'day'
        else:
            if avg_salary < (scalecap['year']/1000):
                required_scale = 'k'
            required_scale = 'year'
        
        # number sanity check
        if avg_salary > 1000000 or min_salary > 1000000 or max_salary > 1000000:
            required_scale = 'thousand'

        # if the min and max salary are in different scale, convert them to the same scale
        if min_salary < max_salary/1000 and max_salary < scalecap['year']:
                min_salary = min_salary * 1000
        elif min_salary < max_salary/1000 and max_salary > scalecap['year']:
            max_salary = max_salary / 1000
        if max_salary < min_salary and min_salary < scalecap['year']:
            max_salary = max_salary * 1000
        elif max_salary < min_salary and min_salary > scalecap['year']:
                min_salary = min_salary / 1000

        avg_salary = (min_salary + max_salary)//2    

        salary_dict['min'] = min_salary * scale[required_scale]
        salary_dict['max'] = max_salary * scale[required_scale]
        salary_dict['per_annum'] = avg_salary * scale[required_scale]
        salary_dict['per_hour'] = (avg_salary * scale[required_scale])// scale['hour']
        salary_dict['per_day'] = (avg_salary* scale[required_scale])// scale['day']
        salary_dict['jobSal_formatted'] = salary_text

    return pd.Series(salary_dict.values(), index=salary_dict.keys())

def job_work_type_processor(jobWorkType_text: str):
    """ Parse job work type from text like Full Time, Contract/Temp, Part Time
    return a list of job work types
    """
    jobWorkType = None
    if jobWorkType_text is None:
        jobWorkType = None
    else:    
        
        if ',' in jobWorkType_text:
            jobWorkType = 'Mixed'
        else:
            jobWorkType = jobWorkType_text.lower()
    return jobWorkType

def process_data(conn):
    # get the current working directory
    cwd = os.getcwd()
    # get the raw data file
    getting_data_sql = f"""select j.*, jd.jobDescription
                        from jobs j
                        inner join jobs_descriptions jd on jd.jobId = j.jobId"""
                        # where jobId not in (select jobId from dbo.jobs_processed)"""
    raw_data = conn.execute(getting_data_sql).fetchdf()
    if len(raw_data) > 0:
        print(f'Processing {len(raw_data)} new jobs from the database')
        # get the keyword dictionary files
        keyword_dict_file = [os.path.join(cwd, 'keywords', file)
                            for file in os.listdir(os.path.join(cwd, 'keywords'))]
        # prepare the data
        processed_data = prepare_data(raw_data, keyword_dict_file)
        # # save the processed data to the database
        table_name = 'jobs_processed'
        pk_columns = ['jobId']
        write_df_to_duckdb(processed_data, table_name, conn, pk_columns)
    else:
        print('No new data to process')

