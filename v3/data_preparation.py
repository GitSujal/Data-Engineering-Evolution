from description_processor import keywords_finder
import json
import pandas as pd
import os
import re
from datetime import datetime, timedelta


def prepare_data(raw_data_file: str, keyword_dict_file: list, output_file: str) -> 0:
    """ 
    Prepare data for modeling. Given the raw data file and a list of keyword dictionary files, 
    this function will create a new data file with the keyword columns added to the raw data file.
    it will use the keywords_finder function from description_processor.py to find the keywords in the Job Description column.
    it will then save the new data file to the output_file path.
        :param raw_data_file: the raw data file
        :param keyword_dict_file: list of keyword dictionary files
        :param output_file: the output file
        :return: 0
    """
    data = pd.read_csv(raw_data_file)
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
    data['searchKeywords'] = data['searchKeywords'].apply(
        lambda x: x.replace('-', ' ').capitalize())

    # move job id to the first column
    data = data[['jobId'] + [col for col in data.columns if col != 'jobId']]

    # convert scrape date to date only
    data['searchDate'] = pd.to_datetime(data['searchDate']).dt.date
    
    # convert salary to min, max, per annum and per hour
    sal_processed = data['jobSalary'].apply(salary_processor)
    sal_df = pd.DataFrame(sal_processed.tolist())
    data = pd.concat([data, sal_df], axis=1)
    
    # convert job post date to date only
    data['jobPostedTime'] = data['jobPostedTime'].apply(date_processor)
    data['jobPostedTime'] = pd.to_datetime(data['jobPostedTime']).dt.date

    # apply jobworktype processor
    data['jobWorkType'] = data['jobWorkType'].apply(job_work_type_processor)

    # save the data
    data.to_csv(output_file, index=False)

    # print success message
    print(f'Data preparation completed successfully! You can find the processed data at: {output_file}')


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
    }
    
    if salary_text == '' or salary_text == 'Unknown':
        return salary_dict
    else:
        # replace 'to' with '-' to make it easier to split
        salary_text = salary_text.replace('to', '-').lower()
        # replace k with 000 to make it easier to split
        salary_text = salary_text.replace('k', '000').replace('K', '000')
        # get rid of comma in salary
        salary_text = salary_text.replace(',', '').replace(', ', '')
        
        # replace 'per hour' with 'per hr' to make it easier to split using regex
        salary_text = salary_text.replace('per hour', 'per hr').replace('per hour', 'per hr')\
            .replace('p. h.', 'per hr').replace('p.h.', 'per hr').replace('p/h', 'per hr')
        
        # replace 'per day' with 'per dy' to make it easier to split using regex
        salary_text = salary_text.replace('per day', 'per dy').replace('perday', 'per dy')\
            .replace('p. d.', 'per dy').replace('p.d.', 'per dy').replace('p/d', 'per dy').replace('per day', 'per dy')
        
        # replace 'per fortnight' with 'per fn' to make it easier to split using regex
        salary_text = salary_text.replace('per fortnight', 'per fn').replace('perfortnight', 'per fn')\
            .replace('p. fn.', 'per fn').replace('p.fn.', 'per fn').replace('p/fn', 'per fn')\
            .replace('2 weeks', 'per fn').replace('2weeks', 'per fn').replace('2-weeks', 'per fn')
        
        # replace 'per week' with 'per wk' to make it easier to split using regex
        salary_text = salary_text.replace('per week', 'per wk').replace('perweek', 'per wk')\
            .replace('p. wk.', 'per wk').replace('p.wk.', 'per wk').replace('p/wk', 'per wk')
            
        min_salary = 0
        max_salary = 0

        if '-' in salary_text:
            sals = salary_text.split('-')
            if len(sals) > 2:
                # when there are too many numbers use ratio to find the two closest numbers and use them as min and max
                all_sals = re.findall(r'\$?\s?(\d+)', salary_text)
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
            min_salary_txt = re.findall(r'\$?\s?(\d+)', salary_text)
            if len(min_salary_txt) > 0:
                min_salary = int(min_salary_txt[0])
                max_salary = min_salary
        if debug:
            print(f'salary_text: {salary_text}')
            print(f'min_salary: {min_salary}, max_salary: {max_salary}')

        if min_salary != 0 and max_salary != 0:
            avg_salary = (min_salary + max_salary) // 2
        elif min_salary != 0 and max_salary == 0:
            avg_salary = min_salary
        elif min_salary == 0 and max_salary != 0:
            avg_salary = max_salary
        else:
            return salary_dict
        
        scale  = {"day": 260, "week": 52,  "year": 1, "fortnight": 26, "hour": 1976, "thousand": 0.001}
        scalecap = {"day": 4000, "week": 20000, "year": 1000000, "fortnight": 40000, "hour": 999}
        required_scale = None
        # find the scale of the salary
        if 'per hr' in salary_text and avg_salary < scalecap['hour']:
            required_scale = 'hour'
        elif 'per dy' in salary_text and avg_salary < scalecap['day']:
            required_scale = 'day'
        elif 'per wk' in salary_text and avg_salary < scalecap['week']:
            required_scale = 'week'
        elif 'per fn' in salary_text and avg_salary < scalecap['fortnight']:
            required_scale = 'fortnight'
        else:
            required_scale = 'year'

        if avg_salary > 1000000 or min_salary > 1000000 or max_salary > 1000000:
            required_scale = 'thousand'

        salary_dict['min'] = min_salary * scale[required_scale]
        salary_dict['max'] = max_salary * scale[required_scale]
        salary_dict['per_annum'] = avg_salary * scale[required_scale]
        salary_dict['per_hour'] = (avg_salary * scale[required_scale])// scale['hour']
        salary_dict['per_day'] = (avg_salary* scale[required_scale])// scale['day']

    return pd.Series(salary_dict.values(), index=salary_dict.keys())


def date_processor(date_text: str, scrape_date = datetime.now()):
    """ Parse data from formats like 2 days ago, 7h ago to python datetime
    return datetime object
    """
    if date_text is None:
        return None
    date_text = date_text.lower()
    
    result = None
    if 'ago' in date_text:     
        if 'h ago' in date_text or 'm ago' in date_text:
            days = 0
            result = scrape_date
        elif 'd ago' in date_text:
            days = re.findall(r'(\d+)\s?d', date_text)[0]
            result = scrape_date - timedelta(days=int(days)) 
    else:
        # format of date text is like 1 Feb 2023
        result = datetime.strptime(date_text, "%d %b %Y")
    
    return result.strftime("%Y-%m-%d")


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

def default():
    # get the current working directory
    cwd = os.getcwd()
    # get the raw data file
    raw_data_file = os.path.join(cwd, 'data', 'seek_scraper_raw_data.csv')
    # get the keyword dictionary files
    keyword_dict_file = [os.path.join(cwd, 'data', 'keywords', file)
                         for file in os.listdir(os.path.join(cwd, 'data', 'keywords'))]
    # get the output file
    output_file = os.path.join(
        cwd, 'data', 'processed', 'seek_scraper_processed_data.csv')
    if not os.path.exists(os.path.join(cwd, 'data', 'processed')):
        os.mkdir(os.path.join(cwd, 'data', 'processed'))
    # prepare the data
    prepare_data(raw_data_file, keyword_dict_file, output_file)


if __name__ == "__main__":
    default()
