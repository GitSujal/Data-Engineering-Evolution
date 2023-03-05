from description_processor import keywords_finder
import json
import pandas as pd
import os


def prepare_data(raw_data_file: str, keyword_dict_file: list, output_file: str) -> None:
    """ 
    Prepare data for modeling. Given the raw data file and a list of keyword dictionary files, 
    this function will create a new data file with the keyword columns added to the raw data file.
    it will use the keywords_finder function from description_processor.py to find the keywords in the Job Description column.
    it will then save the new data file to the output_file path.
        :param raw_data_file: the raw data file
        :param keyword_dict_file: list of keyword dictionary files
        :param output_file: the output file
        :return: None
    """
    data = pd.read_csv(raw_data_file)
    for keyword_dict in keyword_dict_file:
        with open(keyword_dict) as f:
            keywords_mapping = json.load(f)
            # get the file name only without the extension
            keyword_column_name = os.path.basename(keyword_dict).split('.')[0].replace('_keywords', '').capitalize()
            data[keyword_column_name] = data['Job Description'].apply(
                lambda x: keywords_finder(x, keywords_mapping))

    # drop the Job Description column
    data.drop('Job Description', axis=1, inplace=True)

    # for aesthetic purposes, remove - from seach term and capitalize the first letter
    data['Search Term'] = data['Search Term'].apply(
        lambda x: x.replace('-', ' ').capitalize())

    # move job id to the first column
    data = data[['Job ID'] + [col for col in data.columns if col != 'Job ID']]

    # convert scrape date to date only
    data['Scrape Date'] = pd.to_datetime(data['Scrape Date']).dt.date
    
    # save the data
    data.to_csv(output_file, index=False)

    # print success message
    print(f'Data preparation completed successfully! You can find the processed data at: {output_file}')

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
