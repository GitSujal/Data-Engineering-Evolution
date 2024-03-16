import argparse
from seek_scraper import SeekScraper
import pandas as pd

def scrape_data(job: str, location: str, jobs_to_scrape: int, debug: bool=False) -> pd.DataFrame:
    """
    scrape data from seek
    :param job: job title to scrape
    :param location: location to scrape
    :param jobs_to_scrape: number of jobs to scrape
    :param debug: debug mode
    :return: dataframe with scraped data 
    """
    ss = SeekScraper(job=job, location=location, jobs_to_scrape=jobs_to_scrape, debug=debug)
    data = ss()
    # print success message with job count  job and location
    print(f'Successfully scraped {jobs_to_scrape} jobs for {job} in {location}')
    return data


if __name__ == '__main__':
    # parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-j','--job', type=str, default='data-analyst', help='Job title to scrape (default: data-analyst)')
    parser.add_argument('-l','--location', type=str, default='perth', help='Location to scrape (default: perth)')
    parser.add_argument('-n','--num_jobs_to_scrape', type=int, default=100, help='Number of jobs to scrape (default: 100)')
    parser.add_argument('--debug', type=bool, default=False, help='Debug mode (default: False)')
    args = parser.parse_args()

    # call the scrape data function
    scrape_data(job=args.job, location=args.location, jobs_to_scrape=args.num_jobs_to_scrape,
                 debug=args.debug)

