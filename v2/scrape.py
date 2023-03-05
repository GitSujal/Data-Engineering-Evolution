import argparse
from seek_scraper import SeekScraper
import pandas as pd

def scrape_data(job: str, location: str, pages: int, skip: int=1, debug: bool=False) -> pd.DataFrame:
    """
    scrape data from seek
    :param job: job title to scrape
    :param location: location to scrape
    :param pages: number of pages to scrape
    :param skip: number of pages to skip
    :param debug: debug mode
    :return: dataframe with scraped data 
    """
    ss = SeekScraper(job=job, location=location, pages_to_scrape=pages, skip_pages=skip, debug=debug)
    data = ss()
    jobs_count = len(data)
    # print success message with job count  job and location
    print(f'Successfully scraped {jobs_count} jobs for {job} in {location}')
    return data


if __name__ == '__main__':
    # parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--job', type=str, default='data-analyst', help='Job title to scrape (default: data-analyst)')
    parser.add_argument('--location', type=str, default='perth', help='Location to scrape (default: perth)')
    parser.add_argument('--pages', type=int, default=1, help='Number of pages to scrape (default: 1)')
    parser.add_argument('--skip', type=int, default=1, help='Number of pages to skip (default: 1)')
    parser.add_argument('--debug', type=bool, default=False, help='Debug mode (default: False)')
    args = parser.parse_args()

    # call the scrape data function
    scrape_data(job=args.job, location=args.location, pages=args.pages, skip=args.skip, debug=args.debug)

