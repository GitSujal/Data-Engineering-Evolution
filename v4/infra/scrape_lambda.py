from v4.util.seek_scraper import SeekScraper
import logging
import inspect

logger = logging.getLogger()

def lambda_handler(event, context):
    logger.setLevel(logging.INFO)
    if "job" not in event and "location" not in event:
        logger.error("Job and Location must be provided in the event")
    else:
        # get the kwargs from the seek scraper function
        args = inspect.getfullargspec(SeekScraper).args
        kwargs = {}
        for arg in args:
            if arg in event:
                kwargs[arg] = event[arg]
        logger.info(f"Starting to Scrape with kwargs: {kwargs}") 
        scraper = SeekScraper(**kwargs)
        data = scraper()
        if "jobs_to_scrape" in event:
            logger.info(f"Scraped {event['jobs_to_scrape']} jobs")
        else:
            logger.info(f"Successfully scraped 100 jobs")
    return data