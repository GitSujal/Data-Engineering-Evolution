from v4.util.seek_scraper import SeekScraper
import logging

logger = logging.getLogger()

def lambda_handler(event, context):
    if "job" in event and "location" in event:
        job = event["job"]
        location = event["location"]
    else:
        logger.error("Job and Location must be provided in the event")
    logger.setLevel(logging.INFO)
    jobs_to_scrape = 100
    if "log_level" in event:
        logger.setLevel(event["log_level"])
    if "jobs_to_scrape" in event:
        jobs_to_scrape = event["jobs_to_scrape"]
    logger.info(f"Starting to Scrape {jobs_to_scrape} Job: {job}, Location: {location}")
    
    scraper = SeekScraper(
        job=job,
        location=location,
        jobs_to_scrape=jobs_to_scrape,
        log_level=logger.getEffectiveLevel()
        )
    data = scraper()
    logger.info(f"Finished Scraping {jobs_to_scrape} Job: {job}, Location: {location}")
    
    return data