from v4.infra import process_lambda
from v4.infra import scrape_lambda
import boto3
import logging

def test_process_lambda():
    raw_s3_bucket = "jobs-data-scraped-raw"
    raw_s3_filepath = "raw-data"
    raw_s3_data_filename = "seek_scraper_raw_data.parquet"
    raw_s3_index_filename = "seek_scraper_all_ids.parquet"
    event = {"job": "data-scientist", "location": "australia", "log_level": logging.INFO, "jobs_to_scrape": 10, 
             "data_folder": f"{raw_s3_bucket}/{raw_s3_filepath}", "data_filename": f"{raw_s3_data_filename}",
             "data_filename": f"{raw_s3_data_filename}",
             "index_folder": f"{raw_s3_bucket}/{raw_s3_filepath}", "index_filename": f"{raw_s3_index_filename}",
             "index_filename": f"{raw_s3_index_filename}",
             "remote": True
            }
    context = None
    data = scrape_lambda.lambda_handler(event, context)
    event = {
        'raw_bucket': raw_s3_bucket,
        'processed_bucket': 'jobs-data-scraped-processed',
        'keyword_bucket': 'keywords-json-file',
        'jobs_to_process': 'data',
        'keyword_prefixes': 'data-job',
        'output_file': 'data/seek-data-processed.parquet',
        'remote': True
    }
    context = {}

    process_lambda.lambda_handler(event, context)

    # check the output file exists in the processed bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('jobs-data-scraped-processed')
    files = []
    for obj in bucket.objects.filter(Prefix='data/seek-data-processed.parquet'):
        files.append(obj.key)
    assert len(files) == 1
