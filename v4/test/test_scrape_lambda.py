from v4.infra import scrape_lambda
import logging
import pandas as pd
import boto3

def test_scrape_lambda():
    event = {"job": "data-analyst", "location": "Australia", "log_level": logging.INFO, "jobs_to_scrape": 10}
    context = None
    data = scrape_lambda.lambda_handler(event, context)
    assert len(data) == 10

def test_saving_raw_data_to_s3():
    test_s3_bucket = "test-bucket-to-dump-data"
    test_s3_filepath = "test"
    test_s3_data_filename = "seek_scraper_raw_data.parquet"
    test_s3_index_filename = "seek_scraper_all_ids.parquet"
    event = {"job": "data-scientist", "location": "australia", "log_level": logging.INFO, "jobs_to_scrape": 10, 
             "data_folder": f"{test_s3_bucket}/{test_s3_filepath}", "data_filename": f"{test_s3_data_filename}",
             "data_filename": f"{test_s3_data_filename}",
             "index_folder": f"{test_s3_bucket}/{test_s3_filepath}", "index_filename": f"{test_s3_index_filename}",
             "index_filename": f"{test_s3_index_filename}",
             "remote": True
            }
    context = None
    data = scrape_lambda.lambda_handler(event, context)
    
    boto_session = boto3.Session()
    s3_client = boto_session.client("s3")
    # get all files from index folder
    index_files = s3_client.list_objects_v2(Bucket=test_s3_bucket, Prefix=f"{test_s3_filepath}/")
    # find the last modified date from all index files
    all_last_modified = [pd.to_datetime(file["LastModified"]).tz_localize(None) for file in index_files["Contents"]]
    index_last_modified = max(all_last_modified)
    logging.info(f"index_last_modified: {index_last_modified}")
    # get all files from data folder
    data_files = s3_client.list_objects_v2(Bucket=test_s3_bucket, Prefix=f"{test_s3_filepath}/")
    # find the latest last modified date from all data files
    all_last_modified = [pd.to_datetime(file["LastModified"]).tz_localize(None) for file in data_files["Contents"]]
    data_last_modified = max(all_last_modified)
    logging.info(f"data_files: {data_last_modified}")
    # 5 minute buffer while converting to GMT
    min_5_buffer = pd.Timestamp.now() - pd.Timedelta(minutes=485)
    logging.info(f"min_5_buffer: {min_5_buffer}")

    assert index_last_modified > min_5_buffer \
        and data_last_modified > min_5_buffer
