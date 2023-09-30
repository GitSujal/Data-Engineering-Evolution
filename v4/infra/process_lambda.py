from v4.util.data_processor import prepare_data
import logging
import boto3
import pandas as pd
from v4.util.aws_utils import read_from_s3

logger = logging.getLogger()

def gather_files(bucket, prefix=None, fileformat:str='parquet'):
    """Gather all parquet files from S3 bucket"""
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    files = []
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith(f'.{fileformat}'):
            files.append(obj.key)
    return files

def combine_files(bucket, files):
    """Combine all parquet files into a single dataframe"""
    dfs = []
    for file in files:
        dfs.append(pd.read_parquet(f's3://{bucket}/{file}'))
    df = pd.concat(dfs)
    # get rid of duplicates and reset index
    df = df.drop_duplicates(subset=['jobId']).reset_index(drop=True)
    return df


def lambda_handler(event, context):
    logger.info('Received event: ' + str(event))
    logger.info('Received context: ' + str(context))
    # Gather all parquets from S3 bucket
    raw_bucket = event['raw_bucket'] if 'raw_bucket' in event.keys() else 'jobs-data-scraped-raw'
    processed_bucket = event['processed_bucket'] if 'processed_bucket' in event.keys() else 'jobs-data-scraped-processed'
    keyword_bucket = event['keyword_bucket'] if 'keyword_bucket' in event.keys() else 'keywords-json-file'
    jobs_to_process = event['jobs_to_process'] if 'jobs_to_process' in event.keys() else 'data'
    keyword_prefixes = event['keyword_prefixes'] if 'keyword_prefixes' in event.keys() else 'data-job'
    output_file = event['output_file'] if 'output_file' in event.keys() else f'{jobs_to_process}/seek-data-processed.parquet'
    remote = event['remote'] if 'remote' in event.keys() else True
    all_raw_files = gather_files(bucket=raw_bucket, prefix=f'raw-data/{jobs_to_process}')
    # get all keyword jsons
    keyword_jsons = gather_files(bucket=keyword_bucket, prefix=f'keywords/{keyword_prefixes}')
    # Combine all parquets into a single dataframe
    df = combine_files(bucket=raw_bucket, files=all_raw_files)
    # Process data
    prepare_data(data = df, 
                 keyword_jsons = keyword_jsons, 
                 output_file = output_file, 
                 remote = remote, 
                 output_bucket = processed_bucket, 
                 keyword_bucket = keyword_bucket)
    
    logger.info('Processed data successfully')

