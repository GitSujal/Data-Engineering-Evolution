import boto3
import pandas as pd
from io import BytesIO, StringIO
import logging


def dataframe_to_s3(s3_client , input_datafame: pd.DataFrame, bucket_name: str, key: str, file_format: str = 'parquet'):
    """
    This function saves a pandas dataframe to s3 in either parquet or csv format
    Args:
        s3_client (boto3 s3 client): this is the s3 client that is connected to the aws account
        input_datafame (pd.DataFrame): the dataframe that you want to save to s3
        bucket_name (str): the name of the bucket that you want to save the data to
        filepath (str): the filepath that you want to save the data to it should be in the format of 'folder/folder/'
        file_format (str, optional): Format you want to write to s3. Defaults to 'parquet'. This needs to be either 'parquet' or 'csv'.
    """
    if file_format == 'parquet':
        out_buffer = BytesIO()
        input_datafame.to_parquet(out_buffer, index=False)

    elif file_format == 'csv':
        out_buffer = StringIO()
        input_datafame.to_csv(out_buffer, index=False)
    # put the out_buffer to s3 make sure use md5 to check the integrity of the data and check if the data is uploaded successfully
    logging.info(f"Uploading data to s3 bucket {bucket_name} with filepath {key} in {file_format} format")
    resp = s3_client.put_object(Body=out_buffer.getvalue(), Bucket=bucket_name.replace('/',''), Key=key)

    if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
        logging.error(f"Failed to upload data to s3 bucket {bucket_name} with filepath {key}")
        raise Exception(f"Failed to upload data to s3 bucket {bucket_name} with filepath {key}")
    else:
        logging.info(f"Successfully uploaded data to s3 bucket {bucket_name} with filepath {key}")
        return True
    

def read_from_s3(s3_client , bucket_name: str, filepath: str, filename: str):
    """
    This function saves a pandas dataframe to s3 in either parquet or csv format
    Args:
        s3_client (boto3 s3 client): this is the s3 client that is connected to the aws account
        bucket_name (str): the name of the bucket that you want to save the data to
        filepath (str): the filepath that you want to save the data to it should be in the format of 'folder/folder/'
        filename (str): the name of the file that you want to read from s3. The extension from the file would be used
                         to determine the format of the file
    """
    # get the file extension
    file_extension = filename.split('.')[-1]
    # get the file from s3
    file_object = s3_client.get_object(Bucket=bucket_name.replace('/',''), Key=filepath + filename)
    # read the file from the file object
    file_content = file_object['Body'].read()
    # convert the file content to a dataframe
    if file_extension == 'parquet':
        return pd.read_parquet(BytesIO(file_content))
    elif file_extension == 'csv':
        return pd.read_csv(StringIO(file_content))
    else:
        logging.error(f"File extension {file_extension} is not supported")
        raise Exception(f"File extension {file_extension} is not supported")
    