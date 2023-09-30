from v4.util.seek_scraper import SeekScraper
import logging
import pandas as pd
import os

job = "data-scientist"
location = "australia"
seek_scraper_local = SeekScraper(job=job, location=location, profile_name="local")
seek_scraper_remote = SeekScraper(job=job, location=location, profile_name="remote-dump")
# mock df 
df = pd.DataFrame({"jobId": [1, 2, 3], "title": ["data scientist", "data analyst", "data engineer"]})
indexes  = pd.DataFrame({"jobId": [1, 2, 3]})

def test_init_local():
    assert seek_scraper_local.job == job and \
            seek_scraper_local.location == location and \
            seek_scraper_local.job == "data-scientist" and \
            seek_scraper_local.location == "australia" and \
            seek_scraper_local.config["raw-data-folder"] == "data/raw-data" and \
            seek_scraper_local.config["processed-data-folder"] == "data/processed" and \
            seek_scraper_local.config["index-data-folder"] == "data/index" 

def test_init_remote():
    assert seek_scraper_remote.job == job and \
            seek_scraper_remote.location == location and \
            seek_scraper_remote.job == "data-scientist" and \
            seek_scraper_remote.location == "australia" and \
            seek_scraper_remote.config["raw-data-filname-pattern"] == f"jobs_{job}_{location}" 

def test_save_to_csv_local():
    seek_scraper_local.data = df
    seek_scraper_local.save_to_csv()
    # check the output file exists in the raw folder
    full_file_path = os.path.join(seek_scraper_local.config["raw-data-folder"], 
                                  f"{seek_scraper_local.config['raw-data-filname-pattern']}.{seek_scraper_local.config['raw-data-file-format']}")
    read_df = pd.read_csv(full_file_path)
    assert read_df.equals(df)

def test_save_to_csv_remote():
    seek_scraper_remote.data = df
    file_key = seek_scraper_remote.save_to_csv()
    read_df = None
    if seek_scraper_remote.config['raw-data-file-format'] == 'csv':
        read_df = pd.read_csv(file_key)
    elif seek_scraper_remote.config['raw-data-file-format'] == 'parquet':
        read_df = pd.read_parquet(file_key)
    assert read_df.equals(df)

def test_save_all_indexes_local():
    file_key = seek_scraper_local.save_all_indexes(new_indexes = indexes['jobId'].tolist())
    if seek_scraper_local.config['index-data-file-format'] == 'csv':
        read_df = pd.read_csv(file_key)
    elif seek_scraper_local.config['index-data-file-format'] == 'parquet':
        read_df = pd.read_parquet(file_key)
    assert read_df.equals(indexes)

def test_save_all_indexes_remote():
    file_key = seek_scraper_remote.save_all_indexes(new_indexes = indexes['jobId'].tolist())
    if seek_scraper_remote.config['index-data-file-format'] == 'csv':
        read_df = pd.read_csv(file_key)
    elif seek_scraper_remote.config['index-data-file-format'] == 'parquet':
        read_df = pd.read_parquet(file_key)
    assert read_df.equals(indexes)

def test_scraper_local():
    data = seek_scraper_local()
    assert len(data) > 0

def test_scraper_remote():
    data = seek_scraper_remote()
    assert len(data) > 0