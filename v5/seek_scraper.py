import requests
import pandas as pd
import json
import time


def search_keyword(keyword, location, num_pages:int = None, start_page:int = 1):
    #seek api base url
    BASE_URL = "https://www.seek.com.au/api/chalice-search/v4/search"

    SEARCH_PARAM_DICT = {
        "siteKey": "AU-Main",
        "sourcesystem": "houston",
        "where": location,
        "page": start_page,
        "seekSelectAllPages": False,
        "keywords": keyword,
        "locale": "en-AU"
    }

    # get the json path of each attribute
    REQUIRED_COLUMNS_MAPPING = {
        "advertiserName": "advertiser.description",
        "jobClassification": "classification.description",
        "jobSubClassification": "subClassification.description",
        "jobPostedTime": "listingDate",
        "searchLocation": "location",
        "country": "jobLocation.countryCode",
        "searchKeywords": "roleId",
        "jobTitle": "title",
        "jobId": "id",
        "jobSalary": "salary",
        "shortDescription": "teaser",
        "jobWorkType": "workType",
        "jobLocation": "suburb",
        "jobArea": "area",
    }
    return_df = pd.DataFrame()
    pages_scraped = 0
    if not num_pages:
        SEARCH_PARAM_DICT["seekSelectAllPages"] = True
        response = requests.get(BASE_URL, params=SEARCH_PARAM_DICT)
        if response.status_code == 200:
            data = response.json()
            total_jobs = data['totalCount']
            total_pages = data['totalPages']
            print(f"Total Jobs: {total_jobs} found in {total_pages} pages.")
            num_pages = total_pages
    print(f"Scraping {num_pages} pages")
    SEARCH_PARAM_DICT["seekSelectAllPages"] = False
    while pages_scraped <= num_pages:
        SEARCH_PARAM_DICT["page"] += 1
        print(f"Scraping page number: {SEARCH_PARAM_DICT['page']}")
        response = requests.get(BASE_URL, params=SEARCH_PARAM_DICT)
        if response.status_code == 200:
            data = response.json()
            df = pd.json_normalize(data['data'])
            if len(df) == 0:
                print(f"No more data found for {keyword} in {location}")
                break
            else:
                # check if all the required columns are present
                missing_columns = [col for col in REQUIRED_COLUMNS_MAPPING.values() if col not in df.columns]
                if missing_columns:
                    print(f"Missing columns: {missing_columns}")
                    # add the missing columns
                    for col in missing_columns:
                        df[col] = None
                # only select the required columns
                df = df[REQUIRED_COLUMNS_MAPPING.values()]
                # rename the columns
                df.columns = REQUIRED_COLUMNS_MAPPING.keys()
                # convert the jobPostedTime to datetime
                df["jobPostedTime"] = pd.to_datetime(df["jobPostedTime"]).dt.date
                # add a searchDate column
                df["searchDate"] = pd.to_datetime("today").date()
                return_df = pd.concat([return_df, df], ignore_index=True)
        else:
            print(f"Failed to get the data for {keyword} in {location}")
            return return_df
        pages_scraped += 1
    return return_df

def search_related_keywords(keyword, location):
    seek_base_url = "https://www.seek.com.au/api/chalice-search/v4/related-search"
    params = {
        'zone': 'anz-1',
        'keywords': keyword,
        'siteKey': 'au',
        'cluster': 'default',
        'where': location,
    }
    response = requests.get(seek_base_url, params=params)
    columns_mapping = {
        'keywords': 'keywords',
        'totalJobs': 'totalJobs'
    }

    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data['relatedSearches'])
        # keep only the required columns
        df = df[columns_mapping.keys()]
        # rename the columns
        df.columns = columns_mapping.values()
        # Add a searchDate column
        df["searchDate"] = pd.to_datetime("today").date()
        # Add a location column
        df["location"] = location
        # re order the columns
        df = df[['keywords',  'location', 'searchDate', 'totalJobs']]

        return df
    else:
        print(f"Failed to get the data for {keyword} in {location}")
        return pd.DataFrame()

if __name__=="__main__":
    # trends_df = search_related_keywords("Data Analyst", "Sydney")
    # print(trends_df.head())
    # trends_df.to_csv("data_analyst_sydney_trends.csv", index=False)
      
    df = search_keyword("Data Analyst", "Sydney", num_pages=None, start_page=25)
    print(df.head())
    # read existing data
    existing_df = pd.read_csv("data_analyst_sydney.csv")

    # join the new data with existing data
    df = pd.concat([existing_df, df], ignore_index=True)
    # de-duplicate the data based on jobId
    df = df.drop_duplicates(subset=["jobId"], keep="last")
    # save the data to a csv file
    df.to_csv("data_analyst_sydney.csv", index=False)