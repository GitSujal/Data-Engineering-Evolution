from v4.infra import scrape_lambda
import logging
import pandas as pd

def test_scrape_lambda():
    event = {"job": "Data Analyst", "location": "Australia", "log_level": logging.INFO, "jobs_to_scrape": 10}
    context = None
    data = scrape_lambda.lambda_handler(event, context)
    # find the latest date in the data
    latest_date = pd.to_datetime(data["searchDate"].max())
    # asset that teh latest date is within 5 minutes of now
    assert latest_date > pd.Timestamp.now() - pd.Timedelta(minutes=5)
