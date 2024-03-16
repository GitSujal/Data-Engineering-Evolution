import streamlit as st
from scrape import scrape_data
import os
import sqlalchemy
import yaml


config = yaml.safe_load(open('config.yml'))
# create a new db engine
server = config['db_credentials']['server']
database = config['db_credentials']['database']
user = config['db_credentials']['user']
password = os.getenv('DP101_PASSWORD')

sql_engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
                                  connect_args={'connect_timeout': 30})

st.set_page_config(page_title="Scrape Job Data", page_icon="🔍")
st.title("Scrape Data from Seek")

st.write("This is a simple app to scrape data from Seek")



l1, l2, r1, r2 = st.columns(4)

with l1:
    search_term = st.text_input("Search Term", "Data Scientist")
with l2:
    location = st.text_input("Location", "Sydney")
with r1:
    pages = st.number_input("Number of Pages to Scrape", 1, 100, 1)


with st.spinner("Scraping Data..."):
    if st.button("Scrape Data"):
        scraped_df = scrape_data(job=search_term, location=location, 
                         jobs_to_scrape=pages*100, debug=False,
                            save_local=False, save_sql=True, sql_engine=sql_engine
                         )
        with st.spinner("Saving Data..."):
            st.info(f"{len(scraped_df)} jobs scraped and saved to data/seek_scraper_raw_data.csv", icon="ℹ️")
        st.balloons()
        # show the raw data in a table upto 10 rows
        st.write(scraped_df.head(10))

        st.cache_data.clear()