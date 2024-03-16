import streamlit as st
from scrape import scrape_data

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
        df = scrape_data(job=search_term, location=location, jobs_to_scrape=pages*100, debug=False)
        with st.spinner("Saving Data..."):
            st.info(f"{len(df)} jobs scraped and saved to data/seek_scraper_raw_data.csv", icon="ℹ️")
        # show the raw data in a table upto 10 rows
        st.write(df.head(10))
        
