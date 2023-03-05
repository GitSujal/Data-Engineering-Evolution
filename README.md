# Data Engineering Evolution
This repository is an attempt to show the data engineering evolution from basic analytics on your local machine to orchestrated ETL pipelines in the cloud. The goal is to show how the data engineering landscape has evolved over the years and how it is still evolving. This repository is a work in progress and will be updated to include more recent technologies and tools.

## v1 - Local Analysis no spearation of ETL and Analysis
The first version of the data engineering evolution is a simple data pipeline that scrapes data from a website and stores it in a local CSV file. It scrapes the data from seek and stores it in a CSV file. The data is then loaded into a pandas dataframe and analysed. The analysis is then visualised using matplotlib. Everything is done locally on your machine.

## v2 - Local Analysis with separation of ETL and Analysis
The second version of the data engineering evolution is a simple data pipeline that scrapes data from a website and stores it in a local CSV file. It scrapes the data from seek and stores it in a CSV file. The data is then loaded into a pandas dataframe and analysed using a separate processing script. The analysis is then visualised using a web application that is hosted locally. The web application is built using streamlit and visualisation is done using plotly. Everything is done locally on your machine.
