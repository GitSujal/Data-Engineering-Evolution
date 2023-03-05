## v1 - Local Analysis no spearation of ETL and Analysis
The first version of the data engineering evolution is a simple data pipeline that scrapes data from a website and stores it in a local CSV file. It scrapes the data from seek and stores it in a CSV file. The data is then loaded into a pandas dataframe and analysed. The analysis is then visualised using matplotlib. Everything is done locally on your machine.

# Usage

To run the project open the data-cleaning.ipynb file in jupyter notebook and run the cells. The data is stored in `data/` directory.

# Installing Preqrequisites

Install the required python packages using the following command:
```bash
$ pip install -r requirements.txt
```