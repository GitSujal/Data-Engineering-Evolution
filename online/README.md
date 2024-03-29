## v3 - Better Scraping and Extra columns, expanded transformations
The third version of the data engineering evolution is a getting more data. The data grows and so does the complexity of transformations. The data is barely suitable to be stored offline and transformation is taking longer as well. The data is still local and the visualisations are hosted on local machin.
---

## (E) Extract
Python module `seek_scraper` is used to scrape data from seek. The module is built using the requests and BeautifulSoup libraries. The module is used to scrape data from seek and store it in a CSV file. `scrape.py` is the main module that is used to run the scraper. The data is stored in `data/` directory.

## (T) Transform
The data extracted using the above module is then read into pandas dataframe and analysed using transformations. The transformations are defined in `description_processor` module. 

## (L) Load
The result after transformations being applied are stored in `data/processed` directory in CSV format.

## (R) Report / (V) Visualise 
The processed data is then loaded into a web application built using streamlit. The web application is used to visualise the data. The web application is hosted locally on your machine.

---

## Usage

While each module can be used separately or imported into any python program for its own use, the easiest way to run this project is by using the web application. To start the web application locally, run the following command:

```bash
$ streamlit run Skills.py
```

### Installing Preqrequisites

Install the required python packages using the following command:
```bash
$ pip install -r requirements.txt
```


