import requests
from bs4 import BeautifulSoup

pages_to_scrape = 2
for i in range(1, pages_to_scrape + 1):
    URL = f"https://www.seek.com.au/data-analyst-jobs/in-All-Australia?page={i}"
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, "html.parser")
    print(soup.prettify())
    # write soup.preetify() to a file
    with open(f"seek_{i}.html", "w") as file:
        file.write(soup.prettify())



    # results = soup.find("div", {"class": "job-listings"})
    # job_elems = results.find_all("article")

    # for job_elem in job_elems:
    #     title_elem = job_elem.find("a", {"class": "job-title"})
    #     company_elem = job_elem.find("a", {"class": "job-listing-company-name"})
    #     location_elem = job_elem.find("span", {"class": "job-location"})
    #     print(title_elem.text)
    #     print(company_elem.text)
    #     print(location_elem.text)
    #     print()