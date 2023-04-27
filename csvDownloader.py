import io
from typing import Tuple
import pandas as pd
import requests


def get_company_data() -> pd.DataFrame:
    # Define the URL for the daily index file (master.idx) on the EDGAR FTP server
    url = 'https://www.sec.gov/Archives/edgar/daily-index/master.idx'

    # Make a GET request to the URL and retrieve the response
    response = requests.get(url)

    # Read the response content into a StringIO object
    file_content = io.StringIO()
    with response.iter_content(1024) as chunk:
        for chunk in response.iter_content(1024):
            file_content.write(chunk.decode('utf-8'))
    file_content.seek(0)

    # Skip the first 10 lines of the file (which contain metadata) and read the rest into a pandas dataframe
    company_data = pd.read_csv(file_content, skiprows=10, delimiter='|', header=None,
                               names=['cik', 'company_name', 'form_type', 'date_filed', 'file_name'], usecols=[0, 4])

    # Keep only rows where form_type is '10-K' (this will filter out non-public companies)
    company_data = company_data[company_data['form_type'] == '10-K']

    # Extract the ticker symbol from the file_name column
    company_data['ticker'] = company_data['file_name'].str.extract(r'-(\w+)-')[0]

    # Convert the cik column to a string and fill it with leading zeros so that it is always 10 characters long
    company_data['cik'] = company_data['cik'].apply(lambda x: str(x).zfill(10))

    # Drop unnecessary columns
    company_data = company_data.drop(['form_type', 'date_filed', 'file_name'], axis=1)

    # Drop any rows with missing data
    company_data = company_data[pd.notna(company_data).all(axis=1)]

    return company_data
