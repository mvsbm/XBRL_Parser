import os
import zipfile
import requests
import pandas as pd
from pathlib import Path
import sqlite3
from tqdm import tqdm
import time
from threading import Lock, Semaphore

headers = {'User-Agent': "sungbinma@gmail.com"}

# Rate limiter implementation using Semaphores
class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.semaphore = Semaphore(max_calls)
        self.lock = Lock()

    def __enter__(self):
        self.semaphore.acquire()
        self.lock.acquire()
        self.timer = time.time()
        self.lock.release()

    def __exit__(self, *args):
        elapsed = time.time() - self.timer
        if elapsed < self.period:
            time.sleep(self.period - elapsed)
        self.semaphore.release()

def read_company_data_from_csv(file_name):
    return pd.read_csv(file_name, dtype={'cik_str': str})

# Create a rate limiter object with a limit of 10 requests per second
sec_rate_limiter = RateLimiter(max_calls=10, period=1)

def get_filings_list(cik):
    with sec_rate_limiter:
        filings_url = f'https://data.sec.gov/submissions/CIK{cik}.json'
        metadata = requests.get(filings_url, headers=headers)
        all_forms = pd.DataFrame.from_dict(metadata.json()['filings']['recent'])
        return all_forms[all_forms['form'].str.lower().isin(['10-k', '10-q', '20-f', '40-f'])]

def download_and_unzip_filings(cik, ticker, filings, cursor, progress_bar):
    base_url = 'https://www.sec.gov/Archives/edgar/data/'
    filings_path = Path('D:/Filings') / ticker
    filings_path.mkdir(parents=True, exist_ok=True)

    for _, filing in progress_bar(filings.iterrows(), total=len(filings), desc=f"Processing {ticker} (CIK: {cik})"):
        form_type = filing['form'].lower()
        form_dir = filings_path / form_type
        form_dir.mkdir(exist_ok=True)

        accession_number = filing['accessionNumber']

        # Check if the filing already exists in the database
        cursor.execute("""
            SELECT COUNT(*) FROM filings WHERE accession_number = ?
        """, (accession_number,))
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"Filing {accession_number} already downloaded, skipping...")
            continue

        xbrl_zip_url = f'{base_url}{cik}/{accession_number.replace("-", "")}/{accession_number}-xbrl.zip'

        # Download the file with retries
        max_retries = 1
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(xbrl_zip_url, headers=headers)
                if response.status_code == 200:
                    break
                else:
                    print(f"Failed to download {form_type.upper()} filing: {accession_number}, retrying...")
                    retries += 1
                    time.sleep(0.05)  # Wait for 2 seconds before retrying
            except requests.exceptions.RequestException as e:
                print(f"Error downloading {form_type.upper()} filing: {accession_number}, retrying...")
                retries += 1
                time.sleep(0.05)  # Wait for 2 seconds before retrying
        if retries == max_retries:
            print(f"Failed to download {form_type.upper()} filing: {accession_number} after {max_retries} retries.")
            continue

        zip_filename = form_dir / f'{accession_number}-xbrl.zip'
        with open(zip_filename, 'wb') as file:
            file.write(response.content)
            print(f"Downloaded {form_type.upper()} filing: {accession_number}")

        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            report_date = pd.to_datetime(filing['reportDate']).strftime('%Y-%m-%d')
            for file in zip_ref.namelist():
                if file.lower().endswith('.xml'):
                    new_filename = f"[{form_type.upper()}][{report_date}][{ticker}].xbrl"
                    zip_ref.extract(file, form_dir)
                    existing_file_path = form_dir / new_filename
                    new_file_path = form_dir / file
                    if existing_file_path.exists():
                        os.remove(existing_file_path)
                    os.rename(new_file_path, existing_file_path)

        os.remove(zip_filename)

        # Insert metadata into the database
        cursor.execute("""
            INSERT INTO filings (ticker, cik, form_type, report_date, accession_number, file_path)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (ticker, cik, form_type, report_date, accession_number, str(form_dir / new_filename)))

def create_database():
    conn = sqlite3.connect('filings.db')
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS filings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            cik TEXT NOT NULL,
            form_type TEXT NOT NULL,
            report_date TEXT NOT NULL,
            accession_number TEXT NOT NULL,
            file_path TEXT NOT NULL
        )
    """)

    return conn, cursor

def main():
    # Read company data from CSV
    company_data = read_company_data_from_csv('company_tickers.csv')

    # Create a database to store filing metadata
    conn, cursor = create_database()

    # Get the last downloaded filing's ticker and CIK
    cursor.execute("""
        SELECT ticker, cik FROM filings ORDER BY id DESC LIMIT 1
    """)
    last_downloaded_filing = cursor.fetchone()
    if last_downloaded_filing:
        ticker_index = company_data[company_data['ticker'] == last_downloaded_filing[0]].index[0]
        company_data = company_data.loc[ticker_index:]

    for _, row in tqdm(company_data.iterrows(), total=len(company_data)):
        ticker = row['ticker']
        cik = row['cik_str']
        print(f"Processing {ticker} (CIK: {cik})")
        try:
            filings_list = get_filings_list(cik)
            download_and_unzip_filings(cik, ticker, filings_list, cursor, progress_bar=tqdm)
            conn.commit()
        except Exception as e:
            print(f"Error processing {ticker} (CIK: {cik}): {e}")

    conn.close()

if __name__ == "__main__":
    main()