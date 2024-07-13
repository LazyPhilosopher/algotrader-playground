import json
import os
import sqlite3
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import pandas as pd
import requests
from PySide6.QtCore import QThread, Signal
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from Symbol import Symbol


class Worker(QThread):
    update_signal = Signal(int)  # Signal to communicate with the main thread

    def __init__(self, count_from):
        super().__init__()
        self.count_from = count_from

        with open('data_control_module/config.json', 'r') as file:
            data = json.load(file)
        self.symbols_list = [Symbol(item['key'], item['step'], item['api'], item['integrity']) for item in data['symbols']]
        print(self.symbols_list)
        self.perform_integrity_check()

    def perform_integrity_check(self):
        directory = os.path.join("storage", "symbols")
        all_db_files = os.listdir(directory)
        sqlite_files = [file for file in all_db_files if file.endswith(('.sqlite', '.db', '.sqlite3'))]

        for symbol in self.symbols_list:
            db_file_path = os.path.join(directory, f"{symbol.name}.sqlite")
            if os.path.isfile(db_file_path):
                print(f"{symbol}.sqlite")
            else:
                connection = sqlite3.connect(db_file_path)
                cursor = connection.cursor()
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_data (
                    date TEXT,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL)
                ''')

                # Commit the changes and close the connection
                connection.commit()
                connection.close()

    def run(self):
        for i in range(self.count_from, -1, -1):
            self.update_signal.emit(i)
            time.sleep(1)


def get_binance_historical_klines_archive_links(symbol: str) -> List[str]:
    driver = webdriver.Chrome()

    # Open the URL
    url = f"https://data.binance.vision/?prefix=data/spot/monthly/klines/{symbol}/1m/"
    driver.get(url)

    # Wait for the first link to appear (with a timeout of 10 seconds)
    try:
        element_present = EC.presence_of_element_located((By.XPATH, '//tbody[@id="listing"]/tr'))
        WebDriverWait(driver, 10).until(element_present)
    except TimeoutException:
        print("Timed out waiting for page to load")
    finally:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Find all the links that end with '.csv'
        zip_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.endswith('.zip'):
                zip_links.append(href)

        # Close the browser
        driver.quit()
    return zip_links

def download_archive_and_convert_to_csv(archive_link: str) -> None:
    file_name = archive_link.split("/")[-1]
    file_name_without_ext = Path(file_name).stem
    ticker_dir = os.path.join(download_dir, file_name_without_ext.split('-')[0])

    zip_path = os.path.join(ticker_dir, file_name)
    csv_path = os.path.join(ticker_dir, file_name_without_ext + ".csv")
    print(f"Downloading {file_name}...")

    # Download the file
    response = requests.get(archive_link)
    response.raise_for_status()  # Ensure we notice bad responses

    if not os.path.exists(ticker_dir):
        os.makedirs(ticker_dir)

    # Save the ZIP file to the specified directory
    with open(zip_path, 'wb') as file:
        file.write(response.content)
    print(f"{file_name} downloaded and saved to {zip_path}")

    # Extract the ZIP file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(ticker_dir)
    print(f"{file_name} extracted to {csv_path}")

    # Delete the ZIP file
    os.remove(zip_path)
    print(f"{file_name} deleted from {zip_path}")

def merge_into_single_csv(csv_directory: str, output_directory: str) -> None:
    # Define the directory containing CSV files and the output file name
    symbol_name = Path(csv_directory).stem
    output_file = os.path.join(output_directory, symbol_name + ".csv")

    # List all CSV files in the directory
    csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]

    # Initialize an empty list to hold DataFrames
    dataframes = []

    # Iterate through the list of CSV files and read each file into a DataFrame
    for csv_file in csv_files:
        file_path = os.path.join(csv_directory, csv_file)
        print(f"Reading {file_path}...")
        df = pd.read_csv(file_path)
        df.columns = ["Open Time", "Open", "High", "Low", "Close",
                      "Volume", "Close Time", "Quote Asset Volume", "Number of Trades",
                      "Taker Buy Base Asset Volume",
                      "Taker Buy Quote Asset Volume", "Ignore"]
        # Convert 'Open Time' to datetime
        df["Date"] = pd.to_datetime(df["Open Time"], unit="ms")
        df.drop(columns=["Open Time"], inplace=True)

        # Set 'Date' as the index
        df.set_index("Date", inplace=True)
        dataframes.append(df)

    # Concatenate all DataFrames into a single DataFrame
    merged_df = pd.concat(dataframes, axis=0)

    # Select and reorder columns
    merged_df = merged_df[["High", "Low", "Close", "Volume"]].copy()

    # Save the merged DataFrame to a new CSV file
    merged_df.to_csv(output_file)
    print(f"Merged CSV saved to {output_file}")

def store_binance_ticker(ticker_name: str, interval: str, days: str) -> None:
    now = datetime.now(timezone.utc)
    past = str(now - timedelta(days=days))

    bars = client.get_historical_klines(symbol=ticker_name, interval=interval, start_str=past, end_str=None,
                                        limit=1000)
    df = pd.DataFrame(bars)
    print(df.columns)
    # df["Date"] = pd.to_datetime(df.iloc[:,0], unit = "ms")
    df.columns = ["Open Time", "Open", "High", "Low", "Close",
                  "Volume", "Close Time", "Quote Asset Volume", "Number of Trades", "Taker Buy Base Asset Volume",
                  "Taker Buy Quote Asset Volume", "Ignore"]
    # df.drop(columns=["Open"], inplace=True)
    df.set_index("Open Time", inplace=True)
    df.to_csv(os.path.join(download_dir, ticker_name, ticker_name + "_current_month.csv"), index=True)
