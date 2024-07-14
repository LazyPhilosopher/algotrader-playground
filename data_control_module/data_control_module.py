import json
import os
import sqlite3
import time
from typing import List

from PySide6.QtCore import QThread, Signal
from binance.client import Client

from data_control_module.Symbol import Symbol
from data_control_module.helper import get_binance_historical_klines_zip_links, download_zip_and_convert_to_csv, \
    get_current_month_days_count, merge_into_single_csv, store_binance_ticker_to_csv
from secret import api_key, secret_key


data_dir = os.path.join("storage", "data")
# buffer_data_dir = os.path.join(data_dir, "buffer")
# merged_data_dir = os.path.join(data_dir, "merged")
# mysql_data_dir = os.path.join(data_dir, "mysql")


class Worker(QThread):
    update_signal = Signal(int)  # Signal to communicate with the main thread

    def __init__(self, count_from):
        super().__init__()
        self.count_from = count_from

        with open('data_control_module/config.json', 'r') as file:
            data = json.load(file)
        self.symbols_list: list[Symbol] = [Symbol(item['key'], item['step'], item['api'], item['integrity'], item["type"]) for item in data['symbols']]
        print(self.symbols_list)
        self.perform_integrity_check()

    def perform_integrity_check(self):

        for symbol in self.symbols_list:
            self.download_missing_csv(symbol)
            mysql_data_dir = os.path.join(data_dir, symbol.type, "mysql")
            if not os.path.exists(mysql_data_dir):
                os.makedirs(mysql_data_dir)
            sql_file_path: str = os.path.join(mysql_data_dir, symbol.name+".sqlite")
            if not os.path.isfile(sql_file_path):
                connection = sqlite3.connect(sql_file_path)
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

    def download_missing_csv(self, symbol: Symbol):
        kline_zip_links: List[str] = get_binance_historical_klines_zip_links(symbol.name)
        buffer_dir = os.path.join(data_dir, symbol.type, "buffered", symbol.name)
        for zip_link in kline_zip_links:
            file_name = os.path.basename(zip_link)
            file_root, _ = os.path.splitext(file_name)
            csv_path = os.path.join(buffer_dir, f"{file_root}.csv")
            if not os.path.isfile(csv_path):
                download_zip_and_convert_to_csv(archive_link=zip_link,
                                                download_dir=buffer_dir)
            else:
                print(f"{csv_path} already exists.")

        # Get current month ticker data and store into CSV
        client = Client(api_key=api_key, api_secret=secret_key, tld="com")
        store_binance_ticker_to_csv(ticker_name=symbol.name,
                                    download_dir=buffer_dir,
                                    client=client,
                                    interval="1m",
                                    days=get_current_month_days_count())

        # Merge all together
        merge_into_single_csv(csv_directory=buffer_dir,
                              output_directory=os.path.join(data_dir,
                                                            symbol.type,
                                                            "merged",
                                                            symbol.name))
