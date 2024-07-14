import json
import os
import sqlite3
import time

from PySide6.QtCore import QThread, Signal
from binance.client import Client

from data_control_module.Symbol import Symbol, API
from data_control_module.helper import download_missing_binance_csv, create_kline_mysql, parse_csv_data_to_mysql
from secret import api_key, secret_key
import yfinance as yf


data_dir = os.path.join("storage", "data")


def download_missing_csv(symbol: Symbol):
    if symbol.api == API.binance.value:
        client = Client(api_key=api_key, api_secret=secret_key, tld="com")
        download_missing_binance_csv(client=client, symbol=symbol, data_dir=data_dir)
    elif symbol.api == API.yahoo_finance.value:
        df = yf.download(symbol.name, period="1d")
        download_path = os.path.join(data_dir, symbol.type, "merged")
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        df.to_csv(os.path.join(download_path, symbol.name+".csv"))
        pass


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
            download_missing_csv(symbol)
            mysql_data_dir = os.path.join(data_dir, symbol.type, "mysql")
            if not os.path.exists(mysql_data_dir):
                os.makedirs(mysql_data_dir)
            sql_file_path: str = os.path.join(mysql_data_dir, symbol.name+".sqlite")
            if not os.path.isfile(sql_file_path):
                create_kline_mysql(sql_file_path)
            parse_csv_data_to_mysql(os.path.join(data_dir, symbol.type, "merged", symbol.name+".csv"), sql_file_path)


    def run(self):
        for i in range(self.count_from, -1, -1):
            self.update_signal.emit(i)
            time.sleep(1)


