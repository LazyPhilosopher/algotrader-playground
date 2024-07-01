import os
import sqlite3
import csv
from ..models.stock import Stock
from datetime import datetime


DB_DIR: str = "storage"


class DBController:
    symbol_name: str
    connector: sqlite3.Connection
    cursor: sqlite3.Cursor

    def __init__(self, symbol_name: str):
        self.symbol_name = symbol_name

        os.makedirs(DB_DIR, exist_ok=True)
        self.connector = sqlite3.connect(os.path.join(DB_DIR, symbol_name+'.db'))
        self.cur = self.conn.cursor()

        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS stock_prices (
                Date TIMESTAMP NOT NULL,
                High REAL NOT NULL,
                Low REAL NOT NULL,
                Close REAL NOT NULL,
                Volume REAL NOT NULL
            )
        ''')

    def get_cursor(self):
        return self.cursor
    
    def insert_stock_price(self, stock: Stock):
        self.cur.execute('''
            INSERT INTO stock_prices (Date, High, Low, Close, Volume) VALUES (?, ?, ?, ?, ?)
        ''', (stock.date, stock.high, stock.low, stock.close, stock.volume))
        self.conn.commit()

    def insert_stock_price(self, row: str):
        timestamp: datetime = datetime.strptime(row['Date'], '%Y-%m-%d %H:%M:%S')
        self.cur.execute('''
            INSERT INTO stock_prices (Date, High, Low, Close, Volume) VALUES (?, ?, ?, ?, ?)
        ''', (timestamp, row['High'], row['Low'], row['Close'], row['Volume']))
        self.conn.commit()

    def print_stock_data(self):
        self.cur.execute('SELECT * FROM stock_prices')
        rows = self.cur.fetchall()

        # Print the results
        for row in rows:
            print(row)

    def load_csv_to_db(self, csv_file_path):
        with open(csv_file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.insert_stock_price(row=row)
                print(row)


