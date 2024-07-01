import os
import sqlite3
import csv 
from datetime import datetime
from models.stock import Stock


database_catalog_struct: str = '''
CREATE TABLE IF NOT EXISTS database_catalog (
                Id INTEGER PRIMARY KEY, 
                Name TEXT NOT NULL,
                Path TEXT NOT NULL,
                StepMinutes REAL NOT NULL,
                IsComplete INTEGER NOT NULL
            )
'''

symbol_database_struct: str = '''
        CREATE TABLE IF NOT EXISTS stock_prices (
            Date TIMESTAMP NOT NULL,
            High REAL NOT NULL,
            Low REAL NOT NULL,
            Close REAL NOT NULL,
            Volume REAL NOT NULL
        )
    '''


class DBController:

    symbol_name: str
    catalog_connector: sqlite3.Connection
    cursor: sqlite3.Cursor

    def __init__(self, repository_dir: str):
        # create database directofy if not exist
        os.makedirs(repository_dir, exist_ok=True)

        self.catalog_connector = sqlite3.connect(os.path.join(repository_dir, "database_catalog.db"))
        self.cursor = self.catalog_connector.cursor()
        self.cursor.execute(database_catalog_struct)

    def append_stock_database(self, stock_name: str, path: str):
        self.cursor.execute('''
            INSERT INTO database_catalog (Name, Path, StepMinutes, IsComplete) VALUES (?, ?, ?, ?)
        ''', (stock_name, path, 1, 0))
        self.catalog_connector.commit()

    def create_stock_price_database(self, symbol_name: str):
        os.makedirs("storage/symbols", exist_ok=True)
        connector = sqlite3.connect(os.path.join("storage/symbols", symbol_name+'.db'))
        cur = connector.cursor()

        cur.execute('''
            CREATE TABLE IF NOT EXISTS stock_prices (
                Date TIMESTAMP NOT NULL,
                High REAL NOT NULL,
                Low REAL NOT NULL,
                Close REAL NOT NULL,
                Volume REAL NOT NULL
            )
        ''')
        cur.close()
        connector.close()

    def parse_csv_data_into_db(self, csv_path: str, db_path: str):
        # db_connector = sqlite3.connect(db_path)
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Parse the datetime string into a datetime object
                # dt = datetime.strptime(row['Date'], '%Y-%m-%d %H:%M:%S')
                # Convert the datetime object to a Unix timestamp
                # row['Date'] = int(dt.timestamp())
                self.insert_stock_price(row)

    def insert_stock_price(self, row: dict):
        timestamp: datetime = datetime.strptime(row['Date'], '%Y-%m-%d %H:%M:%S')
        self.cursor.execute('''
            INSERT INTO stock_prices (Date, High, Low, Close, Volume) VALUES (?, ?, ?, ?, ?)
        ''', (timestamp, row['High'], row['Low'], row['Close'], row['Volume']))
        self.catalog_connector.commit()

    # def insert_stock_price(self, stock: Stock):
    #     self.cursor.execute('''
    #         INSERT INTO stock_prices (Date, High, Low, Close, Volume) VALUES (?, ?, ?, ?, ?)
    #     ''', (stock.date, stock.high, stock.low, stock.close, stock.volume))
    #     self.catalog_connector.commit()
        
    def print_stock_data(self):
        self.cursor.execute('SELECT * FROM database_catalog')
        rows = self.cursor.fetchall()

        # Print the results
        for row in rows:
            print(row)
