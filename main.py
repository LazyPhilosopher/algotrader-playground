from secret import api_key, secret_key 
import sqlite3
from sqlite.db_controller import DBController
from models.stock import Stock
# from binance.client import Client
# import yfinance as yf


# client = Client(api_key, secret_key)

binance_tickers = [
    # Cryptocurrencies
    'BTCUSDT',  # Bitcoin vs USDT
    'ETHUSDT',  # Ethereum vs USDT
    'BNBUSDT',  # Binance Coin vs USDT
    'ADAUSDT',  # Cardano vs USDT
    'SOLUSDT',  # Solana vs USDT
    'XRPUSDT',  # XRP vs USDT
    'DOTUSDT',  # Polkadot vs USDT
    'DOGEUSDT', # Dogecoin vs USDT
    'AVAXUSDT', # Avalanche vs USDT
    'LINKUSDT', # Chainlink vs USDT
    'MATICUSDT',# Polygon vs USDT
    'LTCUSDT',  # Litecoin vs USDT
    'BCHUSDT',  # Bitcoin Cash vs USDT
    'ATOMUSDT', # Cosmos vs USDT
    'ALGOUSDT', # Algorand vs USDT
    'XLMUSDT',  # Stellar vs USDT
    'VETUSDT',  # VeChain vs USDT
    'ETCUSDT',  # Ethereum Classic vs USDT
    'THETAUSDT',# THETA vs USDT
    'XTZUSDT',  # Tezos vs USDT
]

controller = DBController("storage")
controller.append_stock_database("BNBUSDT", "storage/stocks")
controller.append_stock_database("BTCUSDT", "storage/stocks")
controller.create_stock_price_database("BNBUSDT")
controller.create_stock_price_database("BTCUSDT")
controller.parse_csv_data_into_db("data/binance-stablecoin/BNBUSDT.csv", "storage/symbols/BNBUSDT.db")
# controller.load_csv_to_db("data/binance-stablecoin/BNBUSDT.csv")
controller.print_stock_data()
