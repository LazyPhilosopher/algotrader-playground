from enum import Enum


class API(Enum):
    binance = "binance"
    yahoo_finance = "yahoo_finance"


class SymbolType(Enum):
    stablecoin = "binance-stablecoin"
    ticker = "binance-ticker"


class Symbol:
    def __init__(self, name: str, step: str, api: API, integrity: bool, symbol_type: SymbolType):
        self.name: str = name
        self.step: str = step
        self.api: API = api
        self.integrity: bool = integrity
        self.type: str = str(symbol_type)

    def __repr__(self):
        return f"Symbol(name={self.name}, step={self.step}, api={self.api}, integrity={self.integrity}, type={self.type})"