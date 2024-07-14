from enum import Enum


class SymbolType(Enum):
    stablecoin = "binance-stablecoin"
    ticker = "binance-ticker"


class Symbol:
    def __init__(self, name: str, step: str, api: str, integrity: bool, type: SymbolType):
        self.name: str = name
        self.step: str = step
        self.api: str = api
        self.integrity: bool = integrity
        self.type: str = str(type)

    def __repr__(self):
        return f"Symbol(name={self.name}, step={self.step}, api={self.api}, integrity={self.integrity}, type={self.type})"