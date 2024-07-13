
class Symbol:
    def __init__(self, name: str, step: str, api: str, integrity: bool):
        self.name: str = name
        self.step: str = step
        self.api: str = api
        self.integrity: bool = integrity

    def __repr__(self):
        return f"Symbol(name={self.name}, step={self.step}, api={self.api}, integrity={self.integrity})"