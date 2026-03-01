from ib_async import IB, Stock

def main(
    symbol: str = "TSLA",
    exchange: str = "NASDAQ",
    host: str = "host.docker.internal",
    port: int = 4002,
):
    ib = IB()
    try:
        ib.connect(host, port, clientId=1)
        
        contract = Stock(symbol, exchange, "USD")
        ib.qualifyContracts(contract)
        
        return {
            "symbol": contract.symbol,
            "conid": contract.conId,
            "exchange": contract.exchange,
            "currency": contract.currency,
            "secType": contract.secType,
        }
    finally:
        ib.disconnect()
