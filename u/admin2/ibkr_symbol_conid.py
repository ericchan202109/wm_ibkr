from ib_async import IB, Stock


def main(
    symbol: str = "TSLA",
    exchange: str = "NASDAQ",
    host: str = "host.docker.internal",
    port: int = 4002,
    client_id: int = 7,
):
    resolved_symbol = str(symbol or "").strip().upper()
    if not resolved_symbol:
        raise ValueError("symbol is required")

    resolved_exchange = str(exchange or "NASDAQ").strip().upper() or "NASDAQ"

    ib = IB()
    try:
        ib.connect(host, int(port), clientId=int(client_id))

        contract = Stock(resolved_symbol, resolved_exchange, "USD")
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
