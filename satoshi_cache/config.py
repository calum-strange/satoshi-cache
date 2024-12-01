from pathlib import Path
from typing import Literal, get_args


CACHE_LOCATION = Path("./satoshi_cache")  # your cache location

ExchangeType = Literal["binance"]
available_exchanges = get_args(ExchangeType)

BinanceProductType = Literal["spot", "um", "cm"]
available_binance_products = get_args(BinanceProductType)

BinanceFileType = Literal[
    "trades",
    "aggTrades",
    "klines",
    "indexPriceKlines",
    "markPriceKlines",
    "premiumPriceKlines",
]


def get_cache_location(
    exchange: ExchangeType,
    product_type: BinanceProductType,
    date: int,
    symbol: str,
    file_type: BinanceFileType,
) -> Path:
    return (
        Path(CACHE_LOCATION)
        / exchange
        / product_type
        / str(date)
        / symbol
        / (file_type + ".parquet")
    )
