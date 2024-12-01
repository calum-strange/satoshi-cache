from satoshi_cache.config import BinanceFileType, BinanceProductType

agg_trade_columns = [
    "tradeId",
    "price",
    "quantity",
    "first_tradeId",
    "last_tradeId",
    "timestamp",
    "maker_bought",
    "is_best_match",
]

klines_columns = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]

trade_columns = [
    "tradeId",
    "price",
    "quantity",
    "first_tradeId",
    "last_tradeId",
    "timestamp",
    "maker_bought",
    "is_best_match",
]

usd_m_klines_columns = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]

coin_m_klines_columns = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "base_asset_volume",
    "number_of_trades",
    "taker_buy_quote_asset_volume",
    "taker_buy_base_asset_volume",
    "ignore",
]

usd_m_trade_columns = [
    "tradeId",
    "price",
    "quantity",
    "quote_asset_quantity",
    "timestamp",
    "maker_bought",
]

coin_m_trade_columns = [
    "tradeId",
    "price",
    "quantity",
    "base_asset_quantity",
    "timestamp",
    "maker_bought",
]


def get_binance_file_columns(
    trading_type: BinanceProductType, market_data_type: BinanceFileType
):
    if market_data_type == "aggTrades":
        return agg_trade_columns
    elif market_data_type == "trades":
        if trading_type == "spot":
            return trade_columns
        elif trading_type == "um":
            return usd_m_trade_columns
        elif trading_type == "cm":
            return coin_m_trade_columns
    elif market_data_type == "klines":
        if trading_type == "spot":
            return klines_columns
        elif trading_type == "um":
            return usd_m_klines_columns
        elif trading_type == "cm":
            return coin_m_klines_columns
