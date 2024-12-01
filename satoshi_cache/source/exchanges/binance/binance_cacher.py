import logging
import os
import zipfile
from pathlib import Path

import polars as pl

from satoshi_cache.config import (
    BinanceFileType,
    BinanceProductType,
    get_cache_location,
    DOWLOAD_LOCATION,
)
from satoshi_cache.source.exchanges.binance.binance_public_data.download_aggTrade import (
    download_daily_aggTrades,
)
from satoshi_cache.source.exchanges.binance.binance_public_data.download_futures_indexPriceKlines import (
    download_daily_indexPriceKlines,
)
from satoshi_cache.source.exchanges.binance.binance_public_data.download_futures_markPriceKlines import (
    download_daily_markPriceKlines,
)
from satoshi_cache.source.exchanges.binance.binance_public_data.download_futures_premiumIndexKlines import (
    download_daily_premiumIndexKlines,
)
from satoshi_cache.source.exchanges.binance.binance_public_data.download_kline import (
    download_daily_klines,
)
from satoshi_cache.source.exchanges.binance.binance_public_data.download_trade import (
    download_daily_trades,
)
from satoshi_cache.source.exchanges.binance.binance_public_data.utility import get_path
from satoshi_cache.source.exchanges.binance.file_columns import get_binance_file_columns


class BinanceCacher:
    def __init__(
        self,
        trading_type: BinanceProductType,
        file_type: BinanceFileType,
        kline_intervals: list[str] = ["1m"],
        logger=None,
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._trading_type = trading_type
        self._file_type = file_type
        self._kline_intervals = kline_intervals

    def build_cache(
        self,
        symbols: list[str],
        dates: list[str],
        overwrite: bool = False,
    ) -> None:
        for symbol in symbols:
            for date in dates:
                cache_exists = get_cache_location(
                    exchange="binance",
                    product_type=self._trading_type,
                    date=date,
                    symbol=symbol,
                    file_type=self._file_type,
                ).is_file()

                if cache_exists and not overwrite:
                    self._logger.info(f"Cache for {symbol} on {date} already exists")
                    continue

                self._download_binance_data(symbol=symbol, date=date)
                df = self._load_zipped_file_to_df(symbol=symbol, date=date)
                self._save_to_cache(df=df, symbol=symbol, date=date)
                self._remove_binance_cache(symbol=symbol, date=date)

    def _download_binance_data(self, symbol: str, date: str) -> None:
        self._logger.info(f"Downloading data for {symbol} on {date}")
        if self._file_type == "aggTrades":
            download_daily_aggTrades(
                trading_type=self._trading_type,
                symbols=[symbol],
                num_symbols=1,
                dates=[date],
                start_date=None,
                end_date=None,
                folder=None,
                checksum=1,
            )

        if self._file_type == "trades":
            download_daily_trades(
                trading_type=self._trading_type,
                symbols=[symbol],
                num_symbols=1,
                dates=[date],
                start_date=None,
                end_date=None,
                folder=None,
                checksum=1,
            )

        if self._file_type == "klines":
            download_daily_klines(
                trading_type=self._trading_type,
                symbols=[symbol],
                num_symbols=1,
                intervals=self._kline_intervals,
                dates=[date],
                start_date=None,
                end_date=None,
                folder=None,
                checksum=1,
            )

        if self._file_type == "markPriceKlines":
            download_daily_markPriceKlines(
                trading_type=self._trading_type,
                symbols=[symbol],
                num_symbols=1,
                intervals=self._kline_intervals,
                dates=[date],
                start_date=None,
                end_date=None,
                folder=None,
                checksum=1,
            )

        if self._file_type == "indexPriceKlines":
            download_daily_indexPriceKlines(
                trading_type=self._trading_type,
                symbols=[symbol],
                num_symbols=1,
                intervals=self._kline_intervals,
                dates=[date],
                start_date=None,
                end_date=None,
                folder=None,
                checksum=1,
            )

        if self._file_type == "premiumPriceKlines":
            download_daily_premiumIndexKlines(
                trading_type=self._trading_type,
                symbols=[symbol],
                num_symbols=1,
                intervals=self._kline_intervals,
                dates=[date],
                start_date=None,
                end_date=None,
                folder=None,
                checksum=1,
            )

    def _save_to_cache(self, df: pl.DataFrame, symbol: str, date: str):
        parquet_path = get_cache_location(
            exchange="binance",
            product_type=self._trading_type,
            date=date,
            symbol=symbol,
            file_type=self._file_type,
        )
        self._logger.info(f"Writing to {parquet_path}")
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(parquet_path)

    def _load_zipped_file_to_df(self, symbol: str, date: str) -> pl.DataFrame:
        binance_cache_path = self._get_binance_output_location(symbol=symbol, date=date)
        self._logger.info(f"Loading {binance_cache_path} to df")
        file_columns = get_binance_file_columns(
            trading_type=self._trading_type, market_data_type=self._file_type
        )

        with zipfile.ZipFile(binance_cache_path, "r") as zip_ref:
            file_name = zip_ref.namelist()[0]
            with zip_ref.open(file_name) as file:
                df = pl.read_csv(file, has_header=False, new_columns=file_columns)

        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime(time_unit="ms")))

        return df

    def _remove_binance_cache(self, symbol: str, date: str):
        binance_cache_path = self._get_binance_output_location(symbol=symbol, date=date)
        self._logger.info(f"Removing {binance_cache_path}")
        try:
            os.remove(binance_cache_path)
        except OSError:
            self._logger.info(f"{binance_cache_path} does not exist")

    def _get_binance_output_location(self, symbol: str, date: str) -> Path:
        return (
            DOWLOAD_LOCATION
            / get_path(
                trading_type=self._trading_type,
                market_data_type=self._file_type,
                time_period="daily",
                symbol=symbol,
            )
            / f"{symbol.upper()}-{self._file_type}-{date}.zip"
        )
