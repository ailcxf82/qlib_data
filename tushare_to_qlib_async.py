#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tushare data fetcher and Qlib converter (async version)

Features:
1. Async IO for fetching data from Tushare (technical factors, money flow, margin trading, financial indicators)
2. Convert and save to Qlib standard format (.bin files)
3. Save to specified directory

Usage:
    python tushare_to_qlib_async.py --start_date 20200101 --end_date 20260511
    Or set TUSHARE_API_KEY environment variable before running
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import deque
from datetime import datetime, timedelta

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

QLIB_STD_OHLCV_FROM: Dict[str, str] = {
    "open": "open_qfq",
    "high": "high_qfq",
    "low": "low_qfq",
    "close": "close_qfq",
    "volume": "volume_ratio",
}


def add_qlib_standard_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    for target, source in QLIB_STD_OHLCV_FROM.items():
        if source in df.columns:
            df[target] = df[source]
    return df


class AsyncAPIRateLimiter:
    """Async Tushare API rate limiter with dynamic adjustment and retry mechanism"""

    RATE_LIMITS = {
        'stk_factor_pro': {'normal': 30, 'high': 500, 'current': 30},
        'moneyflow_dc': {'normal': 30, 'high': 100, 'current': 30},
        'margin_detail': {'normal': 30, 'high': 60, 'current': 30},
        'fina_indicator': {'normal': 20, 'high': 40, 'current': 20},
        'stock_basic': {'normal': 60, 'high': 120, 'current': 60}
    }

    MAX_RETRIES = 3
    RETRY_DELAYS = [1, 2, 4]

    def __init__(self, high_speed: bool = False, max_concurrent: int = 10):
        self.high_speed = high_speed
        self.request_history: Dict[str, deque] = {}
        self.mode = 'high' if high_speed else 'normal'
        self._api_locks: Dict[str, asyncio.Lock] = {}
        self.semaphore = asyncio.Semaphore(max_concurrent)

        for api_name in self.RATE_LIMITS.keys():
            self.request_history[api_name] = deque(maxlen=1000)
            self._api_locks[api_name] = asyncio.Lock()
            self.RATE_LIMITS[api_name]['current'] = self.RATE_LIMITS[api_name][self.mode]

        logger.info(f"Async API rate limiter initialized (mode: {self.mode}, max_concurrent: {max_concurrent})")

    def _get_interval(self, api_name: str) -> float:
        if api_name not in self.RATE_LIMITS:
            return 2.0
        rate_per_minute = self.RATE_LIMITS[api_name]['current']
        return (60.0 / rate_per_minute) * 1.1

    async def wait_if_needed(self, api_name: str):
        if api_name not in self.request_history:
            return
        interval = self._get_interval(api_name)
        history = self.request_history[api_name]
        if len(history) > 0:
            elapsed = time.time() - history[-1]
            if elapsed < interval:
                await asyncio.sleep(interval - elapsed)

    def record_request(self, api_name: str):
        if api_name in self.request_history:
            self.request_history[api_name].append(time.time())

    async def execute_with_rate_limit(self, api_name: str, func, *args, **kwargs):
        last_exception = None
        for attempt in range(self.MAX_RETRIES):
            try:
                async with self._api_locks.get(api_name) or asyncio.Lock():
                    await self.wait_if_needed(api_name)
                    self.record_request(api_name)
                async with self.semaphore:
                    result = await asyncio.to_thread(func, *args, **kwargs)
                return result
            except Exception as e:
                last_exception = e
                error_msg = str(e).lower()
                is_rate_limit_error = any(k in error_msg for k in ['limit', 'too many', 'freq', 'rate', 'quick'])
                delay = self.RETRY_DELAYS[min(attempt, len(self.RETRY_DELAYS)-1)]
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(delay)
                    if is_rate_limit_error and api_name in self.RATE_LIMITS:
                        self.RATE_LIMITS[api_name]['current'] = max(5, self.RATE_LIMITS[api_name]['current'] * 0.7)
        raise last_exception

    def get_stats(self) -> Dict:
        stats = {}
        now = time.time()
        for api_name, history in self.request_history.items():
            recent_count = sum(1 for t in history if now - t <= 60)
            rate_limit = self.RATE_LIMITS[api_name]['current']
            stats[api_name] = {
                'recent_requests': recent_count,
                'rate_limit': rate_limit,
                'utilization': recent_count / rate_limit * 100 if rate_limit > 0 else 0
            }
        return stats


def get_tushare_token() -> Optional[str]:
    token = os.environ.get('TUSHARE_TOKEN', '').strip()
    if token:
        return token
    try:
        import yaml
        config_path = Path('config/secrets.yaml')
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                if data and 'tushare' in data:
                    token = data['tushare'].get('token', '').strip()
                    if token:
                        return token
    except Exception:
        pass
    return None


class AsyncTushareDataFetcher:
    """Async Tushare data fetcher with rate limiting"""

    def __init__(self, token: str, cache_dir: str = "data/tushare_cache", rate_limiter=None):
        self.token = token
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limiter = rate_limiter
        try:
            import tushare as ts
            self.ts = ts
            self.pro = ts.pro_api(token)
            logger.info("Tushare Pro API initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Tushare: {e}")
            raise

    def _load_cache(self, name: str) -> Optional[pd.DataFrame]:
        p = self.cache_dir / name
        if p.exists():
            try:
                return pd.read_parquet(p)
            except Exception:
                try:
                    return pd.read_csv(p)
                except Exception:
                    pass
        return None

    def _save_cache(self, name: str, df: pd.DataFrame):
        p = self.cache_dir / name
        try:
            df.to_parquet(p, index=False)
        except Exception:
            df.to_csv(p.with_suffix('.csv'), index=False)

    async def get_stock_list(self) -> pd.DataFrame:
        cached = self._load_cache("stock_list.parquet")
        if cached is not None and len(cached) > 0:
            return cached

        def _fetch():
            return self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

        if self.rate_limiter:
            df = await self.rate_limiter.execute_with_rate_limit('stock_basic', _fetch)
        else:
            df = await asyncio.to_thread(_fetch)

        if df is not None and len(df) > 0:
            self._save_cache("stock_list.parquet", df)
        return df if df is not None else pd.DataFrame()

    async def _fetch_api(self, api_name: str, ts_code: str, start_date: str, end_date: str, fields: List[str], cache_prefix: str):
        cache_name = f"{cache_prefix}_{ts_code}_{start_date}_{end_date}.parquet"
        cached = self._load_cache(cache_name)
        if cached is not None and len(cached) > 0:
            return cached

        field_str = ','.join(fields)
        api_func = getattr(self.pro, api_name)

        def _fetch():
            return api_func(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=field_str)

        try:
            if self.rate_limiter:
                df = await self.rate_limiter.execute_with_rate_limit(api_name, _fetch)
            else:
                df = await asyncio.to_thread(_fetch)

            if df is None:
                return pd.DataFrame()
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            if len(df) > 0:
                self._save_cache(cache_name, df)
            return df
        except Exception as e:
            logger.warning(f"Failed to fetch {api_name} for {ts_code}: {e}")
            return pd.DataFrame()

    async def get_stk_factor_pro(self, ts_code: str, start_date: str, end_date: str, fields: List[str]) -> pd.DataFrame:
        return await self._fetch_api('stk_factor_pro', ts_code, start_date, end_date, fields, 'stk_factor')

    async def get_moneyflow_dc(self, ts_code: str, start_date: str, end_date: str, fields: List[str]) -> pd.DataFrame:
        return await self._fetch_api('moneyflow_dc', ts_code, start_date, end_date, fields, 'moneyflow')

    async def get_margin_detail(self, ts_code: str, start_date: str, end_date: str, fields: List[str]) -> pd.DataFrame:
        return await self._fetch_api('margin_detail', ts_code, start_date, end_date, fields, 'margin')

    async def get_fina_indicator(self, ts_code: str, start_date: str, end_date: str, fields: List[str]) -> pd.DataFrame:
        return await self._fetch_api('fina_indicator', ts_code, start_date, end_date, fields, 'fina_ind')

    async def get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """Fetch trading-day list (YYYYMMDD) within [start_date, end_date]."""
        cache_name = f"trade_cal_{start_date}_{end_date}.parquet"
        cached = self._load_cache(cache_name)
        if cached is not None and len(cached) > 0 and 'cal_date' in cached.columns:
            return sorted(cached['cal_date'].astype(str).tolist())

        def _fetch():
            return self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open='1')

        try:
            if self.rate_limiter:
                df = await self.rate_limiter.execute_with_rate_limit('stock_basic', _fetch)
            else:
                df = await asyncio.to_thread(_fetch)
            if df is None or len(df) == 0:
                return []
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            self._save_cache(cache_name, df)
            return sorted(df['cal_date'].astype(str).tolist())
        except Exception as e:
            logger.warning(f"Failed to fetch trade_cal {start_date}-{end_date}: {e}")
            return []

    async def _fetch_api_by_date(self, api_name: str, trade_date: str, fields: List[str], cache_prefix: str) -> pd.DataFrame:
        """Fetch a single trade_date for ALL listed stocks via Tushare batch-by-date interface."""
        cache_name = f"{cache_prefix}_by_date_{trade_date}.parquet"
        cached = self._load_cache(cache_name)
        if cached is not None and len(cached) > 0:
            return cached

        field_str = ','.join(fields)
        api_func = getattr(self.pro, api_name)

        def _fetch():
            return api_func(trade_date=trade_date, fields=field_str)

        try:
            if self.rate_limiter:
                df = await self.rate_limiter.execute_with_rate_limit(api_name, _fetch)
            else:
                df = await asyncio.to_thread(_fetch)
            if df is None:
                return pd.DataFrame()
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            if len(df) > 0:
                self._save_cache(cache_name, df)
            return df
        except Exception as e:
            logger.warning(f"Failed to fetch {api_name} by_date {trade_date}: {e}")
            return pd.DataFrame()

    async def get_stk_factor_pro_by_date(self, trade_date: str, fields: List[str]) -> pd.DataFrame:
        return await self._fetch_api_by_date('stk_factor_pro', trade_date, fields, 'stk_factor')

    async def get_moneyflow_dc_by_date(self, trade_date: str, fields: List[str]) -> pd.DataFrame:
        return await self._fetch_api_by_date('moneyflow_dc', trade_date, fields, 'moneyflow')

    async def get_margin_detail_by_date(self, trade_date: str, fields: List[str]) -> pd.DataFrame:
        return await self._fetch_api_by_date('margin_detail', trade_date, fields, 'margin')

    async def get_fina_indicator_by_period(self, period: str, fields: List[str]) -> pd.DataFrame:
        """Fetch financial indicators for ALL listed stocks at a single reporting period (YYYYMMDD)."""
        cache_name = f"fina_ind_by_period_{period}.parquet"
        cached = self._load_cache(cache_name)
        if cached is not None and len(cached) > 0:
            return cached

        field_str = ','.join(fields)

        def _fetch():
            return self.pro.fina_indicator(period=period, fields=field_str)

        try:
            if self.rate_limiter:
                df = await self.rate_limiter.execute_with_rate_limit('fina_indicator', _fetch)
            else:
                df = await asyncio.to_thread(_fetch)
            if df is None:
                return pd.DataFrame()
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            if len(df) > 0:
                self._save_cache(cache_name, df)
            return df
        except Exception as e:
            logger.warning(f"Failed to fetch fina_indicator by_period {period}: {e}")
            return pd.DataFrame()


class QlibDataConverter:
    """Qlib format data converter"""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.calendars_dir = self.output_dir / 'calendars'
        self.features_dir = self.output_dir / 'features'
        self.instruments_dir = self.output_dir / 'instruments'
        self.calendars_dir.mkdir(exist_ok=True)
        self.features_dir.mkdir(exist_ok=True)
        self.instruments_dir.mkdir(exist_ok=True)
        logger.info(f"Qlib output directory: {self.output_dir}")

    def save_calendar(self, dates: List[pd.Timestamp], freq: str = 'day'):
        calendar_path = self.calendars_dir / f'{freq}.txt'
        date_strings = [d.strftime('%Y-%m-%d') for d in sorted(dates)]
        with open(calendar_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(date_strings))

    def save_instruments(self, stock_list: pd.DataFrame, start_date: str, end_date: str):
        instruments_path = self.instruments_dir / 'all.txt'
        with open(instruments_path, 'w', encoding='utf-8') as f:
            for _, row in stock_list.iterrows():
                ts_code = row['ts_code']
                list_date = row.get('list_date', start_date)
                f.write(f"{ts_code}\t{list_date}\t{end_date}\n")

    def dataframe_to_bin(self, df: pd.DataFrame, field: str, freq: str = 'day', source_field=None):
        col = source_field if source_field else field
        if col not in df.columns:
            return
        bin_path = self.features_dir / f"{field.lower()}.{freq}.bin"
        data = df[col].dropna()
        if len(data) == 0:
            return
        calendar_path = self.calendars_dir / f'{freq}.txt'
        if not calendar_path.exists():
            return
        with open(calendar_path, 'r', encoding='utf-8') as f:
            calendar_dates = [line.strip() for line in f.readlines()]
        date_to_idx = {date: idx for idx, date in enumerate(calendar_dates)}
        indices, values = [], []
        for dt, val in data.items():
            date_str = dt.strftime('%Y-%m-%d') if isinstance(dt, pd.Timestamp) else str(dt)[:10]
            if date_str in date_to_idx:
                indices.append(date_to_idx[date_str])
                values.append(float(val))
        if len(indices) == 0:
            return
        output_array = np.column_stack([indices, values]).astype('<f')
        output_array.tofile(str(bin_path.resolve()))

    def convert_and_save(self, merged_df: pd.DataFrame, stock_list: pd.DataFrame, start_date: str, end_date: str):
        dates = pd.to_datetime(merged_df.index.get_level_values('datetime').unique()).sort_values()
        self.save_calendar(dates.tolist())
        self.save_instruments(stock_list, start_date, end_date)

        all_fields = ['open_qfq', 'high_qfq', 'low_qfq', 'close_qfq', 'vol', 'amount',
                      'turnover_rate', 'volume_ratio', 'pe', 'pb', 'ps', 'dv_ratio',
                      'ps_ttm', 'pe_ttm', 'dv_ttm', 'total_mv', 'kdj_qfq', 'kdj_d_qfq',
                      'kdj_k_qfq', 'rsi_qfq_12', 'macd_qfq', 'macd_dea_qfq', 'macd_dif_qfq',
                      'atr_qfq', 'mtmma_qfq', 'turnover_rate_f', 'net_amount', 'buy_elg_amount',
                      'buy_lg_amount', 'buy_md_amount', 'buy_sm_amount', 'rzye', 'rqye',
                      'roe', 'roa', 'roa2_yearly', 'profit_to_gr', 'q_profit_yoy', 'q_eps', 'assets_turn']

        available_fields = [f for f in all_fields if f in merged_df.columns]
        for field in available_fields:
            try:
                self.dataframe_to_bin(merged_df, field)
            except Exception as e:
                logger.error(f"Failed to convert field {field}: {e}")

        for target, source in QLIB_STD_OHLCV_FROM.items():
            if source in merged_df.columns:
                try:
                    self.dataframe_to_bin(merged_df, target, freq='day', source_field=source)
                except Exception as e:
                    logger.error(f"Failed to convert standard field {target}: {e}")


class AsyncDataPipeline:
    """Async data pipeline for Tushare to Qlib conversion"""

    STK_FACTOR_FIELDS = [
        'ts_code', 'trade_date', 'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq',
        'vol', 'amount', 'turnover_rate', 'volume_ratio', 'pe', 'pb', 'ps',
        'dv_ratio', 'ps_ttm', 'pe_ttm', 'dv_ttm', 'total_mv',
        'kdj_qfq', 'kdj_d_qfq', 'kdj_k_qfq', 'rsi_qfq_12',
        'macd_qfq', 'macd_dea_qfq', 'macd_dif_qfq', 'atr_qfq', 'mtmma_qfq', 'turnover_rate_f'
    ]

    MONEYFLOW_FIELDS = ['trade_date', 'ts_code', 'net_amount', 'buy_elg_amount', 'buy_lg_amount', 'buy_md_amount', 'buy_sm_amount']
    MARGIN_FIELDS = ['trade_date', 'ts_code', 'rzye', 'rqye']
    FINA_INDICATOR_FIELDS = ['ts_code', 'end_date', 'roe', 'roa', 'roa2_yearly', 'profit_to_gr', 'q_profit_yoy', 'q_eps', 'assets_turn']

    def __init__(self, token: str, output_dir: str, start_date: str, end_date: str, max_stocks=None, high_speed=False, csv_dir=None, max_concurrent=10, fetch_mode: str = "by_date"):
        self.rate_limiter = AsyncAPIRateLimiter(high_speed=high_speed, max_concurrent=max_concurrent)
        self.fetcher = AsyncTushareDataFetcher(token, rate_limiter=self.rate_limiter)
        self.converter = QlibDataConverter(output_dir)
        self.start_date = start_date
        self.end_date = end_date
        self.max_stocks = max_stocks
        self.max_concurrent = max_concurrent
        self.fetch_mode = fetch_mode
        self.csv_dir = Path(csv_dir) if csv_dir else Path(r"D:\qlib_data\csv_data")
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"AsyncDataPipeline fetch_mode={self.fetch_mode}, max_concurrent={self.max_concurrent}, high_speed={high_speed}")

    async def fetch_all_data_for_stock(self, ts_code: str) -> pd.DataFrame:
        factor_df = await self.fetcher.get_stk_factor_pro(ts_code, self.start_date, self.end_date, self.STK_FACTOR_FIELDS)
        if factor_df.empty:
            return pd.DataFrame()

        factor_df['datetime'] = pd.to_datetime(factor_df['trade_date'])
        factor_df.set_index(['datetime'], inplace=True)

        money_df = await self.fetcher.get_moneyflow_dc(ts_code, self.start_date, self.end_date, self.MONEYFLOW_FIELDS)
        if not money_df.empty:
            money_df['datetime'] = pd.to_datetime(money_df['trade_date'])
            money_df.set_index(['datetime'], inplace=True)
            factor_df = factor_df.join(money_df[['net_amount', 'buy_elg_amount', 'buy_lg_amount', 'buy_md_amount', 'buy_sm_amount']], how='left')

        margin_df = await self.fetcher.get_margin_detail(ts_code, self.start_date, self.end_date, self.MARGIN_FIELDS)
        if not margin_df.empty:
            margin_df['datetime'] = pd.to_datetime(margin_df['trade_date'])
            margin_df.set_index(['datetime'], inplace=True)
            factor_df = factor_df.join(margin_df[['rzye', 'rqye']], how='left')

        fina_df = await self.fetcher.get_fina_indicator(ts_code, self.start_date, self.end_date, self.FINA_INDICATOR_FIELDS)
        if not fina_df.empty:
            fina_df['datetime'] = pd.to_datetime(fina_df['end_date']).dt.normalize()
            fina_df.set_index(['datetime'], inplace=True)
            if isinstance(factor_df.index, pd.DatetimeIndex):
                factor_df.index = factor_df.index.normalize()
            else:
                factor_df.index = pd.to_datetime(factor_df.index).dt.normalize()
            factor_df = factor_df.join(fina_df[['roe', 'roa', 'roa2_yearly', 'profit_to_gr', 'q_profit_yoy', 'q_eps', 'assets_turn']], how='left')
            fina_cols = ['roe', 'roa', 'roa2_yearly', 'profit_to_gr', 'q_profit_yoy', 'q_eps', 'assets_turn']
            existing_cols = [col for col in fina_cols if col in factor_df.columns]
            if existing_cols:
                factor_df[existing_cols] = factor_df[existing_cols].ffill()

        factor_df['instrument'] = ts_code
        factor_df.reset_index(inplace=True)
        factor_df.set_index(['instrument', 'datetime'], inplace=True)
        return factor_df

    def save_stock_to_csv(self, ts_code: str, df: pd.DataFrame) -> bool:
        if df.empty:
            return False
        try:
            csv_path = self.csv_dir / f"{ts_code.lower()}.csv"
            df_to_save = df.reset_index()
            add_qlib_standard_ohlcv_columns(df_to_save)
            df_to_save.to_csv(csv_path, index=False, encoding='utf-8-sig')
            return True
        except Exception as e:
            logger.error(f"Failed to save {ts_code} to CSV: {e}")
            return False

    def save_stock_to_csv_merge(self, ts_code: str, new_df: pd.DataFrame) -> bool:
        """Incrementally merge new rows into existing CSV: read old + concat + dedup(datetime) + sort + write."""
        if new_df is None or new_df.empty:
            return False
        try:
            csv_path = self.csv_dir / f"{ts_code.lower()}.csv"
            new_to_save = new_df.reset_index() if isinstance(new_df.index, pd.MultiIndex) or new_df.index.name else new_df.copy()
            add_qlib_standard_ohlcv_columns(new_to_save)
            if 'datetime' in new_to_save.columns:
                new_to_save['datetime'] = pd.to_datetime(new_to_save['datetime'])

            if csv_path.exists():
                try:
                    old_df = pd.read_csv(csv_path, encoding='utf-8-sig')
                    if 'datetime' in old_df.columns:
                        old_df['datetime'] = pd.to_datetime(old_df['datetime'])
                    combined = pd.concat([old_df, new_to_save], ignore_index=True, sort=False)
                    if 'datetime' in combined.columns:
                        combined = combined.drop_duplicates(subset=['datetime'], keep='last')
                        combined = combined.sort_values('datetime')
                    combined.to_csv(csv_path, index=False, encoding='utf-8-sig')
                except Exception as merge_err:
                    logger.warning(f"Merge failed for {ts_code}, overwriting: {merge_err}")
                    new_to_save.to_csv(csv_path, index=False, encoding='utf-8-sig')
            else:
                new_to_save.to_csv(csv_path, index=False, encoding='utf-8-sig')
            return True
        except Exception as e:
            logger.error(f"Failed to merge-save {ts_code}: {e}")
            return False

    def _compute_quarter_periods(self) -> List[str]:
        """Return quarter-end YYYYMMDD strings covering [start_date, end_date], plus one quarter before start
        so that the active financial period at start_date is always included."""
        start = pd.to_datetime(self.start_date, format='%Y%m%d')
        end = pd.to_datetime(self.end_date, format='%Y%m%d')
        cur_q = (start.month - 1) // 3 + 1
        cur_year = start.year
        cur_q -= 1
        if cur_q == 0:
            cur_q = 4
            cur_year -= 1
        periods: List[str] = []
        while True:
            q_end_month = cur_q * 3
            q_end_ts = (pd.Timestamp(year=cur_year, month=q_end_month, day=1) + pd.offsets.MonthEnd(0))
            if q_end_ts > end + pd.Timedelta(days=120):
                break
            periods.append(q_end_ts.strftime('%Y%m%d'))
            cur_q += 1
            if cur_q > 4:
                cur_q = 1
                cur_year += 1
            if cur_year > end.year + 1:
                break
        return periods

    async def download_all_to_csv_by_date(self, stock_codes: List[str] = None) -> Tuple[int, int]:
        """Batch-by-date pipeline: 4 APIs fetched in parallel across trade dates / periods,
        then per-stock joined in memory and merged into existing CSVs. Dramatically faster
        for incremental updates (N days x M stocks -> N+P calls)."""
        t0 = time.time()
        stock_list = await self.fetcher.get_stock_list()
        if stock_codes is None:
            stock_codes = stock_list['ts_code'].tolist()
        if self.max_stocks:
            stock_codes = stock_codes[: self.max_stocks]
        pool_set = set(stock_codes)
        logger.info(f"[by_date] pool size: {len(pool_set)} stocks")

        trade_dates = await self.fetcher.get_trade_dates(self.start_date, self.end_date)
        if not trade_dates:
            logger.error("[by_date] no trade dates in range, abort")
            return 0, 0
        logger.info(f"[by_date] trade dates: {len(trade_dates)} ({trade_dates[0]} ~ {trade_dates[-1]})")

        periods = self._compute_quarter_periods()
        logger.info(f"[by_date] fina periods: {len(periods)} -> {periods}")

        factor_fields = [f for f in AsyncDataPipeline.STK_FACTOR_FIELDS]
        money_fields = [f for f in AsyncDataPipeline.MONEYFLOW_FIELDS]
        margin_fields = [f for f in AsyncDataPipeline.MARGIN_FIELDS]
        fina_fields = [f for f in AsyncDataPipeline.FINA_INDICATOR_FIELDS]

        async def gather_with_progress(coros, desc):
            results = []
            with tqdm(total=len(coros), desc=desc, unit="req") as pbar:
                for coro in asyncio.as_completed(coros):
                    res = await coro
                    results.append(res)
                    pbar.update(1)
            return results

        factor_coros = [self.fetcher.get_stk_factor_pro_by_date(d, factor_fields) for d in trade_dates]
        money_coros = [self.fetcher.get_moneyflow_dc_by_date(d, money_fields) for d in trade_dates]
        margin_coros = [self.fetcher.get_margin_detail_by_date(d, margin_fields) for d in trade_dates]
        fina_coros = [self.fetcher.get_fina_indicator_by_period(p, fina_fields) for p in periods]

        factor_results, money_results, margin_results, fina_results = await asyncio.gather(
            gather_with_progress(factor_coros, "stk_factor_pro by_date"),
            gather_with_progress(money_coros, "moneyflow_dc by_date"),
            gather_with_progress(margin_coros, "margin_detail by_date"),
            gather_with_progress(fina_coros, "fina_indicator by_period"),
        )

        t1 = time.time()
        logger.info(f"[by_date] API fetch elapsed: {t1 - t0:.1f}s")

        def _concat_filter(results, label):
            frames = [df for df in results if isinstance(df, pd.DataFrame) and not df.empty]
            if not frames:
                logger.warning(f"[by_date] {label}: no data")
                return pd.DataFrame()
            out = pd.concat(frames, ignore_index=True)
            if 'ts_code' in out.columns:
                out = out[out['ts_code'].isin(pool_set)].copy()
            logger.info(f"[by_date] {label}: {len(out)} rows after pool filter")
            return out

        factor_df = _concat_filter(factor_results, "stk_factor")
        money_df = _concat_filter(money_results, "moneyflow")
        margin_df = _concat_filter(margin_results, "margin")
        fina_df = _concat_filter(fina_results, "fina_indicator")

        if factor_df.empty:
            logger.error("[by_date] no stk_factor data -> abort")
            return 0, 0

        factor_df['datetime'] = pd.to_datetime(factor_df['trade_date'])
        if not money_df.empty:
            money_df['datetime'] = pd.to_datetime(money_df['trade_date'])
        if not margin_df.empty:
            margin_df['datetime'] = pd.to_datetime(margin_df['trade_date'])
        if not fina_df.empty:
            fina_df['datetime'] = pd.to_datetime(fina_df['end_date']).dt.normalize()
            fina_df = fina_df.drop_duplicates(subset=['ts_code', 'datetime'], keep='last')

        factor_groups = factor_df.groupby('ts_code', sort=False)
        money_groups = money_df.groupby('ts_code', sort=False) if not money_df.empty else None
        margin_groups = margin_df.groupby('ts_code', sort=False) if not margin_df.empty else None
        fina_groups = fina_df.groupby('ts_code', sort=False) if not fina_df.empty else None

        money_cols = ['net_amount', 'buy_elg_amount', 'buy_lg_amount', 'buy_md_amount', 'buy_sm_amount']
        margin_cols = ['rzye', 'rqye']
        fina_cols = ['roe', 'roa', 'roa2_yearly', 'profit_to_gr', 'q_profit_yoy', 'q_eps', 'assets_turn']

        success = 0
        fail = 0
        for ts_code in tqdm(stock_codes, desc="merge & save", unit="stock"):
            try:
                if ts_code not in factor_groups.groups:
                    fail += 1
                    continue
                s_factor = factor_groups.get_group(ts_code).copy()
                s_factor = s_factor.set_index('datetime').sort_index()

                if money_groups is not None and ts_code in money_groups.groups:
                    m = money_groups.get_group(ts_code).set_index('datetime')
                    cols = [c for c in money_cols if c in m.columns]
                    if cols:
                        s_factor = s_factor.join(m[cols], how='left')

                if margin_groups is not None and ts_code in margin_groups.groups:
                    m = margin_groups.get_group(ts_code).set_index('datetime')
                    cols = [c for c in margin_cols if c in m.columns]
                    if cols:
                        s_factor = s_factor.join(m[cols], how='left')

                if fina_groups is not None and ts_code in fina_groups.groups:
                    f = fina_groups.get_group(ts_code).copy()
                    cols = [c for c in fina_cols if c in f.columns]
                    if cols:
                        s_factor.index = s_factor.index.normalize()
                        f['datetime'] = pd.to_datetime(f['datetime']).dt.normalize()
                        f = f.drop_duplicates(subset=['datetime'], keep='last').sort_values('datetime')
                        left = s_factor.reset_index().sort_values('datetime')
                        merged = pd.merge_asof(
                            left,
                            f[['datetime'] + cols],
                            on='datetime',
                            direction='backward',
                        )
                        s_factor = merged.set_index('datetime')
                        s_factor[cols] = s_factor[cols].ffill()

                s_factor['instrument'] = ts_code
                s_factor.reset_index(inplace=True)
                s_factor.set_index(['instrument', 'datetime'], inplace=True)
                if self.save_stock_to_csv_merge(ts_code, s_factor):
                    success += 1
                else:
                    fail += 1
            except Exception as e:
                logger.error(f"[by_date] error on {ts_code}: {e}")
                fail += 1

        t2 = time.time()
        logger.info(f"[by_date] join+save elapsed: {t2 - t1:.1f}s, total: {t2 - t0:.1f}s, success={success}, fail={fail}")
        return success, fail

    async def _download_single_stock(self, ts_code: str) -> str:
        try:
            csv_path = self.csv_dir / f"{ts_code.lower()}.csv"
            if csv_path.exists():
                return "skip"
            df = await self.fetch_all_data_for_stock(ts_code)
            if df.empty:
                return "fail"
            return "success" if self.save_stock_to_csv(ts_code, df) else "fail"
        except Exception as e:
            logger.error(f"Error processing {ts_code}: {e}")
            return "fail"

    async def download_all_to_csv(self, stock_codes: List[str] = None) -> Tuple[int, int]:
        stock_list = await self.fetcher.get_stock_list()
        if stock_codes is None:
            stock_codes = stock_list['ts_code'].tolist()
        if self.max_stocks:
            stock_codes = stock_codes[:self.max_stocks]

        success_count = 0
        fail_count = 0
        skipped_count = 0
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def bounded_download(ts_code: str):
            async with semaphore:
                return await self._download_single_stock(ts_code)

        tasks = [bounded_download(code) for code in stock_codes]
        with tqdm(total=len(stock_codes), desc="Downloading CSV", unit="stock") as pbar:
            for task in asyncio.as_completed(tasks):
                status = await task
                if status == "success":
                    success_count += 1
                elif status == "skip":
                    success_count += 1
                    skipped_count += 1
                else:
                    fail_count += 1
                pbar.update(1)
                pbar.set_postfix(success=success_count, fail=fail_count, skip=skipped_count)

        return success_count, fail_count

    def convert_csv_to_qlib(self, dump_mode: str = "dump_all") -> bool:
        csv_files = sorted(self.csv_dir.glob("*.csv"))
        if not csv_files:
            logger.error("No CSV files found!")
            return False

        sample = pd.read_csv(csv_files[0], nrows=512, encoding="utf-8-sig")
        skip_cols = {"datetime", "instrument", "trade_date", "ts_code"}
        feature_cols = [c for c in sample.columns if c not in skip_cols and pd.api.types.is_numeric_dtype(sample[c])]
        if not feature_cols:
            logger.error("No numeric feature columns found")
            return False

        qlib_main = Path(os.environ.get("QLIB_SOURCE", r"D:\quant_project\qlib-main"))
        dump_script = qlib_main / "scripts" / "dump_bin.py"
        if not dump_script.is_file():
            logger.error(f"dump_bin.py not found: {dump_script}")
            return False

        if str(qlib_main / "scripts") not in sys.path:
            sys.path.insert(0, str(qlib_main / "scripts"))

        max_workers = min(16, (os.cpu_count() or 4))
        try:
            if dump_mode == "dump_update":
                from dump_bin import DumpDataUpdate
                dumper = DumpDataUpdate(
                    data_path=str(self.csv_dir),
                    qlib_dir=str(self.converter.output_dir),
                    date_field_name="datetime",
                    symbol_field_name="instrument",
                    include_fields=",".join(feature_cols),
                    freq="day",
                    max_workers=max_workers,
                )
            else:
                from dump_bin import DumpDataAll
                dumper = DumpDataAll(
                    data_path=str(self.csv_dir),
                    qlib_dir=str(self.converter.output_dir),
                    date_field_name="datetime",
                    symbol_field_name="instrument",
                    include_fields=",".join(feature_cols),
                    freq="day",
                    max_workers=max_workers,
                )
            dumper.dump()
            return True
        except Exception:
            logger.exception("dump_bin conversion failed")
            return False

    async def run(self, stock_codes: List[str] = None, dump_mode: str = "dump_all"):
        if self.fetch_mode == "by_date":
            success, fail = await self.download_all_to_csv_by_date(stock_codes)
        else:
            success, fail = await self.download_all_to_csv(stock_codes)
        logger.info(f"Download stage done: success={success}, fail={fail}")
        if success > 0:
            self.convert_csv_to_qlib(dump_mode=dump_mode)


def main():
    parser = argparse.ArgumentParser(description='Tushare to Qlib data converter (async version)')
    parser.add_argument('--token', type=str, default=None, help='Tushare API Token')
    parser.add_argument('--start_date', type=str, default='20200101', help='Start date (YYYYMMDD)')
    parser.add_argument('--end_date', type=str, default='20231231', help='End date (YYYYMMDD)')
    parser.add_argument('--output_dir', type=str, default=r'D:\qlib_data\qlib_data', help='Qlib output directory')
    parser.add_argument('--csv_dir', type=str, default=r'D:\qlib_data\csv_data', help='CSV directory')
    parser.add_argument('--max_stocks', type=int, default=None, help='Max stocks to process')
    parser.add_argument('--high-speed', action='store_true', help='Enable high-speed mode')
    parser.add_argument('--max_concurrent', type=int, default=10, help='Max concurrent requests')
    parser.add_argument('--stage', type=str, default='all', choices=['all', 'download', 'convert'], help='Execution stage')
    parser.add_argument('--fetch_mode', type=str, default='by_date', choices=['by_date', 'by_stock'],
                        help='by_date: batch fetch all stocks per trade-date (fast for incremental). by_stock: legacy per-stock loop')
    parser.add_argument('--dump_mode', type=str, default='dump_all', choices=['dump_all', 'dump_update'],
                        help='dump_all: full rebuild. dump_update: incremental append')
    args = parser.parse_args()

    token = args.token or os.environ.get('TUSHARE_API_KEY', '').strip() or os.environ.get('TUSHARE_TOKEN', '').strip() or get_tushare_token()

    if not token:
        print("\n[ERR] Error: Tushare API Token not provided!")
        print("\nPlease provide Token via one of the following methods:")
        print("  1. Command line: --token YOUR_TOKEN")
        print("  2. Environment variable: set TUSHARE_API_KEY=YOUR_TOKEN")
        print("  3. Config file: config/secrets.yaml")
        sys.exit(1)

    pipeline = AsyncDataPipeline(
        token=token,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        max_stocks=args.max_stocks,
        high_speed=args.high_speed,
        csv_dir=args.csv_dir,
        max_concurrent=args.max_concurrent,
        fetch_mode=args.fetch_mode,
    )

    if args.stage == 'all':
        asyncio.run(pipeline.run(dump_mode=args.dump_mode))
    elif args.stage == 'download':
        if args.fetch_mode == 'by_date':
            success, fail = asyncio.run(pipeline.download_all_to_csv_by_date())
        else:
            success, fail = asyncio.run(pipeline.download_all_to_csv())
        print(f"\n[STAT] Download completed! Success: {success}, Fail: {fail}")
    elif args.stage == 'convert':
        success = pipeline.convert_csv_to_qlib(dump_mode=args.dump_mode)
        if not success:
            sys.exit(1)


if __name__ == '__main__':
    main()