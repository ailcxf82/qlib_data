#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tushare data fetcher and Qlib converter (async version)

Features:
1. Async IO for fetching data from Tushare (technical factors, money flow, margin trading, financial indicators)
2. Convert and save to Qlib standard format (.bin files)
3. Save to specified directory

Usage:
    python tushare_to_qlib_async.py --start_date 20200101 --end_date 20260428
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

    def __init__(self, token: str, output_dir: str, start_date: str, end_date: str, max_stocks=None, high_speed=False, csv_dir=None, max_concurrent=10):
        self.rate_limiter = AsyncAPIRateLimiter(high_speed=high_speed, max_concurrent=max_concurrent)
        self.fetcher = AsyncTushareDataFetcher(token, rate_limiter=self.rate_limiter)
        self.converter = QlibDataConverter(output_dir)
        self.start_date = start_date
        self.end_date = end_date
        self.max_stocks = max_stocks
        self.max_concurrent = max_concurrent
        self.csv_dir = Path(csv_dir) if csv_dir else Path(r"D:\qlib_data\csv_data")
        self.csv_dir.mkdir(parents=True, exist_ok=True)

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

    def convert_csv_to_qlib(self) -> bool:
        csv_files = sorted(self.csv_dir.glob("*.csv"))
        if not csv_files:
            logger.error("No CSV files found!")
            return False

        for csv_path in csv_files:
            try:
                df_patch = pd.read_csv(csv_path, encoding="utf-8-sig")
                add_qlib_standard_ohlcv_columns(df_patch)
                df_patch.to_csv(csv_path, index=False, encoding="utf-8-sig")
            except Exception as e:
                logger.warning(f"Failed to add OHLCV columns for {csv_path.name}: {e}")

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

        try:
            from dump_bin import DumpDataAll
        except ImportError as exc:
            logger.error(f"Failed to import dump_bin: {exc}")
            return False

        try:
            dumper = DumpDataAll(
                data_path=str(self.csv_dir),
                qlib_dir=str(self.converter.output_dir),
                date_field_name="datetime",
                symbol_field_name="instrument",
                include_fields=",".join(feature_cols),
                freq="day",
                max_workers=min(16, (os.cpu_count() or 4)),
            )
            dumper.dump()
            return True
        except Exception:
            logger.exception("dump_bin conversion failed")
            return False

    async def run(self, stock_codes: List[str] = None):
        success, fail = await self.download_all_to_csv(stock_codes)
        if success > 0:
            self.convert_csv_to_qlib()


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
        max_concurrent=args.max_concurrent
    )

    if args.stage == 'all':
        asyncio.run(pipeline.run())
    elif args.stage == 'download':
        success, fail = asyncio.run(pipeline.download_all_to_csv())
        print(f"\n[STAT] Download completed! Success: {success}, Fail: {fail}")
    elif args.stage == 'convert':
        success = pipeline.convert_csv_to_qlib()
        if not success:
            sys.exit(1)


if __name__ == '__main__':
    main()