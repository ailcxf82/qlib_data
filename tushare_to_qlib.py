#!/usr/bin/env python

# -*- coding: utf-8 -*-

"""

TusharedataGetQlibformat script

Features?1. ushareGet

2. lib?bin?3. D:/qlib_data/qlib_data



Usage?    python tushare_to_qlib.py --start_date 20200101 --end_date 20260424

    ?TUSHARE_API_KEY then run directly"""



from __future__ import annotations



import argparse

import logging

import os

import sys

import time

import threading



import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from pathlib import Path

from typing import Dict, List, Optional, Tuple

from collections import deque

from datetime import datetime, timedelta

from concurrent.futures import ThreadPoolExecutor, as_completed



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

    """

    Add to DataFrame openighowloseolume 

    open_qfqigh_qfqow_qfqlose_qfqolume_ratio?    Does not delete or rename original columns?    """

    if df is None or len(df) == 0:

        return df

    for target, source in QLIB_STD_OHLCV_FROM.items():

        if source in df.columns:

            df[target] = df[source]

    return df





class APIRateLimiter:

    """

    Tushare API rate limit?

    rate limit?    retry mechanism?

    rate limitushare

    - stk_factor_pro: 5000=30?, 8000+=500?

    - moneyflow_dc: 5000?0??    - margin_detail: 2000?0??    - fina_indicator: 2000?0??    - stock_basic: ?    """




    RATE_LIMITS = {

        'stk_factor_pro': {

            'normal': 30,      # 5000

            'high': 500,       # 8000+

            'current': 30      # 

        },

        'moneyflow_dc': {

            'normal': 30,

            'high': 100,

            'current': 30

        },

        'margin_detail': {

            'normal': 30,

            'high': 60,

            'current': 30

        },

        'fina_indicator': {

            'normal': 20,      # financial indicators?            'high': 40,

            'current': 20

        },

        'stock_basic': {

            'normal': 60,

            'high': 120,

            'current': 60

        }

    }




    MAX_RETRIES = 3

    RETRY_DELAYS = [1, 2, 4]  # ?

    def __init__(self, high_speed: bool = False):

        """

        



        Args?            high_speed: ?000+?        """

        self.high_speed = high_speed

        self.request_history: Dict[str, deque] = {}

        self.mode = 'high' if high_speed else 'normal'


        self._api_locks: Dict[str, threading.Lock] = {}




        for api_name in self.RATE_LIMITS.keys():

            self.request_history[api_name] = deque(maxlen=1000)

            self._api_locks[api_name] = threading.Lock()




            self.RATE_LIMITS[api_name]['current'] = self.RATE_LIMITS[api_name][self.mode]



        logger.info(f"APIrate limit (: {self.mode})")



    def _get_interval(self, api_name: str) -> float:

        """

        



        Args?            api_name: 



        ?            

        """

        if api_name not in self.RATE_LIMITS:

            return 2.0  # 2?

        rate_per_minute = self.RATE_LIMITS[api_name]['current']

        interval = 60.0 / rate_per_minute






        return interval



    def wait_if_needed(self, api_name: str):

        """

        ?

        Args?            api_name: 

        """

        if api_name not in self.request_history:

            return



        interval = self._get_interval(api_name)

        history = self.request_history[api_name]



        if len(history) > 0:

            last_request_time = history[-1]

            elapsed = time.time() - last_request_time



            if elapsed < interval:

                wait_time = interval - elapsed

                logger.debug(f"[{api_name}]  {wait_time:.2f} rate limit")

                time.sleep(wait_time)



    def record_request(self, api_name: str):

        """"""

        if api_name in self.request_history:

            self.request_history[api_name].append(time.time())



    def execute_with_rate_limit(self, api_name: str, func, *args, **kwargs):

        """

        API?

        Args?            api_name: 

            func: 

            *args, **kwargs: Args



        ?            



        ?            ?        """

        last_exception = None

        for attempt in range(self.MAX_RETRIES):
            try:
                api_lock = self._api_locks.get(api_name)
                if api_lock is None:
                    api_lock = threading.Lock()
                    self._api_locks[api_name] = api_lock
                with api_lock:
                    self.wait_if_needed(api_name)
                    self.record_request(api_name)

                result = func(*args, **kwargs)
                return result

            except Exception as e:
                last_exception = e
                error_msg = str(e).lower()

                is_rate_limit_error = any(keyword in error_msg for keyword in [
                    'limit', 'too many', 'freq', 'rate'
                ])

                delay = min(2 ** attempt * 1.0, 30.0)

                if is_rate_limit_error and attempt < self.MAX_RETRIES - 1:
                    logger.warning(
                        f"[{api_name}] rate limit ({attempt+1}/{self.MAX_RETRIES}): {e}"
                        f"\n    waiting {delay:.1f}s ..."
                    )
                    time.sleep(delay)

                    if api_name in self.RATE_LIMITS:
                        current_rate = self.RATE_LIMITS[api_name]['current']
                        new_rate = max(5, current_rate * 0.7)
                        self.RATE_LIMITS[api_name]['current'] = new_rate
                        logger.info(
                            f"[{api_name}] rate adjusted: "
                            f"{current_rate:.1f} -> {new_rate:.1f} req/min"
                        )

                elif attempt < self.MAX_RETRIES - 1:
                    logger.warning(
                        f"[{api_name}] error ({attempt+1}/{self.MAX_RETRIES}): {e}"
                        f"\n    waiting {delay:.1f}s ..."
                    )
                    time.sleep(delay)

        logger.error(f"[{api_name}] failed after {self.MAX_RETRIES} retries")
        raise last_exception



    def get_stats(self) -> Dict:

        """"""

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






    if token:

        logger.info("?TUSHARE_TOKEN Getoken")

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

                        logger.info("?config/secrets.yaml Getoken")

                        return token

    except Exception:

        pass



    return None





class TushareDataFetcher:
    """Tushare data fetcher with caching"""

    def __init__(self, token: str, rate_limiter=None, cache_dir: str = None):
        import tushare as ts
        ts.set_token(token)
        self.pro = ts.pro_api()
        self.rate_limiter = rate_limiter
        if cache_dir:
            self._cache_dir = Path(cache_dir)
        else:
            self._cache_dir = Path(r"D:\qlib_data\cache")
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, name: str) -> Path:
        return self._cache_dir / name

    def _load_cache(self, name: str) -> Optional[pd.DataFrame]:
        """Load cached data"""
        p = self._cache_path(name)
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

        """Save cachedata"""

        p = self._cache_path(name)

        try:

            df.to_parquet(p, index=False)

        except Exception:

            df.to_csv(p.with_suffix('.csv'), index=False)



    def get_stock_list(self) -> pd.DataFrame:

        """Get stock list"""

        cache_name = "stock_list.parquet"

        cached = self._load_cache(cache_name)



        if cached is not None and len(cached) > 0:

            return cached



        def _fetch():

            return self.pro.stock_basic(

                exchange='', list_status='L',

                fields='ts_code,symbol,name,area,industry,list_date'

            )



        if self.rate_limiter:
            df = self.rate_limiter.execute_with_rate_limit('stock_basic', _fetch)
        else:
            df = _fetch()

        if df is not None and len(df) > 0:
            self._save_cache(cache_name, df)

        return df if df is not None else pd.DataFrame()



    def get_stk_factor_pro(self, ts_code: str, start_date: str, end_date: str,

                           fields: List[str]) -> pd.DataFrame:

        """

        Getdata (stk_factor_pro)



        rate limit?000=30?, 8000+=500?



        Args?            ts_code: stock code

            start_date: start date(YYYYMMDD)

            end_date: end date (YYYYMMDD)

            fields: 

        """

        cache_name = f"stk_factor_{ts_code}_{start_date}_{end_date}.parquet"

        cached = self._load_cache(cache_name)



        if cached is not None and len(cached) > 0:
            return cached



        field_str = ','.join(fields)



        def _fetch():

            return self.pro.stk_factor_pro(

                ts_code=ts_code,

                start_date=start_date,

                end_date=end_date,

                fields=field_str

            )



        try:

            if self.rate_limiter:

                df = self.rate_limiter.execute_with_rate_limit('stk_factor_pro', _fetch)

            else:

                df = _fetch()







            if df is None:

                logger.debug(f"[{ts_code}] stk_factor_pro None")

                return pd.DataFrame()




            if not isinstance(df, pd.DataFrame):
                logger.warning(f"[{ts_code}] stk_factor_pro not DataFrame: {type(df)}")
                df = pd.DataFrame(df)

            if df is not None and len(df) > 0:
                logger.info(f"[{ts_code}] stk_factor_pro got {len(df)} rows")
                self._save_cache(cache_name, df)



            return df



        except Exception as e:

            logger.warning(f"Get {ts_code} ? {e}")

            import traceback

            logger.debug(f":\n{traceback.format_exc()}")

            return pd.DataFrame()



    def get_moneyflow_dc(self, ts_code: str, start_date: str, end_date: str,

                         fields: List[str]) -> pd.DataFrame:

        """

        Getmoney flowdata (moneyflow_dc)



        rate limit?0??000+?

        Args?            ts_code: stock code

            start_date: start date(YYYYMMDD)

            end_date: end date (YYYYMMDD)

            fields: 

        """

        cache_name = f"moneyflow_{ts_code}_{start_date}_{end_date}.parquet"

        cached = self._load_cache(cache_name)



        if cached is not None and len(cached) > 0:
            return cached



        field_str = ','.join(fields)



        def _fetch():

            return self.pro.moneyflow_dc(

                ts_code=ts_code,

                start_date=start_date,

                end_date=end_date,

                fields=field_str

            )



        try:

            if self.rate_limiter:

                df = self.rate_limiter.execute_with_rate_limit('moneyflow_dc', _fetch)

            else:

                df = _fetch()




            if df is None:

                return pd.DataFrame()




            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            if df is not None and len(df) > 0:
                self._save_cache(cache_name, df)

            return df

        except Exception as e:
            logger.warning(f"Get {ts_code} money flow: {e}")

            return pd.DataFrame()



    def get_margin_detail(self, ts_code: str, start_date: str, end_date: str,

                          fields: List[str]) -> pd.DataFrame:

        """

        Getmargin trading (margin_detail)



        rate limit?0??000+?

        Args?            ts_code: stock code

            start_date: start date(YYYYMMDD)

            end_date: end date (YYYYMMDD)

            fields: 

        """

        cache_name = f"margin_{ts_code}_{start_date}_{end_date}.parquet"

        cached = self._load_cache(cache_name)



        if cached is not None and len(cached) > 0:
            return cached



        field_str = ','.join(fields)



        def _fetch():

            return self.pro.margin_detail(

                ts_code=ts_code,

                start_date=start_date,

                end_date=end_date,

                fields=field_str

            )



        try:

            if self.rate_limiter:

                df = self.rate_limiter.execute_with_rate_limit('margin_detail', _fetch)

            else:

                df = _fetch()




            if df is None:

                return pd.DataFrame()




            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            if df is not None and len(df) > 0:
                self._save_cache(cache_name, df)

            return df

        except Exception as e:
            logger.warning(f"Get {ts_code} margin tradingdata: {e}")

            return pd.DataFrame()



    def get_fina_indicator(self, ts_code: str, start_date: str, end_date: str,

                           fields: List[str]) -> pd.DataFrame:

        """

        Get financial indicator data (fina_indicator)



        rate limit?0??000+?

        ?00



        Args?            ts_code: stock code

            start_date: start date(YYYYMMDD)

            end_date: end date (YYYYMMDD)

            fields: 

        """

        cache_name = f"fina_ind_{ts_code}_{start_date}_{end_date}.parquet"

        cached = self._load_cache(cache_name)



        if cached is not None and len(cached) > 0:
            return cached



        field_str = ','.join(fields)



        def _fetch():

            return self.pro.fina_indicator(

                ts_code=ts_code,

                start_date=start_date,

                end_date=end_date,

                fields=field_str

            )



        try:

            if self.rate_limiter:

                df = self.rate_limiter.execute_with_rate_limit('fina_indicator', _fetch)

            else:

                df = _fetch()




            if df is None:

                return pd.DataFrame()




            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            if df is not None and len(df) > 0:
                self._save_cache(cache_name, df)

            return df

        except Exception as e:
            logger.warning(f"Get {ts_code} financial indicators: {e}")

            return pd.DataFrame()





class QlibDataConverter:
    """Qlib data converter"""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.calendars_dir = self.output_dir / 'calendars'
        self.instruments_dir = self.output_dir / 'instruments'
        self.features_dir = self.output_dir / 'features'
        self.calendars_dir.mkdir(parents=True, exist_ok=True)
        self.instruments_dir.mkdir(parents=True, exist_ok=True)
        self.features_dir.mkdir(parents=True, exist_ok=True)

    def save_calendar(self, dates, freq='day'):
        """
        Save calendar dates to file
        
        Args:
            dates: list of dates
            freq: frequency (day/minute)
        """
        calendar_path = self.calendars_dir / f'{freq}.txt'

        date_strings = [d.strftime('%Y-%m-%d') for d in sorted(dates)]
        with open(calendar_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(date_strings))

        logger.info(f"Saved calendar: {calendar_path}, {len(date_strings)} dates")



    def save_instruments(self, stock_list: pd.DataFrame, start_date: str, end_date: str):

        """

        ?

        Args?            stock_list: DataFrame

            start_date: start date            end_date: end date

        """

        instruments_path = self.instruments_dir / 'all.txt'



        with open(instruments_path, 'w', encoding='utf-8') as f:

            for _, row in stock_list.iterrows():

                ts_code = row['ts_code']

                list_date = row.get('list_date', start_date)

                line = f"{ts_code}\t{list_date}\t{end_date}\n"

                f.write(line)



        logger.info(f": {instruments_path}, ?{len(stock_list)} ?")



    def dataframe_to_bin(

        self,

        df: pd.DataFrame,

        field: str,

        freq: str = 'day',

        source_field: Optional[str] = None,

    ):

        """

        datain



        Qlib bin?        - ?        - ?        - dataloat32?

        Args?            df: DataFrameatetime?            field:  bin  open?            freq: 

            source_field: ?field ?.bin 

        """

        col = source_field if source_field is not None else field

        if col not in df.columns:

            logger.warning(f" {col} ")

            return



        bin_path = self.features_dir / f"{field.lower()}.{freq}.bin"




        data = df[col].dropna()



        if len(data) == 0:

            logger.warning(f" {field} data")

            return




        if not calendar_path.exists():

            logger.error("")

            return



        with open(calendar_path, 'r', encoding='utf-8') as f:

            calendar_dates = [line.strip() for line in f.readlines()]




        date_to_idx = {date: idx for idx, date in enumerate(calendar_dates)}




        values = []



        for dt, val in data.items():

            if isinstance(dt, (pd.Timestamp, str)):

                date_str = dt.strftime('%Y-%m-%d') if isinstance(dt, pd.Timestamp) else str(dt)[:10]

                if date_str in date_to_idx:

                    indices.append(date_to_idx[date_str])

                    values.append(float(val))



        if len(indices) == 0:

            logger.warning(f" {field} data")

            return







        output_array.tofile(str(bin_path.resolve()))



        logger.debug(f" {field} bin: {len(values)} ")



    def convert_and_save(self, merged_df: pd.DataFrame, stock_list: pd.DataFrame,

                         start_date: str, end_date: str):

        """

        Qlib?

        Args?            merged_df: DataFrame

            stock_list: 

            start_date: start date            end_date: end date

        """

        logger.info("Qlib...")




        dates = pd.to_datetime(merged_df.index.get_level_values('datetime').unique()).sort_values()

        self.save_calendar(dates.tolist())








        base_fields = ['open_qfq', 'high_qfq', 'low_qfq', 'close_qfq', 'vol', 'amount']

        tech_fields = [
            'turnover_rate', 'volume_ratio', 'pe', 'pb', 'ps', 'dv_ratio',
            'ps_ttm', 'pe_ttm', 'dv_ttm', 'total_mv',
            'kdj_qfq', 'kdj_d_qfq', 'kdj_k_qfq', 'rsi_qfq_12',
            'macd_qfq', 'macd_dea_qfq', 'macd_dif_qfq', 'atr_qfq', 'mtmma_qfq',
            'turnover_rate_f'
        ]




        money_fields = [

            'net_amount', 'buy_elg_amount', 'buy_lg_amount',

            'buy_md_amount', 'buy_sm_amount'

        ]




        margin_fields = ['rzye', 'rqye']




        fina_fields = [

            'roe', 'roa', 'roa2_yearly', 'profit_to_gr',

            'q_profit_yoy', 'q_eps', 'assets_turn'

        ]



        all_fields = base_fields + tech_fields + money_fields + margin_fields + fina_fields




        available_fields = [f for f in all_fields if f in merged_df.columns]

        logger.info(f"?{len(available_fields)} ")



        for field in available_fields:

            try:

                self.dataframe_to_bin(merged_df, field)

            except Exception as e:

                logger.error(f" {field} : {e}")



        for target, source in QLIB_STD_OHLCV_FROM.items():

            if source not in merged_df.columns:

                continue

            try:

                self.dataframe_to_bin(merged_df, target, freq='day', source_field=source)

            except Exception as e:

                logger.error(f" {target}?{source}? {e}")



        logger.info(f"Qlib: {self.output_dir}")





class DataPipeline:

    """"""




    STK_FACTOR_FIELDS = [

        'ts_code', 'trade_date', 'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq',

        'vol', 'amount', 'turnover_rate', 'volume_ratio', 'pe', 'pb', 'ps',

        'dv_ratio', 'ps_ttm', 'pe_ttm', 'dv_ttm', 'total_mv',

        'kdj_qfq', 'kdj_d_qfq', 'kdj_k_qfq', 'rsi_qfq_12',

        'macd_qfq', 'macd_dea_qfq', 'macd_dif_qfq', 'atr_qfq', 'mtmma_qfq',

        'turnover_rate_f'

    ]




    MONEYFLOW_FIELDS = [

        'trade_date', 'ts_code', 'net_amount', 'buy_elg_amount',

        'buy_lg_amount', 'buy_md_amount', 'buy_sm_amount'

    ]




    MARGIN_FIELDS = [

        'trade_date', 'ts_code', 'rzye', 'rqye'

    ]




    FINA_INDICATOR_FIELDS = [

        'ts_code', 'end_date', 'roe', 'roa', 'roa2_yearly',

        'profit_to_gr', 'q_profit_yoy', 'q_eps', 'assets_turn'

    ]



    def __init__(self, token: str, output_dir: str, start_date: str, end_date: str,

                 max_stocks: int = None, high_speed: bool = False,

                 csv_dir: str = None, download_workers: int = 4):

        """

        ?

        Args?            token: Tushare API token

            output_dir: Qlib

            start_date: start date(YYYYMMDD)

            end_date: end date (YYYYMMDD)

            max_stocks: one?            high_speed: ?000+?            csv_dir: CSVoutput_dir/csv_data?        """





        self.rate_limiter = APIRateLimiter(high_speed=high_speed)
        self.fetcher = TushareDataFetcher(token, rate_limiter=self.rate_limiter)

        self.converter = QlibDataConverter(output_dir)

        self.start_date = start_date

        self.end_date = end_date

        self.max_stocks = max_stocks

        self.download_workers = max(1, int(download_workers))

        if csv_dir:
            self.csv_dir = Path(csv_dir)
        else:
            self.csv_dir = Path(r"D:\qlib_data\csv_data")



        self.csv_dir.mkdir(parents=True, exist_ok=True)



        logger.info(f"data?(? {high_speed})")

        logger.info(f"CSVdata: {self.csv_dir}")

        logger.info(f"Qlib: {self.converter.output_dir}")

        logger.info(f"? {self.download_workers}")



    def _download_single_stock(self, ts_code: str) -> str:

        """

        ?        

            "success" | "skip" | "fail"

        """

        try:
            csv_filename = f"{ts_code.lower()}.csv"
            csv_path = self.csv_dir / csv_filename
            if csv_path.exists():
                return "skip"

            df = self.fetch_all_data_for_stock(ts_code)
            if df.empty:
                return "fail"
            return "success" if self.save_stock_to_csv(ts_code, df) else "fail"
        except Exception as e:
            logger.error(f" {ts_code} ? {e}")

            return "fail"



    def fetch_all_data_for_stock(self, ts_code: str) -> pd.DataFrame:

        """

        Get



        Args?            ts_code: stock code



        ?            DataFrame

        """

        logger.info(f"\n{'='*60}")

        logger.info(f": {ts_code}")

        logger.info(f"{'='*60}")




        logger.info("  [1/4] Getdata...")

        factor_df = self.fetcher.get_stk_factor_pro(

            ts_code, self.start_date, self.end_date,

            self.STK_FACTOR_FIELDS

        )



        if factor_df.empty:

            logger.warning(f"  {ts_code} ")

            return pd.DataFrame()




        factor_df['datetime'] = pd.to_datetime(factor_df['trade_date'])

        factor_df.set_index(['datetime'], inplace=True)



        # 2. Get money flow data

        logger.info("  [2/4] Get money flow data...")

        money_df = self.fetcher.get_moneyflow_dc(

            ts_code, self.start_date, self.end_date,

            self.MONEYFLOW_FIELDS

        )



        if not money_df.empty:

            money_df['datetime'] = pd.to_datetime(money_df['trade_date'])

            money_df.set_index(['datetime'], inplace=True)


            factor_df = factor_df.join(money_df[

                ['net_amount', 'buy_elg_amount', 'buy_lg_amount',

                 'buy_md_amount', 'buy_sm_amount']

            ], how='left')

        else:

            logger.info("  ")



        # 3. Get margin trading data

        logger.info("  [3/4] Get margin trading data...")

        margin_df = self.fetcher.get_margin_detail(

            ts_code, self.start_date, self.end_date,

            self.MARGIN_FIELDS

        )



        if not margin_df.empty:

            margin_df['datetime'] = pd.to_datetime(margin_df['trade_date'])

            margin_df.set_index(['datetime'], inplace=True)


            factor_df = factor_df.join(margin_df[['rzye', 'rqye']], how='left')

        else:

            logger.info("  ")



        # 4. Get financial indicator data

        logger.info("  [4/4] Get financial indicator data...")

        fina_df = self.fetcher.get_fina_indicator(

            ts_code, self.start_date, self.end_date,

            self.FINA_INDICATOR_FIELDS

        )



        if not fina_df.empty:
            if 'end_date' in fina_df.columns and 'datetime' not in fina_df.columns:
                fina_df['datetime'] = pd.to_datetime(fina_df['end_date'])
            fina_df.set_index(['datetime'], inplace=True)




            factor_df_index = factor_df.index

            if hasattr(factor_df_index, 'to_frame'):

                pass

            else:

                if isinstance(factor_df.index, pd.DatetimeIndex):

                    factor_df.index = factor_df.index.normalize()

                else:

                    factor_df.index = pd.to_datetime(factor_df.index).dt.normalize()




            factor_df = factor_df.join(fina_df[

                ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',

                 'q_profit_yoy', 'q_eps', 'assets_turn']

            ], how='left')




            roe_col = 'roe' if 'roe' in factor_df.columns else None

            if roe_col and factor_df[roe_col].notna().any():

                logger.info(f"  ?columns{factor_df[roe_col].notna().sum()} ")

            elif roe_col:

                logger.warning(f"    joinfinancial indicatorsdataerge...")




                fina_for_merge = fina_df.reset_index()

                fina_for_merge.rename(columns={'datetime': 'datetime_fina'}, inplace=True)



                factor_reset = factor_df.reset_index()

                factor_reset['datetime'] = pd.to_datetime(factor_reset['datetime']).dt.date

                fina_for_merge['datetime_fina'] = pd.to_datetime(fina_for_merge['datetime_fina']).dt.date




                merged_data = []

                for idx, row in factor_reset.iterrows():


                    if not matching_fina.empty:


                        for col in ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',

                                    'q_profit_yoy', 'q_eps', 'assets_turn']:

                            if col in latest_fina.index:

                                row[col] = latest_fina[col]

                    merged_data.append(row)



                factor_df = pd.DataFrame(merged_data)

                factor_df.set_index(['instrument', 'datetime'], inplace=True)



                if 'roe' in factor_df.columns and factor_df['roe'].notna().any():

                    logger.info(f"  ?merge {factor_df['roe'].notna().sum()} ")

            else:

                logger.warning("    OE")




                factor_df = factor_df.sort_index()

                fina_cols = ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',

                             'q_profit_yoy', 'q_eps', 'assets_turn']

                existing_cols = [col for col in fina_cols if col in factor_df.columns]

                if existing_cols:


                    before_fill = {col: factor_df[col].notna().sum() for col in existing_cols}




                    factor_df[existing_cols] = factor_df[existing_cols].ffill()




                    after_fill = {col: factor_df[col].notna().sum() for col in existing_cols}




                    fill_info = ', '.join([f"{col}:{before_fill[col]}->{after_fill[col]}" for col in existing_cols])

                    logger.info(f"  ?financial indicators ({fill_info})")

        else:

            logger.info("    ")




        factor_df['instrument'] = ts_code

        factor_df.reset_index(inplace=True)

        factor_df.set_index(['instrument', 'datetime'], inplace=True)



        logger.info(f"  {ts_code} dataGet {len(factor_df)} ")

        return factor_df



    def save_stock_to_csv(self, ts_code: str, df: pd.DataFrame) -> bool:

        """

        CSV



        Args?            ts_code: stock code (?'000001.SZ')

            df: dataDataFrame



        ?            

        """

        if df.empty:

            return False



        try:


            csv_filename = f"{ts_code.lower()}.csv"

            csv_path = self.csv_dir / csv_filename




            df_to_save = df.reset_index()

            add_qlib_standard_ohlcv_columns(df_to_save)




            df_to_save.to_csv(csv_path, index=False, encoding='utf-8-sig')



            logger.info(f"   ? {csv_path.name} ({len(df)} ?")

            return True



        except Exception as e:

            logger.error(f" {ts_code} CSV: {e}")

            return False



    def download_all_to_csv(self, stock_codes: List[str] = None) -> Tuple[int, int]:

        """

        1SV



        Args?            stock_codes: stock codeone?

        ?            (, )

        """

        logger.info("\n" + "="*80)

        logger.info("?/2CSV")

        logger.info("="*80)

        logger.info(f": {self.start_date} ~ {self.end_date}")

        logger.info(f"CSV: {self.csv_dir}")

        logger.info(f"rate limit: {'?8000+)' if self.rate_limiter.high_speed else '(5000)'}")



        # Get stock list

        stock_list = self.fetcher.get_stock_list()



        if stock_codes is None:

            stock_codes = stock_list['ts_code'].tolist()




            stock_codes = stock_codes[:self.max_stocks]

            logger.info(f"?{self.max_stocks} ")



        logger.info(f"? {len(stock_codes)}")



        success_count = 0

        fail_count = 0

        skipped_count = 0

        total = len(stock_codes)



        workers = min(self.download_workers, total) if total > 0 else 1

        logger.info(f": {'' if workers == 1 else f'({workers})'}")



        if workers == 1:

            pbar = tqdm(stock_codes, total=total, desc="CSV", unit="stock")

            for i, ts_code in enumerate(pbar, 1):

                status = self._download_single_stock(ts_code)

                if status == "success":

                    success_count += 1

                elif status == "skip":

                    success_count += 1

                    skipped_count += 1

                else:

                    fail_count += 1



                pbar.set_postfix(

                    success=success_count,

                    fail=fail_count,

                    skip=skipped_count,

                    refresh=False,

                )

                if i % 50 == 0:

                    self._print_rate_stats()

                    logger.info(f"\n : ={success_count}, ={fail_count}, ={skipped_count}")

            pbar.close()

        else:

            futures = {}

            with ThreadPoolExecutor(max_workers=workers) as executor:

                for ts_code in stock_codes:

                    futures[executor.submit(self._download_single_stock, ts_code)] = ts_code



                pbar = tqdm(total=total, desc="CSV", unit="stock")

                for i, future in enumerate(as_completed(futures), 1):

                    status = future.result()

                    if status == "success":

                        success_count += 1

                    elif status == "skip":

                        success_count += 1

                        skipped_count += 1

                    else:

                        fail_count += 1



                    pbar.update(1)

                    pbar.set_postfix(

                        success=success_count,

                        fail=fail_count,

                        skip=skipped_count,

                        refresh=False,

                    )

                    if i % 50 == 0:

                        self._print_rate_stats()

                        logger.info(f"\n : ={success_count}, ={fail_count}, ={skipped_count}")

                pbar.close()



        logger.info(f"\n{'='*60}")

        logger.info("?SV:")

        logger.info(f"  ?: {success_count} ")

        logger.info(f"    (?: {skipped_count} ")

        logger.info(f"  ?: {fail_count} ")

        logger.info(f"   CSV? {self.csv_dir}")



        return success_count, fail_count



    def convert_csv_to_qlib(self, dump_mode: str = "dump_all") -> bool:

        """

        2?qlib  scripts/dump_bin.py  DumpDataAll  Qlib ?

        ?features/<stock code>/<>.day.bin qlib LocalFeatureProvider

         features/<>.day.bin ?

        Args:
            dump_mode: "dump_all" or "dump_update"
                dump_all: rebuild all data (calendars, instruments, features)
                dump_update: incremental update, only append new dates
        """

        logger.info("\n" + "="*80)

        logger.info("?/2CSVlib?dump_bin.DumpDataAll")

        logger.info("="*80)

        logger.info(f"CSV: {self.csv_dir}")

        logger.info(f"Qlib: {self.converter.output_dir}")



        csv_files = sorted(self.csv_dir.glob("*.csv"))

        if not csv_files:

            logger.error("SV?")

            return False



        logger.info(f" {len(csv_files)} SV")

        sample = pd.read_csv(csv_files[0], nrows=512, encoding="utf-8-sig")

        skip_cols = {"datetime", "instrument", "trade_date", "ts_code"}

        feature_cols = [

            c

            for c in sample.columns

            if c not in skip_cols and pd.api.types.is_numeric_dtype(sample[c])

        ]

        if not feature_cols:

            logger.error("SVdata")

            return False



        include_fields = ",".join(feature_cols)

        logger.info(f"?dump ? {len(feature_cols)}")



        qlib_main = Path(os.environ.get("QLIB_SOURCE", r"D:\quant_project\qlib-main"))

        scripts_dir = qlib_main / "scripts"

        dump_script = scripts_dir / "dump_bin.py"

        if not dump_script.is_file():

            logger.error(
                "Cannot find qlib dump_bin.py: %s. Set QLIB_SOURCE env or install qlib",
                dump_script,
            )

            return False



        if str(scripts_dir) not in sys.path:

            sys.path.insert(0, str(scripts_dir))

        out_dir = str(self.converter.output_dir)

        try:

            from dump_bin import DumpDataAll

        except ImportError as exc:

            logger.warning(
                "Cannot import dump_bin directly: %s. Trying conda env qlib_zhengshi...",
                exc,
            )

            try:
                import subprocess
                cmd = [
                    "conda", "run", "-n", "qlib_zhengshi",
                    "python", str(dump_script), dump_mode,
                    f"--data_path={self.csv_dir}",
                    f"--qlib_dir={out_dir}",
                    f"--include_fields={include_fields}",
                    "--date_field_name=datetime",
                    "--symbol_field_name=instrument",
                    "--freq=day",
                    f"--max_workers={min(16, (os.cpu_count() or 4))}",
                ]
                logger.info("Running: %s", " ".join(cmd))
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
                if result.stdout:
                    logger.info(result.stdout)
                if result.stderr:
                    logger.warning(result.stderr)
                if result.returncode != 0:
                    logger.error("conda run dump_bin failed with return code %d", result.returncode)
                    return False
                logger.info("conda run dump_bin completed successfully")
                return True
            except Exception as sub_exc:
                logger.error("conda run dump_bin also failed: %s", sub_exc)
                return False

        max_workers = min(16, (os.cpu_count() or 4))
        try:
            if dump_mode == "dump_update":
                from dump_bin import DumpDataUpdate
                dumper = DumpDataUpdate(
                    data_path=str(self.csv_dir),
                    qlib_dir=out_dir,
                    date_field_name="datetime",
                    symbol_field_name="instrument",
                    include_fields=include_fields,
                    freq="day",
                    max_workers=max_workers,
                )
            else:
                from dump_bin import DumpDataAll
                dumper = DumpDataAll(
                    data_path=str(self.csv_dir),
                    qlib_dir=out_dir,
                    date_field_name="datetime",
                    symbol_field_name="instrument",
                    include_fields=include_fields,
                    freq="day",
                    max_workers=max_workers,
                )
            dumper.dump()
        except Exception:
            logger.exception("dump_bin ")
            return False



        logger.info("\n" + "="*80)

        logger.info("?lib: calendars/ + instruments/ + features/<>/")

        logger.info("="*80)

        logger.info(" Qlibdata: %s", self.converter.output_dir)

        logger.info("? qlib.init(provider_uri=r'%s')", out_dir)



        return True



    def run(self, stock_codes: List[str] = None, dump_mode: str = "dump_all"):

        """



        Args?            stock_codes: stock codeone?
            dump_mode: "dump_all" or "dump_update"
        """

        logger.info("\n" + "="*80)

        logger.info("")

        logger.info("="*80)




        success, fail = self.download_all_to_csv(stock_codes)



        if success == 0 and fail == 0:

            logger.error("Get")

            return




        if success > 0:

            self.convert_csv_to_qlib(dump_mode=dump_mode)

        else:

            logger.warning("Qlib")






    def _print_rate_stats(self):

        """"""

        stats = self.rate_limiter.get_stats()

        logger.info("\n API??")

        for api_name, info in stats.items():

            logger.info(

                f"   {api_name:20s}: "

                f"{info['recent_requests']:3d}?/ "

                f"{info['rate_limit']:5.1f}? "

                f"(? {info['utilization']:5.1f}%)"

            )



    def _print_final_stats(self, success_count: int, fail_count: int, merged_df: Optional[pd.DataFrame]):
        """Print final statistics"""
        logger.info(f"Final stats: success={success_count}, fail={fail_count}")


def main():
    parser = argparse.ArgumentParser(description='Tushare to Qlib data pipeline')
    parser.add_argument('--token', type=str, default=None,
                        help='Tushare Pro API Token (env: TUSHARE_API_KEY)')
    parser.add_argument('--start_date', type=str, default='20200101',
                        help='start date(YYYYMMDD), default: 20200101')
    parser.add_argument('--end_date', type=str, default='20231231',
                        help='end date (YYYYMMDD), default: 20231231')
    parser.add_argument('--output_dir', type=str,
                        default=r'D:\qlib_data\qlib_data',
                        help=f'Qlib data output dir, default: D:\\qlib_data\\qlib_data')
    parser.add_argument('--csv_dir', type=str, default=r'D:\qlib_data\csv_data',
                        help='CSV temp dir, default: D:\\qlib_data\\csv_data')
    parser.add_argument('--max_stocks', type=int, default=None,
                        help='max stocks to process')
    parser.add_argument('--high-speed', action='store_true',
                        help='high speed mode (requires 8000+ Tushare points)')
    parser.add_argument('--download_workers', type=int, default=4,
                        help='download workers, default: 4 (suggest 2-8)')
    parser.add_argument('--stage', type=str, default='all',
                        choices=['all', 'download', 'convert'],
                        help='stage: all/download/convert, default: all')



    args = parser.parse_args()






    if not token:


        if token:

            logger.info(f"??TUSHARE_API_KEY Getoken")



    if not token:


        token = os.environ.get('TUSHARE_TOKEN', '').strip()

        if token:

            logger.info("  ?TUSHARE_TOKEN Getoken?TUSHARE_API_KEY")



    if not token:


        token = get_tushare_token()



    if not token:
        print("\nError: Tushare API Token not found!")
        print("\nPlease set token via one of:")
        print("  1. Command line: --token YOUR_TOKEN")
        print("  2. Environment: set TUSHARE_API_KEY=YOUR_TOKEN")
        print("  3. Config file: config/secrets.yaml")
        print("\nExample:")
        print("  set TUSHARE_API_KEY=your_token_here")
        print("  python tushare_to_qlib.py --start_date 20230101 --end_date 20231231\n")
        sys.exit(1)




    try:

        pd.to_datetime(args.start_date)

        pd.to_datetime(args.end_date)

    except ValueError:

        logger.error(" YYYYMMDD ")

        sys.exit(1)




    pipeline = DataPipeline(

        token=token,

        output_dir=args.output_dir,

        start_date=args.start_date,

        end_date=args.end_date,

        max_stocks=args.max_stocks,

        high_speed=args.high_speed,

        csv_dir=args.csv_dir,

        download_workers=args.download_workers

    )




    if args.stage == 'all':


        pipeline.run()

    elif args.stage == 'download':


        success, fail = pipeline.download_all_to_csv()

        print(f"\n ? {success}, : {fail}")

        print(f" CSV: {pipeline.csv_dir}")

    elif args.stage == 'convert':


        success = pipeline.convert_csv_to_qlib()

        if not success:

            sys.exit(1)





if __name__ == '__main__':

    main()



