#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tushare鏁版嵁鑾峰彇骞惰浆鎹负Qlib鏍煎紡鐨勮剼鏈?
鍔熻兘锛?1. 浠嶵ushare鑾峰彇澶氫釜鎺ュ彛鐨勬暟鎹紙鎶€鏈洜瀛愩€佽祫閲戞祦鍚戙€佽瀺璧勮瀺鍒搞€佽储鍔℃寚鏍囷級
2. 灏嗘暟鎹悎骞跺苟杞崲涓篞lib鏍囧噯鏍煎紡锛?bin鏂囦欢锛?3. 淇濆瓨鍒版寚瀹氱洰褰曪細D:/qlib_data/qlib_data

浣跨敤鏂规硶锛?    python tushare_to_qlib.py --start_date 20200101 --end_date 20260424
    鎴栬缃幆澧冨彉閲?TUSHARE_API_KEY 鍚庣洿鎺ヨ繍琛?"""

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

# Qlib 甯哥敤鐗瑰緛鍚?鈫?鏁版嵁婧愬垪锛堝墠澶嶆潈 OHLC锛泇olume 浣跨敤閲忔瘮锛屼笌 vol 鎵嬫暟鏃犲叧锛?QLIB_STD_OHLCV_FROM: Dict[str, str] = {
    "open": "open_qfq",
    "high": "high_qfq",
    "low": "low_qfq",
    "close": "close_qfq",
    "volume": "volume_ratio",
}


def add_qlib_standard_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    涓?DataFrame 澧炲姞 open銆乭igh銆乴ow銆乧lose銆乿olume 鍒楋紝鍐呭鍒嗗埆鏉ヨ嚜
    open_qfq銆乭igh_qfq銆乴ow_qfq銆乧lose_qfq銆乿olume_ratio銆?    涓嶅垹闄ゆ垨閲嶅懡鍚嶅師鏈夊垪锛涙簮鍒椾笉瀛樺湪鏃惰烦杩囧搴旂洰鏍囧垪銆?    """
    if df is None or len(df) == 0:
        return df
    for target, source in QLIB_STD_OHLCV_FROM.items():
        if source in df.columns:
            df[target] = df[source]
    return df


class APIRateLimiter:
    """
    Tushare API 棰戠巼闄愬埗鍣?
    鏍规嵁鍚勬帴鍙ｇ殑瀹樻柟棰戠巼闄愬埗鑷姩鎺у埗璇锋眰閫熺巼锛?    鏀寔鍔ㄦ€佽皟鏁村拰閿欒閲嶈瘯鏈哄埗銆?
    瀹樻柟棰戠巼闄愬埗锛堟潵鑷猅ushare鏂囨。锛夛細
    - stk_factor_pro: 5000绉垎=30娆?鍒嗛挓, 8000+绉垎=500娆?鍒嗛挓
    - moneyflow_dc: 5000绉垎锛堜繚瀹堜及璁?0娆?鍒嗛挓锛?    - margin_detail: 2000绉垎锛堜繚瀹堜及璁?0娆?鍒嗛挓锛?    - fina_indicator: 2000绉垎锛堜繚瀹堜及璁?0娆?鍒嗛挓锛?    - stock_basic: 鍩虹鎺ュ彛锛堣緝瀹芥澗锛?    """

    # 鍚勬帴鍙ｇ殑棰戠巼閰嶇疆 (璇锋眰娆℃暟/鍒嗛挓)
    RATE_LIMITS = {
        'stk_factor_pro': {
            'normal': 30,      # 5000绉垎鐢ㄦ埛
            'high': 500,       # 8000+绉垎鐢ㄦ埛
            'current': 30      # 褰撳墠浣跨敤鐨勯€熺巼
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
            'normal': 20,      # 璐㈠姟鎸囨爣鎺ュ彛鏇翠弗鏍?            'high': 40,
            'current': 20
        },
        'stock_basic': {
            'normal': 60,
            'high': 120,
            'current': 60
        }
    }

    # 閿欒閲嶈瘯閰嶇疆
    MAX_RETRIES = 3
    RETRY_DELAYS = [1, 2, 4]  # 閲嶈瘯寤惰繜锛堢锛夛紝鎸囨暟閫€閬?
    def __init__(self, high_speed: bool = False):
        """
        鍒濆鍖栭鐜囬檺鍒跺櫒

        鍙傛暟锛?            high_speed: 鏄惁浣跨敤楂橀€熸ā寮忥紙闇€瑕?000+绉垎锛?        """
        self.high_speed = high_speed
        self.request_history: Dict[str, deque] = {}
        self.mode = 'high' if high_speed else 'normal'
        # 骞跺彂鍦烘櫙涓嬫寜鎺ュ彛鍔犻攣锛岀‘淇濆悓涓€鎺ュ彛鐨勯檺棰戦€昏緫绾跨▼瀹夊叏
        self._api_locks: Dict[str, threading.Lock] = {}

        # 鍒濆鍖栨瘡涓帴鍙ｇ殑璇锋眰鍘嗗彶璁板綍
        for api_name in self.RATE_LIMITS.keys():
            self.request_history[api_name] = deque(maxlen=1000)
            self._api_locks[api_name] = threading.Lock()

            # 璁剧疆褰撳墠閫熺巼
            self.RATE_LIMITS[api_name]['current'] = self.RATE_LIMITS[api_name][self.mode]

        logger.info(f"API棰戠巼闄愬埗鍣ㄥ垵濮嬪寲瀹屾垚 (妯″紡: {self.mode})")

    def _get_interval(self, api_name: str) -> float:
        """
        璁＄畻涓や釜璇锋眰涔嬮棿鐨勬渶灏忛棿闅旀椂闂达紙绉掞級

        鍙傛暟锛?            api_name: 鎺ュ彛鍚嶇О

        杩斿洖锛?            鏈€灏忛棿闅旀椂闂达紙绉掞級
        """
        if api_name not in self.RATE_LIMITS:
            return 2.0  # 榛樿2绉掗棿闅?
        rate_per_minute = self.RATE_LIMITS[api_name]['current']
        interval = 60.0 / rate_per_minute

        # 娣诲姞10%鐨勫畨鍏ㄤ綑閲?        interval *= 1.1

        return interval

    def wait_if_needed(self, api_name: str):
        """
        濡傛灉闇€瑕侊紝绛夊緟鐩村埌鍙互鍙戦€佷笅涓€涓姹?
        鍙傛暟锛?            api_name: 鎺ュ彛鍚嶇О
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
                logger.debug(f"[{api_name}] 绛夊緟 {wait_time:.2f} 绉掍互绗﹀悎棰戠巼闄愬埗")
                time.sleep(wait_time)

    def record_request(self, api_name: str):
        """璁板綍涓€娆PI璇锋眰"""
        if api_name in self.request_history:
            self.request_history[api_name].append(time.time())

    def execute_with_rate_limit(self, api_name: str, func, *args, **kwargs):
        """
        鍦ㄩ鐜囬檺鍒朵笅鎵цAPI璋冪敤锛屾敮鎸佽嚜鍔ㄩ噸璇?
        鍙傛暟锛?            api_name: 鎺ュ彛鍚嶇О
            func: 瑕佹墽琛岀殑鍑芥暟
            *args, **kwargs: 鍑芥暟鍙傛暟

        杩斿洖锛?            鍑芥暟鎵ц缁撴灉

        寮傚父锛?            杩炵画閲嶈瘯澶辫触鍚庢姏鍑烘渶鍚庝竴涓紓甯?        """
        last_exception = None

        for attempt in range(self.MAX_RETRIES):
            try:
                # 骞跺彂鍦烘櫙锛氬悓涓€鎺ュ彛涓茶鑺傛祦锛岄伩鍏嶇珵浜夊鑷寸殑瓒呴
                api_lock = self._api_locks.get(api_name)
                if api_lock is None:
                    api_lock = threading.Lock()
                    self._api_locks[api_name] = api_lock
                with api_lock:
                    # 绛夊緟鐩村埌鍙互鍙戦€佽姹?                    self.wait_if_needed(api_name)
                    # 璁板綍璇锋眰鏃堕棿
                    self.record_request(api_name)

                # 鎵ц瀹為檯鐨凙PI璋冪敤
                result = func(*args, **kwargs)

                return result

            except Exception as e:
                last_exception = e
                error_msg = str(e).lower()

                # 妫€鏌ユ槸鍚︽槸棰戠巼闄愬埗閿欒
                is_rate_limit_error = any(keyword in error_msg for keyword in [
                    'limit', 'too many', '棰戠巼', '闄愭祦', '璇锋眰杩囧揩'
                ])

                if is_rate_limit_error and attempt < self.MAX_RETRIES - 1:
                    # 濡傛灉鏄鐜囬檺鍒堕敊璇紝澧炲姞绛夊緟鏃堕棿骞堕噸璇?                    delay = self.RETRY_DELAYS[min(attempt, len(self.RETRY_DELAYS)-1)]
                    logger.warning(
                        f"[{api_name}] 鍙兘瑙﹀彂棰戠巼闄愬埗 ({attempt+1}/{self.MAX_RETRIES}): {e}"
                        f"\n   绛夊緟 {delay} 绉掑悗閲嶈瘯..."
                    )
                    time.sleep(delay)

                    # 鍔ㄦ€侀檷浣庤鎺ュ彛鐨勯€熺巼
                    if api_name in self.RATE_LIMITS:
                        current_rate = self.RATE_LIMITS[api_name]['current']
                        new_rate = max(5, current_rate * 0.7)  # 闄嶄綆30%锛屾渶浣?娆?鍒嗛挓
                        self.RATE_LIMITS[api_name]['current'] = new_rate
                        logger.info(
                            f"[{api_name}] 鑷姩闄嶄綆閫熺巼: "
                            f"{current_rate:.1f} -> {new_rate:.1f} 娆?鍒嗛挓"
                        )

                elif attempt < self.MAX_RETRIES - 1:
                    # 鍏朵粬閿欒锛屾甯搁噸璇?                    delay = self.RETRY_DELAYS[min(attempt, len(self.RETRY_DELAYS)-1)]
                    logger.warning(
                        f"[{api_name}] 璇锋眰澶辫触 ({attempt+1}/{self.MAX_RETRIES}): {e}"
                        f"\n   绛夊緟 {delay} 绉掑悗閲嶈瘯..."
                    )
                    time.sleep(delay)

        # 鎵€鏈夐噸璇曢兘澶辫触
        logger.error(f"[{api_name}] 缁忚繃 {self.MAX_RETRIES} 娆￠噸璇曞悗浠嶇劧澶辫触")
        raise last_exception

    def get_stats(self) -> Dict:
        """鑾峰彇缁熻淇℃伅"""
        stats = {}
        now = time.time()

        for api_name, history in self.request_history.items():
            # 缁熻鏈€杩?鍒嗛挓鐨勮姹傛暟
            recent_count = sum(1 for t in history if now - t <= 60)
            rate_limit = self.RATE_LIMITS[api_name]['current']

            stats[api_name] = {
                'recent_requests': recent_count,
                'rate_limit': rate_limit,
                'utilization': recent_count / rate_limit * 100 if rate_limit > 0 else 0
            }

        return stats


def get_tushare_token() -> Optional[str]:
    """
    鑾峰彇Tushare Token锛堟寜浼樺厛绾э級

    浼樺厛绾ч『搴忥細
    1. 鍛戒护琛屽弬鏁?--token
    2. 鐜鍙橀噺 TUSHARE_API_KEY
    3. 鐜鍙橀噺 TUSHARE_TOKEN
    4. 閰嶇疆鏂囦欢 config/secrets.yaml

    杩斿洖锛?        Token瀛楃涓叉垨None
    """

    # 鏂规硶1 & 2: 鐢眒ain()鍑芥暟澶勭悊鍛戒护琛屽弬鏁板拰鐜鍙橀噺
    # 杩欓噷鍙鐞嗗鐢ㄦ柟娉?
    # 鏂规硶3: 鐜鍙橀噺 TUSHARE_TOKEN锛堝吋瀹规棫鐗堟湰锛?    token = os.environ.get('TUSHARE_TOKEN', '').strip()
    if token:
        logger.info("浠庣幆澧冨彉閲?TUSHARE_TOKEN 鑾峰彇鍒癟oken")
        return token

    # 鏂规硶4: 閰嶇疆鏂囦欢
    try:
        import yaml
        config_path = Path('config/secrets.yaml')
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                if data and 'tushare' in data:
                    token = data['tushare'].get('token', '').strip()
                    if token:
                        logger.info("浠庨厤缃枃浠?config/secrets.yaml 鑾峰彇鍒癟oken")
                        return token
    except Exception:
        pass

    return None


class TushareDataFetcher:
    """Tushare鏁版嵁鑾峰彇鍣紙闆嗘垚API棰戠巼闄愬埗锛?""

    def __init__(self, token: str, cache_dir: str = "data/tushare_cache",
                 rate_limiter: APIRateLimiter = None):
        self.token = token
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limiter = rate_limiter

        try:
            import tushare as ts
            self.ts = ts
            self.pro = ts.pro_api(token)
            logger.info("Tushare Pro API 鍒濆鍖栨垚鍔?)
        except Exception as e:
            logger.error(f"鍒濆鍖朤ushare澶辫触: {e}")
            raise

    def _cache_path(self, name: str) -> Path:
        return self.cache_dir / name

    def _load_cache(self, name: str) -> Optional[pd.DataFrame]:
        """鍔犺浇缂撳瓨鏁版嵁"""
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
        """淇濆瓨缂撳瓨鏁版嵁"""
        p = self._cache_path(name)
        try:
            df.to_parquet(p, index=False)
        except Exception:
            df.to_csv(p.with_suffix('.csv'), index=False)

    def get_stock_list(self) -> pd.DataFrame:
        """鑾峰彇鑲＄エ鍒楄〃"""
        cache_name = "stock_list.parquet"
        cached = self._load_cache(cache_name)

        # 瀹夊叏妫€鏌ョ紦瀛樻暟鎹?        if cached is not None and len(cached) > 0:
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

        # 瀹夊叏妫€鏌ヨ繑鍥炴暟鎹?        if df is not None and len(df) > 0:
            self._save_cache(cache_name, df)

        return df if df is not None else pd.DataFrame()

    def get_stk_factor_pro(self, ts_code: str, start_date: str, end_date: str,
                           fields: List[str]) -> pd.DataFrame:
        """
        鑾峰彇鎶€鏈潰鍥犲瓙鏁版嵁 (stk_factor_pro鎺ュ彛)

        棰戠巼闄愬埗锛?000绉垎=30娆?鍒嗛挓, 8000+绉垎=500娆?鍒嗛挓

        鍙傛暟锛?            ts_code: 鑲＄エ浠ｇ爜
            start_date: 寮€濮嬫棩鏈?(YYYYMMDD)
            end_date: 缁撴潫鏃ユ湡 (YYYYMMDD)
            fields: 闇€瑕佺殑瀛楁鍒楄〃
        """
        cache_name = f"stk_factor_{ts_code}_{start_date}_{end_date}.parquet"
        cached = self._load_cache(cache_name)

        # 瀹夊叏妫€鏌ョ紦瀛樻暟鎹?        if cached is not None and len(cached) > 0:
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

            # 璋冭瘯鏃ュ織锛氭樉绀鸿繑鍥炴暟鎹殑绫诲瀷鍜屽舰鐘?            logger.debug(f"[{ts_code}] stk_factor_pro 杩斿洖绫诲瀷: {type(df)}, 鍊? {df}")

            # 瀹夊叏妫€鏌ワ細纭繚df鏄湁鏁堢殑DataFrame
            if df is None:
                logger.debug(f"[{ts_code}] stk_factor_pro 杩斿洖None")
                return pd.DataFrame()

            # 杞崲涓篋ataFrame锛堥槻姝㈣繑鍥濻eries鎴栧叾浠栫被鍨嬶級
            if not isinstance(df, pd.DataFrame):
                logger.warning(f"[{ts_code}] stk_factor_pro 杩斿洖闈濪ataFrame绫诲瀷: {type(df)}锛屽皾璇曡浆鎹?)
                df = pd.DataFrame(df)

            # 妫€鏌ユ槸鍚︿负绌?            if len(df) > 0:
                logger.info(f"  鉁?鑾峰彇鍒?{ts_code} 鎶€鏈洜瀛愭暟鎹紝鍏?{len(df)} 鏉¤褰?)
                self._save_cache(cache_name, df)
            else:
                logger.info(f"  鈿狅笍  {ts_code} 鎶€鏈洜瀛愭暟鎹负绌猴紙鍙兘璇ユ椂闂存鏃犳暟鎹級")

            return df

        except Exception as e:
            logger.warning(f"鑾峰彇 {ts_code} 鎶€鏈洜瀛愬け璐? {e}")
            import traceback
            logger.debug(f"璇︾粏閿欒淇℃伅:\n{traceback.format_exc()}")
            return pd.DataFrame()

    def get_moneyflow_dc(self, ts_code: str, start_date: str, end_date: str,
                         fields: List[str]) -> pd.DataFrame:
        """
        鑾峰彇涓偂璧勯噾娴佸悜鏁版嵁 (moneyflow_dc鎺ュ彛)

        棰戠巼闄愬埗锛氫繚瀹堜及璁?0娆?鍒嗛挓锛堥渶瑕?000+绉垎锛?
        鍙傛暟锛?            ts_code: 鑲＄エ浠ｇ爜
            start_date: 寮€濮嬫棩鏈?(YYYYMMDD)
            end_date: 缁撴潫鏃ユ湡 (YYYYMMDD)
            fields: 闇€瑕佺殑瀛楁鍒楄〃
        """
        cache_name = f"moneyflow_{ts_code}_{start_date}_{end_date}.parquet"
        cached = self._load_cache(cache_name)

        # 瀹夊叏妫€鏌ョ紦瀛樻暟鎹?        if cached is not None and len(cached) > 0:
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

            # 瀹夊叏妫€鏌ワ細纭繚df鏄湁鏁堢殑DataFrame
            if df is None:
                return pd.DataFrame()

            # 杞崲涓篋ataFrame锛堥槻姝㈣繑鍥濻eries鎴栧叾浠栫被鍨嬶級
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            # 妫€鏌ユ槸鍚︿负绌?            if len(df) > 0:
                self._save_cache(cache_name, df)

            return df

        except Exception as e:
            logger.warning(f"鑾峰彇 {ts_code} 璧勯噾娴佸悜澶辫触: {e}")
            return pd.DataFrame()

    def get_margin_detail(self, ts_code: str, start_date: str, end_date: str,
                          fields: List[str]) -> pd.DataFrame:
        """
        鑾峰彇铻嶈祫铻嶅埜浜ゆ槗鏄庣粏 (margin_detail鎺ュ彛)

        棰戠巼闄愬埗锛氫繚瀹堜及璁?0娆?鍒嗛挓锛堥渶瑕?000+绉垎锛?
        鍙傛暟锛?            ts_code: 鑲＄エ浠ｇ爜
            start_date: 寮€濮嬫棩鏈?(YYYYMMDD)
            end_date: 缁撴潫鏃ユ湡 (YYYYMMDD)
            fields: 闇€瑕佺殑瀛楁鍒楄〃
        """
        cache_name = f"margin_{ts_code}_{start_date}_{end_date}.parquet"
        cached = self._load_cache(cache_name)

        # 瀹夊叏妫€鏌ョ紦瀛樻暟鎹?        if cached is not None and len(cached) > 0:
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

            # 瀹夊叏妫€鏌ワ細纭繚df鏄湁鏁堢殑DataFrame
            if df is None:
                return pd.DataFrame()

            # 杞崲涓篋ataFrame锛堥槻姝㈣繑鍥濻eries鎴栧叾浠栫被鍨嬶級
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            # 妫€鏌ユ槸鍚︿负绌?            if len(df) > 0:
                self._save_cache(cache_name, df)

            return df

        except Exception as e:
            logger.warning(f"鑾峰彇 {ts_code} 铻嶈祫铻嶅埜鏁版嵁澶辫触: {e}")
            return pd.DataFrame()

    def get_fina_indicator(self, ts_code: str, start_date: str, end_date: str,
                           fields: List[str]) -> pd.DataFrame:
        """
        鑾峰彇璐㈠姟鎸囨爣鏁版嵁 (fina_indicator鎺ュ彛)

        棰戠巼闄愬埗锛氫繚瀹堜及璁?0娆?鍒嗛挓锛堥渶瑕?000+绉垎锛屾寜鑲＄エ鏌ヨ锛?
        娉ㄦ剰锛氭鎺ュ彛姣忔鏈€澶氳繑鍥?00鏉¤褰曪紝涓斿彧鑳芥寜鍗曞彧鑲＄エ鏌ヨ

        鍙傛暟锛?            ts_code: 鑲＄エ浠ｇ爜
            start_date: 寮€濮嬫棩鏈?(YYYYMMDD)
            end_date: 缁撴潫鏃ユ湡 (YYYYMMDD)
            fields: 闇€瑕佺殑瀛楁鍒楄〃
        """
        cache_name = f"fina_ind_{ts_code}_{start_date}_{end_date}.parquet"
        cached = self._load_cache(cache_name)

        # 瀹夊叏妫€鏌ョ紦瀛樻暟鎹?        if cached is not None and len(cached) > 0:
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

            # 瀹夊叏妫€鏌ワ細纭繚df鏄湁鏁堢殑DataFrame
            if df is None:
                return pd.DataFrame()

            # 杞崲涓篋ataFrame锛堥槻姝㈣繑鍥濻eries鎴栧叾浠栫被鍨嬶級
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            # 妫€鏌ユ槸鍚︿负绌?            if len(df) > 0:
                self._save_cache(cache_name, df)

            return df

        except Exception as e:
            logger.warning(f"鑾峰彇 {ts_code} 璐㈠姟鎸囨爣澶辫触: {e}")
            return pd.DataFrame()


class QlibDataConverter:
    """Qlib鏍煎紡鏁版嵁杞崲鍣?""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 鍒涘缓Qlib鎵€闇€鐨勭洰褰曠粨鏋?        self.calendars_dir = self.output_dir / 'calendars'
        self.features_dir = self.output_dir / 'features'
        self.instruments_dir = self.output_dir / 'instruments'

        self.calendars_dir.mkdir(exist_ok=True)
        self.features_dir.mkdir(exist_ok=True)
        self.instruments_dir.mkdir(exist_ok=True)

        logger.info(f"Qlib鏁版嵁杈撳嚭鐩綍: {self.output_dir}")

    def save_calendar(self, dates: List[pd.Timestamp], freq: str = 'day'):
        """
        淇濆瓨浜ゆ槗鏃ュ巻

        鍙傛暟锛?            dates: 浜ゆ槗鏃ュ垪琛?            freq: 棰戠巼 (day/min绛?
        """
        calendar_path = self.calendars_dir / f'{freq}.txt'

        date_strings = [d.strftime('%Y-%m-%d') for d in sorted(dates)]
        with open(calendar_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(date_strings))

        logger.info(f"浜ゆ槗鏃ュ巻宸蹭繚瀛? {calendar_path}, 鍏?{len(date_strings)} 涓氦鏄撴棩")

    def save_instruments(self, stock_list: pd.DataFrame, start_date: str, end_date: str):
        """
        淇濆瓨鑲＄エ姹犱俊鎭?
        鍙傛暟锛?            stock_list: 鑲＄エ鍒楄〃DataFrame
            start_date: 寮€濮嬫棩鏈?            end_date: 缁撴潫鏃ユ湡
        """
        instruments_path = self.instruments_dir / 'all.txt'

        with open(instruments_path, 'w', encoding='utf-8') as f:
            for _, row in stock_list.iterrows():
                ts_code = row['ts_code']
                list_date = row.get('list_date', start_date)
                line = f"{ts_code}\t{list_date}\t{end_date}\n"
                f.write(line)

        logger.info(f"鑲＄エ姹犱俊鎭凡淇濆瓨: {instruments_path}, 鍏?{len(stock_list)} 鍙偂绁?)

    def dataframe_to_bin(
        self,
        df: pd.DataFrame,
        field: str,
        freq: str = 'day',
        source_field: Optional[str] = None,
    ):
        """
        灏嗗崟涓瓧娈电殑鏁版嵁杞崲涓篵in鏍煎紡

        Qlib bin鏍煎紡璇存槑锛?        - 绗竴琛屾槸鏃ユ湡绱㈠紩锛堝湪鏃ュ巻涓殑浣嶇疆锛?        - 鍚庣画鏄疄闄呮暟鎹€?        - 鏁版嵁绫诲瀷锛歠loat32锛堝皬绔簭锛?
        鍙傛暟锛?            df: DataFrame锛屽寘鍚玠atetime绱㈠紩鍜岀洰鏍囧瓧娈?            field: 杈撳嚭 bin 鏂囦欢鍚嶆墍鐢ㄥ瓧娈靛悕锛堝 open锛?            freq: 棰戠巼
            source_field: 鑻ユ寚瀹氾紝浠庤鍒楄鍙栨暟鍊硷紝浠嶅啓鍏?field 瀵瑰簲鐨?.bin 鏂囦欢
        """
        col = source_field if source_field is not None else field
        if col not in df.columns:
            logger.warning(f"瀛楁 {col} 涓嶅瓨鍦?)
            return

        bin_path = self.features_dir / f"{field.lower()}.{freq}.bin"

        # 鍑嗗鏁版嵁锛氱‘淇濇湁datetime绱㈠紩
        data = df[col].dropna()

        if len(data) == 0:
            logger.warning(f"瀛楁 {field} 娌℃湁鏈夋晥鏁版嵁")
            return

        # 璇诲彇鏃ュ巻浠ュ缓绔嬬储寮曟槧灏?        calendar_path = self.calendars_dir / f'{freq}.txt'
        if not calendar_path.exists():
            logger.error("璇峰厛淇濆瓨浜ゆ槗鏃ュ巻")
            return

        with open(calendar_path, 'r', encoding='utf-8') as f:
            calendar_dates = [line.strip() for line in f.readlines()]

        # 鍒涘缓鏃ユ湡鍒扮储寮曠殑鏄犲皠
        date_to_idx = {date: idx for idx, date in enumerate(calendar_dates)}

        # 涓烘瘡涓暟鎹偣鎵惧埌瀵瑰簲鐨勬棩鍘嗙储寮?        indices = []
        values = []

        for dt, val in data.items():
            if isinstance(dt, (pd.Timestamp, str)):
                date_str = dt.strftime('%Y-%m-%d') if isinstance(dt, pd.Timestamp) else str(dt)[:10]
                if date_str in date_to_idx:
                    indices.append(date_to_idx[date_str])
                    values.append(float(val))

        if len(indices) == 0:
            logger.warning(f"瀛楁 {field} 娌℃湁鍖归厤鍒颁氦鏄撴棩鍘嗙殑鏁版嵁")
            return

        # 鍒涘缓鏁扮粍锛氱涓€鍒楁槸鏃ユ湡绱㈠紩锛岀浜屽垪鏄€?        output_array = np.column_stack([indices, values]).astype('<f')

        # 鍐欏叆bin鏂囦欢
        output_array.tofile(str(bin_path.resolve()))

        logger.debug(f"瀛楁 {field} 宸茶浆鎹负bin鏍煎紡: {len(values)} 鏉¤褰?)

    def convert_and_save(self, merged_df: pd.DataFrame, stock_list: pd.DataFrame,
                         start_date: str, end_date: str):
        """
        灏嗗悎骞跺悗鐨勬暟鎹浆鎹负Qlib鏍煎紡骞朵繚瀛?
        鍙傛暟锛?            merged_df: 鍚堝苟鍚庣殑DataFrame锛堝寘鍚墍鏈夊瓧娈碉級
            stock_list: 鑲＄エ鍒楄〃
            start_date: 寮€濮嬫棩鏈?            end_date: 缁撴潫鏃ユ湡
        """
        logger.info("寮€濮嬭浆鎹负Qlib鏍煎紡...")

        # 1. 淇濆瓨浜ゆ槗鏃ュ巻
        dates = pd.to_datetime(merged_df.index.get_level_values('datetime').unique()).sort_values()
        self.save_calendar(dates.tolist())

        # 2. 淇濆瓨鑲＄エ姹犱俊鎭?        self.save_instruments(stock_list, start_date, end_date)

        # 3. 閫愪釜瀛楁杞崲涓篵in鏍煎紡
        # 鍩虹OHLCV瀛楁
        base_fields = ['open_qfq', 'high_qfq', 'low_qfq', 'close_qfq', 'vol', 'amount']

        # 鎵╁睍鎶€鏈寚鏍囧瓧娈?        tech_fields = [
            'turnover_rate', 'volume_ratio', 'pe', 'pb', 'ps', 'dv_ratio',
            'ps_ttm', 'pe_ttm', 'dv_ttm', 'total_mv',
            'kdj_qfq', 'kdj_d_qfq', 'kdj_k_qfq', 'rsi_qfq_12',
            'macd_qfq', 'macd_dea_qfq', 'macd_dif_qfq', 'atr_qfq', 'mtmma_qfq',
            'turnover_rate_f'
        ]

        # 璧勯噾娴佸悜瀛楁
        money_fields = [
            'net_amount', 'buy_elg_amount', 'buy_lg_amount',
            'buy_md_amount', 'buy_sm_amount'
        ]

        # 铻嶈祫铻嶅埜瀛楁
        margin_fields = ['rzye', 'rqye']

        # 璐㈠姟鎸囨爣瀛楁
        fina_fields = [
            'roe', 'roa', 'roa2_yearly', 'profit_to_gr',
            'q_profit_yoy', 'q_eps', 'assets_turn'
        ]

        all_fields = base_fields + tech_fields + money_fields + margin_fields + fina_fields

        # 杩囨护鍑哄疄闄呭瓨鍦ㄧ殑瀛楁
        available_fields = [f for f in all_fields if f in merged_df.columns]
        logger.info(f"灏嗚浆鎹?{len(available_fields)} 涓瓧娈?)

        for field in available_fields:
            try:
                self.dataframe_to_bin(merged_df, field)
            except Exception as e:
                logger.error(f"杞崲瀛楁 {field} 澶辫触: {e}")

        for target, source in QLIB_STD_OHLCV_FROM.items():
            if source not in merged_df.columns:
                continue
            try:
                self.dataframe_to_bin(merged_df, target, freq='day', source_field=source)
            except Exception as e:
                logger.error(f"杞崲鏍囧噯瀛楁 {target}锛堟潵鑷?{source}锛夊け璐? {e}")

        logger.info(f"Qlib鏍煎紡杞崲瀹屾垚锛佹暟鎹繚瀛樺湪: {self.output_dir}")


class DataPipeline:
    """鏁版嵁澶勭悊绠￠亾锛氫粠Tushare鑾峰彇鏁版嵁骞惰浆鎹负Qlib鏍煎紡锛堥泦鎴愭櫤鑳介鐜囨帶鍒讹級"""

    # stk_factor_pro鎺ュ彛闇€瑕佺殑瀛楁
    STK_FACTOR_FIELDS = [
        'ts_code', 'trade_date', 'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq',
        'vol', 'amount', 'turnover_rate', 'volume_ratio', 'pe', 'pb', 'ps',
        'dv_ratio', 'ps_ttm', 'pe_ttm', 'dv_ttm', 'total_mv',
        'kdj_qfq', 'kdj_d_qfq', 'kdj_k_qfq', 'rsi_qfq_12',
        'macd_qfq', 'macd_dea_qfq', 'macd_dif_qfq', 'atr_qfq', 'mtmma_qfq',
        'turnover_rate_f'
    ]

    # moneyflow_dc鎺ュ彛闇€瑕佺殑瀛楁
    MONEYFLOW_FIELDS = [
        'trade_date', 'ts_code', 'net_amount', 'buy_elg_amount',
        'buy_lg_amount', 'buy_md_amount', 'buy_sm_amount'
    ]

    # margin_detail鎺ュ彛闇€瑕佺殑瀛楁
    MARGIN_FIELDS = [
        'trade_date', 'ts_code', 'rzye', 'rqye'
    ]

    # fina_indicator鎺ュ彛闇€瑕佺殑瀛楁
    FINA_INDICATOR_FIELDS = [
        'ts_code', 'end_date', 'roe', 'roa', 'roa2_yearly',
        'profit_to_gr', 'q_profit_yoy', 'q_eps', 'assets_turn'
    ]

    def __init__(self, token: str, output_dir: str, start_date: str, end_date: str,
                 max_stocks: int = None, high_speed: bool = False,
                 csv_dir: str = None, download_workers: int = 4):
        """
        鍒濆鍖栨暟鎹閬?
        鍙傛暟锛?            token: Tushare API token
            output_dir: Qlib鏍煎紡杈撳嚭鐩綍
            start_date: 寮€濮嬫棩鏈?(YYYYMMDD)
            end_date: 缁撴潫鏃ユ湡 (YYYYMMDD)
            max_stocks: 鏈€澶у鐞嗚偂绁ㄦ暟閲忥紙鐢ㄤ簬娴嬭瘯锛孨one琛ㄧず鍏ㄩ儴锛?            high_speed: 鏄惁鍚敤楂橀€熸ā寮忥紙闇€瑕?000+绉垎锛?            csv_dir: CSV鏂囦欢淇濆瓨鐩綍锛堥粯璁や负output_dir/csv_data锛?        """
        # 鍒涘缓API棰戠巼闄愬埗鍣?        self.rate_limiter = APIRateLimiter(high_speed=high_speed)

        # 鍒涘缓鏁版嵁鑾峰彇鍣紙浼犲叆棰戠巼闄愬埗鍣級
        self.fetcher = TushareDataFetcher(token, rate_limiter=self.rate_limiter)
        self.converter = QlibDataConverter(output_dir)
        self.start_date = start_date
        self.end_date = end_date
        self.max_stocks = max_stocks
        self.download_workers = max(1, int(download_workers))

        # CSV鏁版嵁鐩綍锛堢敤浜庝腑闂村瓨鍌級
        # 榛樿缁熶竴鍒板浐瀹氱洰褰曪紝閬垮厤娴嬭瘯涓庢寮忔祦绋嬭矾寰勪笉涓€鑷淬€?        if csv_dir:
            self.csv_dir = Path(csv_dir)
        else:
            self.csv_dir = Path(r"D:\qlib_data\csv_data")

        self.csv_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"鏁版嵁绠￠亾鍒濆鍖栧畬鎴?(楂橀€熸ā寮? {high_speed})")
        logger.info(f"CSV鏁版嵁鐩綍: {self.csv_dir}")
        logger.info(f"Qlib杈撳嚭鐩綍: {self.converter.output_dir}")
        logger.info(f"涓嬭浇骞跺彂绾跨▼鏁? {self.download_workers}")

    def _download_single_stock(self, ts_code: str) -> str:
        """
        涓嬭浇骞朵繚瀛樺崟鍙偂绁ㄦ暟鎹€?        杩斿洖鍊硷細
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
            logger.error(f"澶勭悊 {ts_code} 鏃跺嚭閿? {e}")
            return "fail"

    def fetch_all_data_for_stock(self, ts_code: str) -> pd.DataFrame:
        """
        鑾峰彇鍗曞彧鑲＄エ鐨勬墍鏈夋暟鎹苟鍚堝苟

        鍙傛暟锛?            ts_code: 鑲＄エ浠ｇ爜

        杩斿洖锛?            鍚堝苟鍚庣殑DataFrame
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"姝ｅ湪澶勭悊鑲＄エ: {ts_code}")
        logger.info(f"{'='*60}")

        # 1. 鑾峰彇鎶€鏈潰鍥犲瓙鏁版嵁
        logger.info("  [1/4] 鑾峰彇鎶€鏈潰鍥犲瓙鏁版嵁...")
        factor_df = self.fetcher.get_stk_factor_pro(
            ts_code, self.start_date, self.end_date,
            self.STK_FACTOR_FIELDS
        )

        if factor_df.empty:
            logger.warning(f"  {ts_code} 鏃犳妧鏈洜瀛愭暟鎹紝璺宠繃")
            return pd.DataFrame()

        # 璁剧疆绱㈠紩
        factor_df['datetime'] = pd.to_datetime(factor_df['trade_date'])
        factor_df.set_index(['datetime'], inplace=True)

        # 2. 鑾峰彇璧勯噾娴佸悜鏁版嵁
        logger.info("  [2/4] 鑾峰彇璧勯噾娴佸悜鏁版嵁...")
        money_df = self.fetcher.get_moneyflow_dc(
            ts_code, self.start_date, self.end_date,
            self.MONEYFLOW_FIELDS
        )

        if not money_df.empty:
            money_df['datetime'] = pd.to_datetime(money_df['trade_date'])
            money_df.set_index(['datetime'], inplace=True)
            # 鍚堝苟璧勯噾娴佸悜鏁版嵁
            factor_df = factor_df.join(money_df[
                ['net_amount', 'buy_elg_amount', 'buy_lg_amount',
                 'buy_md_amount', 'buy_sm_amount']
            ], how='left')
        else:
            logger.info("  鏃犺祫閲戞祦鍚戞暟鎹?)

        # 3. 鑾峰彇铻嶈祫铻嶅埜鏁版嵁
        logger.info("  [3/4] 鑾峰彇铻嶈祫铻嶅埜鏁版嵁...")
        margin_df = self.fetcher.get_margin_detail(
            ts_code, self.start_date, self.end_date,
            self.MARGIN_FIELDS
        )

        if not margin_df.empty:
            margin_df['datetime'] = pd.to_datetime(margin_df['trade_date'])
            margin_df.set_index(['datetime'], inplace=True)
            # 鍚堝苟铻嶈祫铻嶅埜鏁版嵁
            factor_df = factor_df.join(margin_df[['rzye', 'rqye']], how='left')
        else:
            logger.info("  鏃犺瀺璧勮瀺鍒告暟鎹?)

        # 4. 鑾峰彇璐㈠姟鎸囨爣鏁版嵁
        logger.info("  [4/4] 鑾峰彇璐㈠姟鎸囨爣鏁版嵁...")
        fina_df = self.fetcher.get_fina_indicator(
            ts_code, self.start_date, self.end_date,
            self.FINA_INDICATOR_FIELDS
        )

        if not fina_df.empty:
            # 璐㈠姟鎸囨爣浣跨敤 end_date锛堟姤鍛婃湡锛変綔涓烘棩鏈?            # 鍏抽敭淇锛氳鑼冨寲鏃ユ湡鏍煎紡锛岀‘淇濆彧淇濈暀鏃ユ湡閮ㄥ垎锛堜笉鍖呭惈鏃堕棿锛?            fina_df['datetime'] = pd.to_datetime(fina_df['end_date']).dt.normalize()
            fina_df.set_index(['datetime'], inplace=True)

            # 鍚屾牱瑙勮寖鍖杅actor_df鐨勭储寮曪紙纭繚鏃ユ湡鏍煎紡涓€鑷达級
            factor_df_index = factor_df.index
            if hasattr(factor_df_index, 'to_frame'):
                pass
            else:
                if isinstance(factor_df.index, pd.DatetimeIndex):
                    factor_df.index = factor_df.index.normalize()
                else:
                    factor_df.index = pd.to_datetime(factor_df.index).dt.normalize()

            # 鍚堝苟璐㈠姟鎸囨爣鏁版嵁
            factor_df = factor_df.join(fina_df[
                ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',
                 'q_profit_yoy', 'q_eps', 'assets_turn']
            ], how='left')

            # 妫€鏌ユ槸鍚︽垚鍔熷尮閰嶅埌浠讳綍鏁版嵁
            roe_col = 'roe' if 'roe' in factor_df.columns else None
            if roe_col and factor_df[roe_col].notna().any():
                logger.info(f"  鉁?鎴愬姛鍖归厤鍒?{factor_df[roe_col].notna().sum()} 鏉¤储鍔℃寚鏍囪褰?)
            elif roe_col:
                logger.warning(f"  鈿狅笍  join鏈尮閰嶅埌浠讳綍璐㈠姟鎸囨爣鏁版嵁锛屽皾璇曚娇鐢╩erge鏂瑰紡...")

                # 澶囩敤鏂规锛氫娇鐢╩erge鑰屼笉鏄痡oin
                fina_for_merge = fina_df.reset_index()
                fina_for_merge.rename(columns={'datetime': 'datetime_fina'}, inplace=True)

                factor_reset = factor_df.reset_index()
                factor_reset['datetime'] = pd.to_datetime(factor_reset['datetime']).dt.date
                fina_for_merge['datetime_fina'] = pd.to_datetime(fina_for_merge['datetime_fina']).dt.date

                # 浣跨敤merge锛屽厑璁?灏忎簬绛変簬"鍖归厤
                merged_data = []
                for idx, row in factor_reset.iterrows():
                    # 鎵惧埌鏈€杩戠殑璐㈠姟鎶ュ憡鏃ユ湡锛?= 褰撳墠鏃ユ湡锛?                    matching_fina = fina_for_merge[fina_for_merge['datetime_fina'] <= row['datetime']]
                    if not matching_fina.empty:
                        # 鍙栨渶杩戠殑涓€鏉?                        latest_fina = matching_fina.iloc[-1]
                        for col in ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',
                                    'q_profit_yoy', 'q_eps', 'assets_turn']:
                            if col in latest_fina.index:
                                row[col] = latest_fina[col]
                    merged_data.append(row)

                factor_df = pd.DataFrame(merged_data)
                factor_df.set_index(['instrument', 'datetime'], inplace=True)

                if 'roe' in factor_df.columns and factor_df['roe'].notna().any():
                    logger.info(f"  鉁?merge鏂瑰紡鎴愬姛锛佸尮閰嶅埌 {factor_df['roe'].notna().sum()} 鏉¤褰?)
            else:
                logger.warning("  鈿狅笍  鏃燫OE瀛楁")

            # 鍓嶅悜濉厖(ffill)灏嗗搴︽暟鎹～鍏呭埌姣忎釜浜ゆ槗鏃?            if len(factor_df) > 0:
                factor_df = factor_df.sort_index()
                fina_cols = ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',
                             'q_profit_yoy', 'q_eps', 'assets_turn']
                existing_cols = [col for col in fina_cols if col in factor_df.columns]
                if existing_cols:
                    # 缁熻濉厖鍓嶇殑闈炵┖鏁伴噺
                    before_fill = {col: factor_df[col].notna().sum() for col in existing_cols}

                    # 鎵ц鍓嶅悜濉厖
                    factor_df[existing_cols] = factor_df[existing_cols].ffill()

                    # 缁熻濉厖鍚庣殑闈炵┖鏁伴噺
                    after_fill = {col: factor_df[col].notna().sum() for col in existing_cols}

                    # 璁板綍鏃ュ織
                    fill_info = ', '.join([f"{col}:{before_fill[col]}鈫抺after_fill[col]}" for col in existing_cols])
                    logger.info(f"  鉁?璐㈠姟鎸囨爣鍓嶅悜濉厖瀹屾垚 ({fill_info})")
        else:
            logger.info("  鈿狅笍  鏃犺储鍔℃寚鏍囨暟鎹紙鎺ュ彛鍙兘杩斿洖绌烘垨绉垎涓嶈冻锛?)

        # 娣诲姞鑲＄エ浠ｇ爜浣滀负instrument绱㈠紩
        factor_df['instrument'] = ts_code
        factor_df.reset_index(inplace=True)
        factor_df.set_index(['instrument', 'datetime'], inplace=True)

        logger.info(f"  {ts_code} 鏁版嵁鑾峰彇瀹屾垚锛屽叡 {len(factor_df)} 鏉¤褰?)
        return factor_df

    def save_stock_to_csv(self, ts_code: str, df: pd.DataFrame) -> bool:
        """
        灏嗗崟鍙偂绁ㄦ暟鎹繚瀛樹负CSV鏂囦欢

        鍙傛暟锛?            ts_code: 鑲＄エ浠ｇ爜 (濡?'000001.SZ')
            df: 鑲＄エ鏁版嵁DataFrame

        杩斿洖锛?            鏄惁淇濆瓨鎴愬姛
        """
        if df.empty:
            return False

        try:
            # 涓?qlib dump_bin 涓€鑷达細鏂囦欢鍚?stem 鍗充负璇佸埜浠ｇ爜锛屽 000001.sz -> 000001.SZ
            csv_filename = f"{ts_code.lower()}.csv"
            csv_path = self.csv_dir / csv_filename

            # 閲嶇疆绱㈠紩锛屽皢instrument鍜?datetime鍙樹负鏅€氬垪
            df_to_save = df.reset_index()
            add_qlib_standard_ohlcv_columns(df_to_save)

            # 淇濆瓨涓篊SV
            df_to_save.to_csv(csv_path, index=False, encoding='utf-8-sig')

            logger.info(f"  馃捑 宸蹭繚瀛? {csv_path.name} ({len(df)} 鏉¤褰?")
            return True

        except Exception as e:
            logger.error(f"淇濆瓨 {ts_code} CSV澶辫触: {e}")
            return False

    def download_all_to_csv(self, stock_codes: List[str] = None) -> Tuple[int, int]:
        """
        闃舵1锛氫笅杞芥墍鏈夎偂绁ㄦ暟鎹苟淇濆瓨涓篊SV鏂囦欢

        鍙傛暟锛?            stock_codes: 瑕佸鐞嗙殑鑲＄エ浠ｇ爜鍒楄〃锛圢one琛ㄧず澶勭悊鍏ㄩ儴锛?
        杩斿洖锛?            (鎴愬姛鏁伴噺, 澶辫触鏁伴噺)
        """
        logger.info("\n" + "="*80)
        logger.info("銆愰樁娈?/2銆戝紑濮嬩笅杞芥暟鎹埌CSV鏂囦欢")
        logger.info("="*80)
        logger.info(f"鏃堕棿鑼冨洿: {self.start_date} ~ {self.end_date}")
        logger.info(f"CSV鐩綍: {self.csv_dir}")
        logger.info(f"棰戠巼闄愬埗妯″紡: {'楂橀€?8000+绉垎)' if self.rate_limiter.high_speed else '鏍囧噯(5000绉垎)'}")

        # 鑾峰彇鑲＄エ鍒楄〃
        stock_list = self.fetcher.get_stock_list()

        if stock_codes is None:
            stock_codes = stock_list['ts_code'].tolist()

        # 闄愬埗澶勭悊鐨勮偂绁ㄦ暟閲忥紙鐢ㄤ簬娴嬭瘯锛?        if self.max_stocks:
            stock_codes = stock_codes[:self.max_stocks]
            logger.info(f"娴嬭瘯妯″紡锛氫粎澶勭悊鍓?{self.max_stocks} 鍙偂绁?)

        logger.info(f"寰呭鐞嗚偂绁ㄦ暟閲? {len(stock_codes)}")

        success_count = 0
        fail_count = 0
        skipped_count = 0
        total = len(stock_codes)

        workers = min(self.download_workers, total) if total > 0 else 1
        logger.info(f"涓嬭浇鎵ц妯″紡: {'涓茶' if workers == 1 else f'骞跺彂({workers}绾跨▼)'}")

        if workers == 1:
            pbar = tqdm(stock_codes, total=total, desc="涓嬭浇CSV", unit="stock")
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
                    logger.info(f"\n馃搳 褰撳墠杩涘害: 鎴愬姛={success_count}, 澶辫触={fail_count}, 璺宠繃={skipped_count}")
            pbar.close()
        else:
            futures = {}
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for ts_code in stock_codes:
                    futures[executor.submit(self._download_single_stock, ts_code)] = ts_code

                pbar = tqdm(total=total, desc="涓嬭浇CSV", unit="stock")
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
                        logger.info(f"\n馃搳 褰撳墠杩涘害: 鎴愬姛={success_count}, 澶辫触={fail_count}, 璺宠繃={skipped_count}")
                pbar.close()

        logger.info(f"\n{'='*60}")
        logger.info("銆愰樁娈?瀹屾垚銆慍SV涓嬭浇缁熻:")
        logger.info(f"  鉁?鎴愬姛: {success_count} 鍙偂绁?)
        logger.info(f"  鈴笍  璺宠繃(宸插瓨鍦?: {skipped_count} 鍙偂绁?)
        logger.info(f"  鉂?澶辫触: {fail_count} 鍙偂绁?)
        logger.info(f"  馃搧 CSV鏂囦欢淇濆瓨鍦? {self.csv_dir}")

        return success_count, fail_count

    def convert_csv_to_qlib(self) -> bool:
        """
        闃舵2锛氫娇鐢ㄥ井杞?qlib 瀹樻柟 scripts/dump_bin.py 涓殑 DumpDataAll 杞负 Qlib 鐩綍缁撴瀯銆?
        姝ｇ‘鐩綍甯冨眬涓?features/<鑲＄エ浠ｇ爜鐩綍>/<瀛楁>.day.bin锛堣 qlib LocalFeatureProvider锛夛紝
        涓嶅彲鐢ㄥ崟鏂囦欢 features/<瀛楁>.day.bin 娣峰瓨澶氬彧鑲＄エ銆?        """
        logger.info("\n" + "="*80)
        logger.info("銆愰樁娈?/2銆戜粠CSV杞崲鍒癚lib鏍煎紡锛堝畼鏂?dump_bin.DumpDataAll锛?)
        logger.info("="*80)
        logger.info(f"CSV鐩綍: {self.csv_dir}")
        logger.info(f"Qlib杈撳嚭鐩綍: {self.converter.output_dir}")

        csv_files = sorted(self.csv_dir.glob("*.csv"))
        if not csv_files:
            logger.error("鏈壘鍒颁换浣旵SV鏂囦欢锛佽鍏堟墽琛岄樁娈?锛堜笅杞斤級")
            return False

        logger.info(f"鎵惧埌 {len(csv_files)} 涓狢SV鏂囦欢")

        for csv_path in csv_files:
            try:
                df_patch = pd.read_csv(csv_path, encoding="utf-8-sig")
                add_qlib_standard_ohlcv_columns(df_patch)
                df_patch.to_csv(csv_path, index=False, encoding="utf-8-sig")
            except Exception as e:
                logger.warning("涓?%s 鍐欏叆鏍囧噯 OHLCV 鍒楀け璐? %s", csv_path.name, e)

        sample = pd.read_csv(csv_files[0], nrows=512, encoding="utf-8-sig")
        skip_cols = {"datetime", "instrument", "trade_date", "ts_code"}
        feature_cols = [
            c
            for c in sample.columns
            if c not in skip_cols and pd.api.types.is_numeric_dtype(sample[c])
        ]
        if not feature_cols:
            logger.error("鏈兘浠嶤SV涓瘑鍒暟鍊肩壒寰佸垪锛岃妫€鏌ュ垪鍚嶄笌鏁版嵁绫诲瀷")
            return False

        include_fields = ",".join(feature_cols)
        logger.info(f"灏?dump 鐨勬暟鍊肩壒寰佸垪鏁? {len(feature_cols)}")

        qlib_main = Path(os.environ.get("QLIB_SOURCE", r"D:\quant_project\qlib-main"))
        scripts_dir = qlib_main / "scripts"
        dump_script = scripts_dir / "dump_bin.py"
        if not dump_script.is_file():
            logger.error(
                "鏈壘鍒?qlib 婧愮爜涓殑 dump_bin.py: %s銆傝鍏嬮殕 qlib 鎴栬缃幆澧冨彉閲?QLIB_SOURCE 鎸囧悜 qlib 鏍圭洰褰?,
                dump_script,
            )
            return False

        if str(scripts_dir) not in sys.path:
            sys.path.insert(0, str(scripts_dir))

        try:
            from dump_bin import DumpDataAll
        except ImportError as exc:
            logger.error(
                "鏃犳硶瀵煎叆 dump_bin锛堥渶瑕佸凡瀹夎 qlib: pip install qlib锛夈€傝鎯? %s",
                exc,
            )
            return False

        out_dir = str(self.converter.output_dir)
        try:
            dumper = DumpDataAll(
                data_path=str(self.csv_dir),
                qlib_dir=out_dir,
                date_field_name="datetime",
                symbol_field_name="instrument",
                include_fields=include_fields,
                freq="day",
                max_workers=min(16, (os.cpu_count() or 4)),
            )
            dumper.dump()
        except Exception:
            logger.exception("dump_bin 杞崲澶辫触")
            return False

        logger.info("\n" + "="*80)
        logger.info("銆愰樁娈?瀹屾垚銆慟lib鏍煎紡杞崲鎴愬姛锛堝畼鏂瑰竷灞€: calendars/ + instruments/ + features/<浠ｇ爜>/锛?)
        logger.info("="*80)
        logger.info("馃搧 Qlib鏁版嵁鐩綍: %s", self.converter.output_dir)
        logger.info("鍒濆鍖栫ず渚? qlib.init(provider_uri=r'%s')", out_dir)

        return True

    def run(self, stock_codes: List[str] = None):
        """
        杩愯瀹屾暣鐨勬暟鎹閬擄紙涓ら樁娈碉級

        鍙傛暟锛?            stock_codes: 瑕佸鐞嗙殑鑲＄エ浠ｇ爜鍒楄〃锛圢one琛ㄧず澶勭悊鍏ㄩ儴锛?        """
        logger.info("\n" + "="*80)
        logger.info("寮€濮嬫墽琛屽畬鏁存暟鎹閬擄紙涓ら樁娈碉級")
        logger.info("="*80)

        # 闃舵1锛氫笅杞藉埌CSV
        success, fail = self.download_all_to_csv(stock_codes)

        if success == 0 and fail == 0:
            logger.error("娌℃湁鑾峰彇鍒颁换浣曟暟鎹紒")
            return

        # 闃舵2锛氳浆鎹负Qlib鏍煎紡
        if success > 0:
            self.convert_csv_to_qlib()
        else:
            logger.warning("娌℃湁鎴愬姛鐨勪笅杞斤紝璺宠繃Qlib杞崲闃舵")

        # 鎵撳嵃鏈€缁堢粺璁?        self._print_final_stats(success, fail, None)

    def _print_rate_stats(self):
        """鎵撳嵃API璋冪敤缁熻淇℃伅"""
        stats = self.rate_limiter.get_stats()
        logger.info("\n馃搳 API璋冪敤缁熻锛堟渶杩?鍒嗛挓锛?")
        for api_name, info in stats.items():
            logger.info(
                f"   {api_name:20s}: "
                f"{info['recent_requests']:3d}娆?/ "
                f"{info['rate_limit']:5.1f}娆?鍒嗛挓 "
                f"(鍒╃敤鐜? {info['utilization']:5.1f}%)"
            )

    def _print_final_stats(self, success_count: int, fail_count: int, merged_df: Optional[pd.DataFrame]):
        """鎵撳嵃鏈€缁堢粺璁′俊鎭?""
        print("\n馃搳 鏁版嵁缁熻:")
        print(f"  鉁?鎴愬姛澶勭悊鑲＄エ: {success_count}")
        print(f"  鉂?澶辫触鑲＄エ: {fail_count}")
        if merged_df is not None:
            print(f"  馃搱 鎬昏褰曟暟: {len(merged_df)}")
            print(f"  馃梻锔? 鍖呭惈瀛楁: {len(merged_df.columns)} 涓?)
        else:
            print("  馃搱 鎬昏褰曟暟: (闃舵2 宸茬敤 dump_bin锛屾湭淇濈暀鍚堝苟 DataFrame)")
        print(f"  馃搧 杈撳嚭鐩綍: {self.converter.output_dir}")

        # 鎵撳嵃API璋冪敤缁熻
        stats = self.rate_limiter.get_stats()
        print(f"\n鈴憋笍  API棰戠巼浣跨敤鎯呭喌:")
        for api_name, info in stats.items():
            print(f"  {api_name}: {info['recent_requests']} 娆?鍒嗛挓 (闄愬埗: {info['rate_limit']:.0f}娆?鍒嗛挓)")


def main():
    """涓诲嚱鏁?""
    parser = argparse.ArgumentParser(
        description='浠嶵ushare鑾峰彇閲戣瀺鏁版嵁骞惰浆鎹负Qlib鏍煎紡',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
鐜鍙橀噺鏀寔锛?  TUSHARE_API_KEY  - Tushare API Token锛堜紭鍏堢骇鏈€楂橈級
  TUSHARE_TOKEN    - 鍏煎鏃х増鐜鍙橀噺鍚?
绀轰緥鐢ㄦ硶锛?  # 浣跨敤鐜鍙橀噺Token
  set TUSHARE_API_KEY=your_token
  python tushare_to_qlib.py --start_date 20200101 --end_date 20231231

  # 浣跨敤鍛戒护琛屽弬鏁癟oken
  python tushare_to_qlib.py --token YOUR_TOKEN --start_date 20200101 --end_date 20231231

  # 娴嬭瘯妯″紡锛堝彧澶勭悊10鍙偂绁級
  python tushare_to_qlib.py --start_date 20230101 --end_date 20231231 --max_stocks 10

  # 楂橀€熸ā寮忥紙闇€瑕?000+绉垎锛?  python tushare_to_qlib.py --start_date 20230101 --end_date 20231231 --high-speed

  # 鑷畾涔夎緭鍑虹洰褰?  python tushare_to_qlib.py --token YOUR_TOKEN --output_dir D:/my_qlib_data

棰戠巼闄愬埗璇存槑锛?  鏍囧噯妯″紡 (榛樿): 閫傚悎5000绉垎鐢ㄦ埛
    - stk_factor_pro: 30娆?鍒嗛挓
    - 鍏朵粬鎺ュ彛: 20-30娆?鍒嗛挓

  楂橀€熸ā寮?(--high-speed): 闇€瑕?000+绉垎
    - stk_factor_pro: 500娆?鍒嗛挓
    - 鍏朵粬鎺ュ彛: 40-120娆?鍒嗛挓
        """
    )

    parser.add_argument('--token', type=str, default=None,
                        help='Tushare Pro API Token锛堝彲閫夛紝涔熷彲浣跨敤鐜鍙橀噺 TUSHARE_API_KEY锛?)
    parser.add_argument('--start_date', type=str, default='20200101',
                        help='寮€濮嬫棩鏈?(YYYYMMDD)锛岄粯璁? 20200101')
    parser.add_argument('--end_date', type=str, default='20231231',
                        help='缁撴潫鏃ユ湡 (YYYYMMDD)锛岄粯璁? 20231231')
    parser.add_argument('--output_dir', type=str,
                        default=r'D:\qlib_data\qlib_data',
                        help=f'Qlib鏁版嵁杈撳嚭鐩綍锛岄粯璁? D:\\qlib_data\\qlib_data')
    parser.add_argument('--csv_dir', type=str, default=r'D:\qlib_data\csv_data',
                        help='CSV鏂囦欢淇濆瓨鐩綍锛堥粯璁? D:\\qlib_data\\csv_data锛?)
    parser.add_argument('--max_stocks', type=int, default=None,
                        help='鏈€澶у鐞嗚偂绁ㄦ暟閲忥紙鐢ㄤ簬娴嬭瘯锛夛紝榛樿: 鍏ㄩ儴')
    parser.add_argument('--high-speed', action='store_true',
                        help='鍚敤楂橀€熸ā寮忥紙闇€瑕?000+绉垎锛屽ぇ骞呮彁楂樿姹傞€熺巼锛?)
    parser.add_argument('--download_workers', type=int, default=4,
                        help='涓嬭浇闃舵骞跺彂绾跨▼鏁帮紝榛樿: 4锛堝缓璁?2-8锛?)
    parser.add_argument('--stage', type=str, default='all',
                        choices=['all', 'download', 'convert'],
                        help='鎵ц闃舵: all(瀹屾暣娴佺▼)/download(浠呬笅杞紺SV)/convert(浠呰浆Qlib)锛岄粯璁? all')

    args = parser.parse_args()

    # 鑾峰彇Token锛堜紭鍏堢骇锛氬懡浠よ鍙傛暟 > 鐜鍙橀噺 > 閰嶇疆鏂囦欢锛?    token = args.token

    if not token:
        # 灏濊瘯浠庣幆澧冨彉閲忚幏鍙?        token = os.environ.get('TUSHARE_API_KEY', '').strip()
        if token:
            logger.info(f"鉁?浠庣幆澧冨彉閲?TUSHARE_API_KEY 鑾峰彇鍒癟oken")

    if not token:
        # 灏濊瘯澶囩敤鐜鍙橀噺
        token = os.environ.get('TUSHARE_TOKEN', '').strip()
        if token:
            logger.info("鈿狅笍  浠庣幆澧冨彉閲?TUSHARE_TOKEN 鑾峰彇鍒癟oken锛堝缓璁娇鐢?TUSHARE_API_KEY锛?)

    if not token:
        # 灏濊瘯閰嶇疆鏂囦欢
        token = get_tushare_token()

    if not token:
        print("\n鉂?閿欒锛氭湭鎻愪緵Tushare API Token锛?)
        print("\n璇烽€氳繃浠ヤ笅浠讳竴鏂瑰紡鎻愪緵Token锛?)
        print("  1. 鍛戒护琛屽弬鏁? --token YOUR_TOKEN")
        print("  2. 鐜鍙橀噺: set TUSHARE_API_KEY=YOUR_TOKEN")
        print("  3. 閰嶇疆鏂囦欢: config/secrets.yaml")
        print("\n绀轰緥:")
        print("  set TUSHARE_API_KEY=your_token_here")
        print("  python tushare_to_qlib.py --start_date 20230101 --end_date 20231231\n")
        sys.exit(1)

    # 楠岃瘉鏃ユ湡鏍煎紡
    try:
        pd.to_datetime(args.start_date)
        pd.to_datetime(args.end_date)
    except ValueError:
        logger.error("鏃ユ湡鏍煎紡閿欒锛岃浣跨敤 YYYYMMDD 鏍煎紡")
        sys.exit(1)

    # 鍒涘缓绠￠亾
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

    # 鏍规嵁stage鍙傛暟鎵ц涓嶅悓鎿嶄綔
    if args.stage == 'all':
        # 瀹屾暣娴佺▼锛氫笅杞?+ 杞崲
        pipeline.run()
    elif args.stage == 'download':
        # 浠呬笅杞藉埌CSV
        success, fail = pipeline.download_all_to_csv()
        print(f"\n馃搳 涓嬭浇瀹屾垚锛佹垚鍔? {success}, 澶辫触: {fail}")
        print(f"馃搧 CSV鐩綍: {pipeline.csv_dir}")
    elif args.stage == 'convert':
        # 浠呬粠CSV杞崲涓篞lib
        success = pipeline.convert_csv_to_qlib()
        if not success:
            sys.exit(1)


if __name__ == '__main__':
    main()

