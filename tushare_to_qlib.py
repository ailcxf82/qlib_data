#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tushare数据获取并转换为Qlib格式的脚本

功能：
1. 从Tushare获取多个接口的数据（技术因子、资金流向、融资融券、财务指标）
2. 将数据合并并转换为Qlib标准格式（.bin文件）
3. 保存到指定目录：D:/qlib_data/qlib_data

使用方法：
    python tushare_to_qlib.py --start_date 20200101 --end_date 20260424
    或设置环境变量 TUSHARE_API_KEY 后直接运行
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import threading
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

# Qlib 常用特征名 ← 数据源列（前复权 OHLC；volume 使用量比，与 vol 手数无关）
QLIB_STD_OHLCV_FROM: Dict[str, str] = {
    "open": "open_qfq",
    "high": "high_qfq",
    "low": "low_qfq",
    "close": "close_qfq",
    "volume": "volume_ratio",
}


def add_qlib_standard_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    为 DataFrame 增加 open、high、low、close、volume 列，内容分别来自
    open_qfq、high_qfq、low_qfq、close_qfq、volume_ratio。
    不删除或重命名原有列；源列不存在时跳过对应目标列。
    """
    if df is None or len(df) == 0:
        return df
    for target, source in QLIB_STD_OHLCV_FROM.items():
        if source in df.columns:
            df[target] = df[source]
    return df


class APIRateLimiter:
    """
    Tushare API 频率限制器

    根据各接口的官方频率限制自动控制请求速率，
    支持动态调整和错误重试机制。

    官方频率限制（来自Tushare文档）：
    - stk_factor_pro: 5000积分=30次/分钟, 8000+积分=500次/分钟
    - moneyflow_dc: 5000积分（保守估计30次/分钟）
    - margin_detail: 2000积分（保守估计30次/分钟）
    - fina_indicator: 2000积分（保守估计30次/分钟）
    - stock_basic: 基础接口（较宽松）
    """

    # 各接口的频率配置 (请求次数/分钟)
    RATE_LIMITS = {
        'stk_factor_pro': {
            'normal': 30,      # 5000积分用户
            'high': 500,       # 8000+积分用户
            'current': 30      # 当前使用的速率
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
            'normal': 20,      # 财务指标接口更严格
            'high': 40,
            'current': 20
        },
        'stock_basic': {
            'normal': 60,
            'high': 120,
            'current': 60
        }
    }

    # 错误重试配置
    MAX_RETRIES = 3
    RETRY_DELAYS = [1, 2, 4]  # 重试延迟（秒），指数退避

    def __init__(self, high_speed: bool = False):
        """
        初始化频率限制器

        参数：
            high_speed: 是否使用高速模式（需要8000+积分）
        """
        self.high_speed = high_speed
        self.request_history: Dict[str, deque] = {}
        self.mode = 'high' if high_speed else 'normal'
        # 并发场景下按接口加锁，确保同一接口的限频逻辑线程安全
        self._api_locks: Dict[str, threading.Lock] = {}

        # 初始化每个接口的请求历史记录
        for api_name in self.RATE_LIMITS.keys():
            self.request_history[api_name] = deque(maxlen=1000)
            self._api_locks[api_name] = threading.Lock()

            # 设置当前速率
            self.RATE_LIMITS[api_name]['current'] = self.RATE_LIMITS[api_name][self.mode]

        logger.info(f"API频率限制器初始化完成 (模式: {self.mode})")

    def _get_interval(self, api_name: str) -> float:
        """
        计算两个请求之间的最小间隔时间（秒）

        参数：
            api_name: 接口名称

        返回：
            最小间隔时间（秒）
        """
        if api_name not in self.RATE_LIMITS:
            return 2.0  # 默认2秒间隔

        rate_per_minute = self.RATE_LIMITS[api_name]['current']
        interval = 60.0 / rate_per_minute

        # 添加10%的安全余量
        interval *= 1.1

        return interval

    def wait_if_needed(self, api_name: str):
        """
        如果需要，等待直到可以发送下一个请求

        参数：
            api_name: 接口名称
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
                logger.debug(f"[{api_name}] 等待 {wait_time:.2f} 秒以符合频率限制")
                time.sleep(wait_time)

    def record_request(self, api_name: str):
        """记录一次API请求"""
        if api_name in self.request_history:
            self.request_history[api_name].append(time.time())

    def execute_with_rate_limit(self, api_name: str, func, *args, **kwargs):
        """
        在频率限制下执行API调用，支持自动重试

        参数：
            api_name: 接口名称
            func: 要执行的函数
            *args, **kwargs: 函数参数

        返回：
            函数执行结果

        异常：
            连续重试失败后抛出最后一个异常
        """
        last_exception = None

        for attempt in range(self.MAX_RETRIES):
            try:
                # 并发场景：同一接口串行节流，避免竞争导致的超频
                api_lock = self._api_locks.get(api_name)
                if api_lock is None:
                    api_lock = threading.Lock()
                    self._api_locks[api_name] = api_lock
                with api_lock:
                    # 等待直到可以发送请求
                    self.wait_if_needed(api_name)
                    # 记录请求时间
                    self.record_request(api_name)

                # 执行实际的API调用
                result = func(*args, **kwargs)

                return result

            except Exception as e:
                last_exception = e
                error_msg = str(e).lower()

                # 检查是否是频率限制错误
                is_rate_limit_error = any(keyword in error_msg for keyword in [
                    'limit', 'too many', '频率', '限流', '请求过快'
                ])

                if is_rate_limit_error and attempt < self.MAX_RETRIES - 1:
                    # 如果是频率限制错误，增加等待时间并重试
                    delay = self.RETRY_DELAYS[min(attempt, len(self.RETRY_DELAYS)-1)]
                    logger.warning(
                        f"[{api_name}] 可能触发频率限制 ({attempt+1}/{self.MAX_RETRIES}): {e}"
                        f"\n   等待 {delay} 秒后重试..."
                    )
                    time.sleep(delay)

                    # 动态降低该接口的速率
                    if api_name in self.RATE_LIMITS:
                        current_rate = self.RATE_LIMITS[api_name]['current']
                        new_rate = max(5, current_rate * 0.7)  # 降低30%，最低5次/分钟
                        self.RATE_LIMITS[api_name]['current'] = new_rate
                        logger.info(
                            f"[{api_name}] 自动降低速率: "
                            f"{current_rate:.1f} -> {new_rate:.1f} 次/分钟"
                        )

                elif attempt < self.MAX_RETRIES - 1:
                    # 其他错误，正常重试
                    delay = self.RETRY_DELAYS[min(attempt, len(self.RETRY_DELAYS)-1)]
                    logger.warning(
                        f"[{api_name}] 请求失败 ({attempt+1}/{self.MAX_RETRIES}): {e}"
                        f"\n   等待 {delay} 秒后重试..."
                    )
                    time.sleep(delay)

        # 所有重试都失败
        logger.error(f"[{api_name}] 经过 {self.MAX_RETRIES} 次重试后仍然失败")
        raise last_exception

    def get_stats(self) -> Dict:
        """获取统计信息"""
        stats = {}
        now = time.time()

        for api_name, history in self.request_history.items():
            # 统计最近1分钟的请求数
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
    获取Tushare Token（按优先级）

    优先级顺序：
    1. 命令行参数 --token
    2. 环境变量 TUSHARE_API_KEY
    3. 环境变量 TUSHARE_TOKEN
    4. 配置文件 config/secrets.yaml

    返回：
        Token字符串或None
    """

    # 方法1 & 2: 由main()函数处理命令行参数和环境变量
    # 这里只处理备用方法

    # 方法3: 环境变量 TUSHARE_TOKEN（兼容旧版本）
    token = os.environ.get('TUSHARE_TOKEN', '').strip()
    if token:
        logger.info("从环境变量 TUSHARE_TOKEN 获取到Token")
        return token

    # 方法4: 配置文件
    try:
        import yaml
        config_path = Path('config/secrets.yaml')
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                if data and 'tushare' in data:
                    token = data['tushare'].get('token', '').strip()
                    if token:
                        logger.info("从配置文件 config/secrets.yaml 获取到Token")
                        return token
    except Exception:
        pass

    return None


class TushareDataFetcher:
    """Tushare数据获取器（集成API频率限制）"""

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
            logger.info("Tushare Pro API 初始化成功")
        except Exception as e:
            logger.error(f"初始化Tushare失败: {e}")
            raise

    def _cache_path(self, name: str) -> Path:
        return self.cache_dir / name

    def _load_cache(self, name: str) -> Optional[pd.DataFrame]:
        """加载缓存数据"""
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
        """保存缓存数据"""
        p = self._cache_path(name)
        try:
            df.to_parquet(p, index=False)
        except Exception:
            df.to_csv(p.with_suffix('.csv'), index=False)

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        cache_name = "stock_list.parquet"
        cached = self._load_cache(cache_name)

        # 安全检查缓存数据
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

        # 安全检查返回数据
        if df is not None and len(df) > 0:
            self._save_cache(cache_name, df)

        return df if df is not None else pd.DataFrame()

    def get_stk_factor_pro(self, ts_code: str, start_date: str, end_date: str,
                           fields: List[str]) -> pd.DataFrame:
        """
        获取技术面因子数据 (stk_factor_pro接口)

        频率限制：5000积分=30次/分钟, 8000+积分=500次/分钟

        参数：
            ts_code: 股票代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            fields: 需要的字段列表
        """
        cache_name = f"stk_factor_{ts_code}_{start_date}_{end_date}.parquet"
        cached = self._load_cache(cache_name)

        # 安全检查缓存数据
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

            # 调试日志：显示返回数据的类型和形状
            logger.debug(f"[{ts_code}] stk_factor_pro 返回类型: {type(df)}, 值: {df}")

            # 安全检查：确保df是有效的DataFrame
            if df is None:
                logger.debug(f"[{ts_code}] stk_factor_pro 返回None")
                return pd.DataFrame()

            # 转换为DataFrame（防止返回Series或其他类型）
            if not isinstance(df, pd.DataFrame):
                logger.warning(f"[{ts_code}] stk_factor_pro 返回非DataFrame类型: {type(df)}，尝试转换")
                df = pd.DataFrame(df)

            # 检查是否为空
            if len(df) > 0:
                logger.info(f"  ✅ 获取到 {ts_code} 技术因子数据，共 {len(df)} 条记录")
                self._save_cache(cache_name, df)
            else:
                logger.info(f"  ⚠️  {ts_code} 技术因子数据为空（可能该时间段无数据）")

            return df

        except Exception as e:
            logger.warning(f"获取 {ts_code} 技术因子失败: {e}")
            import traceback
            logger.debug(f"详细错误信息:\n{traceback.format_exc()}")
            return pd.DataFrame()

    def get_moneyflow_dc(self, ts_code: str, start_date: str, end_date: str,
                         fields: List[str]) -> pd.DataFrame:
        """
        获取个股资金流向数据 (moneyflow_dc接口)

        频率限制：保守估计30次/分钟（需要5000+积分）

        参数：
            ts_code: 股票代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            fields: 需要的字段列表
        """
        cache_name = f"moneyflow_{ts_code}_{start_date}_{end_date}.parquet"
        cached = self._load_cache(cache_name)

        # 安全检查缓存数据
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

            # 安全检查：确保df是有效的DataFrame
            if df is None:
                return pd.DataFrame()

            # 转换为DataFrame（防止返回Series或其他类型）
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            # 检查是否为空
            if len(df) > 0:
                self._save_cache(cache_name, df)

            return df

        except Exception as e:
            logger.warning(f"获取 {ts_code} 资金流向失败: {e}")
            return pd.DataFrame()

    def get_margin_detail(self, ts_code: str, start_date: str, end_date: str,
                          fields: List[str]) -> pd.DataFrame:
        """
        获取融资融券交易明细 (margin_detail接口)

        频率限制：保守估计30次/分钟（需要2000+积分）

        参数：
            ts_code: 股票代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            fields: 需要的字段列表
        """
        cache_name = f"margin_{ts_code}_{start_date}_{end_date}.parquet"
        cached = self._load_cache(cache_name)

        # 安全检查缓存数据
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

            # 安全检查：确保df是有效的DataFrame
            if df is None:
                return pd.DataFrame()

            # 转换为DataFrame（防止返回Series或其他类型）
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            # 检查是否为空
            if len(df) > 0:
                self._save_cache(cache_name, df)

            return df

        except Exception as e:
            logger.warning(f"获取 {ts_code} 融资融券数据失败: {e}")
            return pd.DataFrame()

    def get_fina_indicator(self, ts_code: str, start_date: str, end_date: str,
                           fields: List[str]) -> pd.DataFrame:
        """
        获取财务指标数据 (fina_indicator接口)

        频率限制：保守估计20次/分钟（需要2000+积分，按股票查询）

        注意：此接口每次最多返回100条记录，且只能按单只股票查询

        参数：
            ts_code: 股票代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            fields: 需要的字段列表
        """
        cache_name = f"fina_ind_{ts_code}_{start_date}_{end_date}.parquet"
        cached = self._load_cache(cache_name)

        # 安全检查缓存数据
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

            # 安全检查：确保df是有效的DataFrame
            if df is None:
                return pd.DataFrame()

            # 转换为DataFrame（防止返回Series或其他类型）
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            # 检查是否为空
            if len(df) > 0:
                self._save_cache(cache_name, df)

            return df

        except Exception as e:
            logger.warning(f"获取 {ts_code} 财务指标失败: {e}")
            return pd.DataFrame()


class QlibDataConverter:
    """Qlib格式数据转换器"""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 创建Qlib所需的目录结构
        self.calendars_dir = self.output_dir / 'calendars'
        self.features_dir = self.output_dir / 'features'
        self.instruments_dir = self.output_dir / 'instruments'

        self.calendars_dir.mkdir(exist_ok=True)
        self.features_dir.mkdir(exist_ok=True)
        self.instruments_dir.mkdir(exist_ok=True)

        logger.info(f"Qlib数据输出目录: {self.output_dir}")

    def save_calendar(self, dates: List[pd.Timestamp], freq: str = 'day'):
        """
        保存交易日历

        参数：
            dates: 交易日列表
            freq: 频率 (day/min等)
        """
        calendar_path = self.calendars_dir / f'{freq}.txt'

        date_strings = [d.strftime('%Y-%m-%d') for d in sorted(dates)]
        with open(calendar_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(date_strings))

        logger.info(f"交易日历已保存: {calendar_path}, 共 {len(date_strings)} 个交易日")

    def save_instruments(self, stock_list: pd.DataFrame, start_date: str, end_date: str):
        """
        保存股票池信息

        参数：
            stock_list: 股票列表DataFrame
            start_date: 开始日期
            end_date: 结束日期
        """
        instruments_path = self.instruments_dir / 'all.txt'

        with open(instruments_path, 'w', encoding='utf-8') as f:
            for _, row in stock_list.iterrows():
                ts_code = row['ts_code']
                list_date = row.get('list_date', start_date)
                line = f"{ts_code}\t{list_date}\t{end_date}\n"
                f.write(line)

        logger.info(f"股票池信息已保存: {instruments_path}, 共 {len(stock_list)} 只股票")

    def dataframe_to_bin(
        self,
        df: pd.DataFrame,
        field: str,
        freq: str = 'day',
        source_field: Optional[str] = None,
    ):
        """
        将单个字段的数据转换为bin格式

        Qlib bin格式说明：
        - 第一行是日期索引（在日历中的位置）
        - 后续是实际数据值
        - 数据类型：float32（小端序）

        参数：
            df: DataFrame，包含datetime索引和目标字段
            field: 输出 bin 文件名所用字段名（如 open）
            freq: 频率
            source_field: 若指定，从该列读取数值，仍写入 field 对应的 .bin 文件
        """
        col = source_field if source_field is not None else field
        if col not in df.columns:
            logger.warning(f"字段 {col} 不存在")
            return

        bin_path = self.features_dir / f"{field.lower()}.{freq}.bin"

        # 准备数据：确保有datetime索引
        data = df[col].dropna()

        if len(data) == 0:
            logger.warning(f"字段 {field} 没有有效数据")
            return

        # 读取日历以建立索引映射
        calendar_path = self.calendars_dir / f'{freq}.txt'
        if not calendar_path.exists():
            logger.error("请先保存交易日历")
            return

        with open(calendar_path, 'r', encoding='utf-8') as f:
            calendar_dates = [line.strip() for line in f.readlines()]

        # 创建日期到索引的映射
        date_to_idx = {date: idx for idx, date in enumerate(calendar_dates)}

        # 为每个数据点找到对应的日历索引
        indices = []
        values = []

        for dt, val in data.items():
            if isinstance(dt, (pd.Timestamp, str)):
                date_str = dt.strftime('%Y-%m-%d') if isinstance(dt, pd.Timestamp) else str(dt)[:10]
                if date_str in date_to_idx:
                    indices.append(date_to_idx[date_str])
                    values.append(float(val))

        if len(indices) == 0:
            logger.warning(f"字段 {field} 没有匹配到交易日历的数据")
            return

        # 创建数组：第一列是日期索引，第二列是值
        output_array = np.column_stack([indices, values]).astype('<f')

        # 写入bin文件
        output_array.tofile(str(bin_path.resolve()))

        logger.debug(f"字段 {field} 已转换为bin格式: {len(values)} 条记录")

    def convert_and_save(self, merged_df: pd.DataFrame, stock_list: pd.DataFrame,
                         start_date: str, end_date: str):
        """
        将合并后的数据转换为Qlib格式并保存

        参数：
            merged_df: 合并后的DataFrame（包含所有字段）
            stock_list: 股票列表
            start_date: 开始日期
            end_date: 结束日期
        """
        logger.info("开始转换为Qlib格式...")

        # 1. 保存交易日历
        dates = pd.to_datetime(merged_df.index.get_level_values('datetime').unique()).sort_values()
        self.save_calendar(dates.tolist())

        # 2. 保存股票池信息
        self.save_instruments(stock_list, start_date, end_date)

        # 3. 逐个字段转换为bin格式
        # 基础OHLCV字段
        base_fields = ['open_qfq', 'high_qfq', 'low_qfq', 'close_qfq', 'vol', 'amount']

        # 扩展技术指标字段
        tech_fields = [
            'turnover_rate', 'volume_ratio', 'pe', 'pb', 'ps', 'dv_ratio',
            'ps_ttm', 'pe_ttm', 'dv_ttm', 'total_mv',
            'kdj_qfq', 'kdj_d_qfq', 'kdj_k_qfq', 'rsi_qfq_12',
            'macd_qfq', 'macd_dea_qfq', 'macd_dif_qfq', 'atr_qfq', 'mtmma_qfq',
            'turnover_rate_f'
        ]

        # 资金流向字段
        money_fields = [
            'net_amount', 'buy_elg_amount', 'buy_lg_amount',
            'buy_md_amount', 'buy_sm_amount'
        ]

        # 融资融券字段
        margin_fields = ['rzye', 'rqye']

        # 财务指标字段
        fina_fields = [
            'roe', 'roa', 'roa2_yearly', 'profit_to_gr',
            'q_profit_yoy', 'q_eps', 'assets_turn'
        ]

        all_fields = base_fields + tech_fields + money_fields + margin_fields + fina_fields

        # 过滤出实际存在的字段
        available_fields = [f for f in all_fields if f in merged_df.columns]
        logger.info(f"将转换 {len(available_fields)} 个字段")

        for field in available_fields:
            try:
                self.dataframe_to_bin(merged_df, field)
            except Exception as e:
                logger.error(f"转换字段 {field} 失败: {e}")

        for target, source in QLIB_STD_OHLCV_FROM.items():
            if source not in merged_df.columns:
                continue
            try:
                self.dataframe_to_bin(merged_df, target, freq='day', source_field=source)
            except Exception as e:
                logger.error(f"转换标准字段 {target}（来自 {source}）失败: {e}")

        logger.info(f"Qlib格式转换完成！数据保存在: {self.output_dir}")


class DataPipeline:
    """数据处理管道：从Tushare获取数据并转换为Qlib格式（集成智能频率控制）"""

    # stk_factor_pro接口需要的字段
    STK_FACTOR_FIELDS = [
        'ts_code', 'trade_date', 'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq',
        'vol', 'amount', 'turnover_rate', 'volume_ratio', 'pe', 'pb', 'ps',
        'dv_ratio', 'ps_ttm', 'pe_ttm', 'dv_ttm', 'total_mv',
        'kdj_qfq', 'kdj_d_qfq', 'kdj_k_qfq', 'rsi_qfq_12',
        'macd_qfq', 'macd_dea_qfq', 'macd_dif_qfq', 'atr_qfq', 'mtmma_qfq',
        'turnover_rate_f'
    ]

    # moneyflow_dc接口需要的字段
    MONEYFLOW_FIELDS = [
        'trade_date', 'ts_code', 'net_amount', 'buy_elg_amount',
        'buy_lg_amount', 'buy_md_amount', 'buy_sm_amount'
    ]

    # margin_detail接口需要的字段
    MARGIN_FIELDS = [
        'trade_date', 'ts_code', 'rzye', 'rqye'
    ]

    # fina_indicator接口需要的字段
    FINA_INDICATOR_FIELDS = [
        'ts_code', 'end_date', 'roe', 'roa', 'roa2_yearly',
        'profit_to_gr', 'q_profit_yoy', 'q_eps', 'assets_turn'
    ]

    def __init__(self, token: str, output_dir: str, start_date: str, end_date: str,
                 max_stocks: int = None, high_speed: bool = False,
                 csv_dir: str = None, download_workers: int = 4):
        """
        初始化数据管道

        参数：
            token: Tushare API token
            output_dir: Qlib格式输出目录
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            max_stocks: 最大处理股票数量（用于测试，None表示全部）
            high_speed: 是否启用高速模式（需要8000+积分）
            csv_dir: CSV文件保存目录（默认为output_dir/csv_data）
        """
        # 创建API频率限制器
        self.rate_limiter = APIRateLimiter(high_speed=high_speed)

        # 创建数据获取器（传入频率限制器）
        self.fetcher = TushareDataFetcher(token, rate_limiter=self.rate_limiter)
        self.converter = QlibDataConverter(output_dir)
        self.start_date = start_date
        self.end_date = end_date
        self.max_stocks = max_stocks
        self.download_workers = max(1, int(download_workers))

        # CSV数据目录（用于中间存储）
        # 默认统一到固定目录，避免测试与正式流程路径不一致。
        if csv_dir:
            self.csv_dir = Path(csv_dir)
        else:
            self.csv_dir = Path(r"D:\qlib_data\csv_data")

        self.csv_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"数据管道初始化完成 (高速模式: {high_speed})")
        logger.info(f"CSV数据目录: {self.csv_dir}")
        logger.info(f"Qlib输出目录: {self.converter.output_dir}")
        logger.info(f"下载并发线程数: {self.download_workers}")

    def _download_single_stock(self, ts_code: str) -> str:
        """
        下载并保存单只股票数据。
        返回值：
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
            logger.error(f"处理 {ts_code} 时出错: {e}")
            return "fail"

    def fetch_all_data_for_stock(self, ts_code: str) -> pd.DataFrame:
        """
        获取单只股票的所有数据并合并

        参数：
            ts_code: 股票代码

        返回：
            合并后的DataFrame
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"正在处理股票: {ts_code}")
        logger.info(f"{'='*60}")

        # 1. 获取技术面因子数据
        logger.info("  [1/4] 获取技术面因子数据...")
        factor_df = self.fetcher.get_stk_factor_pro(
            ts_code, self.start_date, self.end_date,
            self.STK_FACTOR_FIELDS
        )

        if factor_df.empty:
            logger.warning(f"  {ts_code} 无技术因子数据，跳过")
            return pd.DataFrame()

        # 设置索引
        factor_df['datetime'] = pd.to_datetime(factor_df['trade_date'])
        factor_df.set_index(['datetime'], inplace=True)

        # 2. 获取资金流向数据
        logger.info("  [2/4] 获取资金流向数据...")
        money_df = self.fetcher.get_moneyflow_dc(
            ts_code, self.start_date, self.end_date,
            self.MONEYFLOW_FIELDS
        )

        if not money_df.empty:
            money_df['datetime'] = pd.to_datetime(money_df['trade_date'])
            money_df.set_index(['datetime'], inplace=True)
            # 合并资金流向数据
            factor_df = factor_df.join(money_df[
                ['net_amount', 'buy_elg_amount', 'buy_lg_amount',
                 'buy_md_amount', 'buy_sm_amount']
            ], how='left')
        else:
            logger.info("  无资金流向数据")

        # 3. 获取融资融券数据
        logger.info("  [3/4] 获取融资融券数据...")
        margin_df = self.fetcher.get_margin_detail(
            ts_code, self.start_date, self.end_date,
            self.MARGIN_FIELDS
        )

        if not margin_df.empty:
            margin_df['datetime'] = pd.to_datetime(margin_df['trade_date'])
            margin_df.set_index(['datetime'], inplace=True)
            # 合并融资融券数据
            factor_df = factor_df.join(margin_df[['rzye', 'rqye']], how='left')
        else:
            logger.info("  无融资融券数据")

        # 4. 获取财务指标数据
        logger.info("  [4/4] 获取财务指标数据...")
        fina_df = self.fetcher.get_fina_indicator(
            ts_code, self.start_date, self.end_date,
            self.FINA_INDICATOR_FIELDS
        )

        if not fina_df.empty:
            # 财务指标使用 end_date（报告期）作为日期
            # 关键修复：规范化日期格式，确保只保留日期部分（不包含时间）
            fina_df['datetime'] = pd.to_datetime(fina_df['end_date']).dt.normalize()
            fina_df.set_index(['datetime'], inplace=True)

            # 同样规范化factor_df的索引（确保日期格式一致）
            factor_df_index = factor_df.index
            if hasattr(factor_df_index, 'to_frame'):
                # MultiIndex情况，重置后再处理
                pass
            else:
                # 确保factor_df的datetime索引也是规范化的
                factor_df.index = pd.to_datetime(factor_df.index).dt.normalize()

            # 合并财务指标数据
            factor_df = factor_df.join(fina_df[
                ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',
                 'q_profit_yoy', 'q_eps', 'assets_turn']
            ], how='left')

            # 检查是否成功匹配到任何数据
            roe_col = 'roe' if 'roe' in factor_df.columns else None
            if roe_col and factor_df[roe_col].notna().any():
                logger.info(f"  ✅ 成功匹配到 {factor_df[roe_col].notna().sum()} 条财务指标记录")
            elif roe_col:
                logger.warning(f"  ⚠️  join未匹配到任何财务指标数据，尝试使用merge方式...")

                # 备用方案：使用merge而不是join
                fina_for_merge = fina_df.reset_index()
                fina_for_merge.rename(columns={'datetime': 'datetime_fina'}, inplace=True)

                factor_reset = factor_df.reset_index()
                factor_reset['datetime'] = pd.to_datetime(factor_reset['datetime']).dt.date
                fina_for_merge['datetime_fina'] = pd.to_datetime(fina_for_merge['datetime_fina']).dt.date

                # 使用merge，允许"小于等于"匹配
                merged_data = []
                for idx, row in factor_reset.iterrows():
                    # 找到最近的财务报告日期（<= 当前日期）
                    matching_fina = fina_for_merge[fina_for_merge['datetime_fina'] <= row['datetime']]
                    if not matching_fina.empty:
                        # 取最近的一条
                        latest_fina = matching_fina.iloc[-1]
                        for col in ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',
                                    'q_profit_yoy', 'q_eps', 'assets_turn']:
                            if col in latest_fina.index:
                                row[col] = latest_fina[col]
                    merged_data.append(row)

                factor_df = pd.DataFrame(merged_data)
                factor_df.set_index(['instrument', 'datetime'], inplace=True)

                if 'roe' in factor_df.columns and factor_df['roe'].notna().any():
                    logger.info(f"  ✅ merge方式成功！匹配到 {factor_df['roe'].notna().sum()} 条记录")
            else:
                logger.warning("  ⚠️  无ROE字段")

            # 前向填充(ffill)将季度数据填充到每个交易日
            if len(factor_df) > 0:
                factor_df = factor_df.sort_index()
                fina_cols = ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',
                             'q_profit_yoy', 'q_eps', 'assets_turn']
                existing_cols = [col for col in fina_cols if col in factor_df.columns]
                if existing_cols:
                    # 统计填充前的非空数量
                    before_fill = {col: factor_df[col].notna().sum() for col in existing_cols}

                    # 执行前向填充
                    factor_df[existing_cols] = factor_df[existing_cols].ffill()

                    # 统计填充后的非空数量
                    after_fill = {col: factor_df[col].notna().sum() for col in existing_cols}

                    # 记录日志
                    fill_info = ', '.join([f"{col}:{before_fill[col]}→{after_fill[col]}" for col in existing_cols])
                    logger.info(f"  ✅ 财务指标前向填充完成 ({fill_info})")
        else:
            logger.info("  ⚠️  无财务指标数据（接口可能返回空或积分不足）")

        # 添加股票代码作为instrument索引
        factor_df['instrument'] = ts_code
        factor_df.reset_index(inplace=True)
        factor_df.set_index(['instrument', 'datetime'], inplace=True)

        logger.info(f"  {ts_code} 数据获取完成，共 {len(factor_df)} 条记录")
        return factor_df

    def save_stock_to_csv(self, ts_code: str, df: pd.DataFrame) -> bool:
        """
        将单只股票数据保存为CSV文件

        参数：
            ts_code: 股票代码 (如 '000001.SZ')
            df: 股票数据DataFrame

        返回：
            是否保存成功
        """
        if df.empty:
            return False

        try:
            # 与 qlib dump_bin 一致：文件名 stem 即为证券代码，如 000001.sz -> 000001.SZ
            csv_filename = f"{ts_code.lower()}.csv"
            csv_path = self.csv_dir / csv_filename

            # 重置索引，将instrument和 datetime变为普通列
            df_to_save = df.reset_index()
            add_qlib_standard_ohlcv_columns(df_to_save)

            # 保存为CSV
            df_to_save.to_csv(csv_path, index=False, encoding='utf-8-sig')

            logger.info(f"  💾 已保存: {csv_path.name} ({len(df)} 条记录)")
            return True

        except Exception as e:
            logger.error(f"保存 {ts_code} CSV失败: {e}")
            return False

    def download_all_to_csv(self, stock_codes: List[str] = None) -> Tuple[int, int]:
        """
        阶段1：下载所有股票数据并保存为CSV文件

        参数：
            stock_codes: 要处理的股票代码列表（None表示处理全部）

        返回：
            (成功数量, 失败数量)
        """
        logger.info("\n" + "="*80)
        logger.info("【阶段1/2】开始下载数据到CSV文件")
        logger.info("="*80)
        logger.info(f"时间范围: {self.start_date} ~ {self.end_date}")
        logger.info(f"CSV目录: {self.csv_dir}")
        logger.info(f"频率限制模式: {'高速(8000+积分)' if self.rate_limiter.high_speed else '标准(5000积分)'}")

        # 获取股票列表
        stock_list = self.fetcher.get_stock_list()

        if stock_codes is None:
            stock_codes = stock_list['ts_code'].tolist()

        # 限制处理的股票数量（用于测试）
        if self.max_stocks:
            stock_codes = stock_codes[:self.max_stocks]
            logger.info(f"测试模式：仅处理前 {self.max_stocks} 只股票")

        logger.info(f"待处理股票数量: {len(stock_codes)}")

        success_count = 0
        fail_count = 0
        skipped_count = 0
        total = len(stock_codes)

        workers = min(self.download_workers, total) if total > 0 else 1
        logger.info(f"下载执行模式: {'串行' if workers == 1 else f'并发({workers}线程)'}")

        if workers == 1:
            pbar = tqdm(stock_codes, total=total, desc="下载CSV", unit="stock")
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
                    logger.info(f"\n📊 当前进度: 成功={success_count}, 失败={fail_count}, 跳过={skipped_count}")
            pbar.close()
        else:
            futures = {}
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for ts_code in stock_codes:
                    futures[executor.submit(self._download_single_stock, ts_code)] = ts_code

                pbar = tqdm(total=total, desc="下载CSV", unit="stock")
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
                        logger.info(f"\n📊 当前进度: 成功={success_count}, 失败={fail_count}, 跳过={skipped_count}")
                pbar.close()

        logger.info(f"\n{'='*60}")
        logger.info("【阶段1完成】CSV下载统计:")
        logger.info(f"  ✅ 成功: {success_count} 只股票")
        logger.info(f"  ⏭️  跳过(已存在): {skipped_count} 只股票")
        logger.info(f"  ❌ 失败: {fail_count} 只股票")
        logger.info(f"  📁 CSV文件保存在: {self.csv_dir}")

        return success_count, fail_count

    def convert_csv_to_qlib(self) -> bool:
        """
        阶段2：使用微软 qlib 官方 scripts/dump_bin.py 中的 DumpDataAll 转为 Qlib 目录结构。

        正确目录布局为 features/<股票代码目录>/<字段>.day.bin（见 qlib LocalFeatureProvider），
        不可用单文件 features/<字段>.day.bin 混存多只股票。
        """
        logger.info("\n" + "="*80)
        logger.info("【阶段2/2】从CSV转换到Qlib格式（官方 dump_bin.DumpDataAll）")
        logger.info("="*80)
        logger.info(f"CSV目录: {self.csv_dir}")
        logger.info(f"Qlib输出目录: {self.converter.output_dir}")

        csv_files = sorted(self.csv_dir.glob("*.csv"))
        if not csv_files:
            logger.error("未找到任何CSV文件！请先执行阶段1（下载）")
            return False

        logger.info(f"找到 {len(csv_files)} 个CSV文件")

        for csv_path in csv_files:
            try:
                df_patch = pd.read_csv(csv_path, encoding="utf-8-sig")
                add_qlib_standard_ohlcv_columns(df_patch)
                df_patch.to_csv(csv_path, index=False, encoding="utf-8-sig")
            except Exception as e:
                logger.warning("为 %s 写入标准 OHLCV 列失败: %s", csv_path.name, e)

        sample = pd.read_csv(csv_files[0], nrows=512, encoding="utf-8-sig")
        skip_cols = {"datetime", "instrument", "trade_date", "ts_code"}
        feature_cols = [
            c
            for c in sample.columns
            if c not in skip_cols and pd.api.types.is_numeric_dtype(sample[c])
        ]
        if not feature_cols:
            logger.error("未能从CSV中识别数值特征列，请检查列名与数据类型")
            return False

        include_fields = ",".join(feature_cols)
        logger.info(f"将 dump 的数值特征列数: {len(feature_cols)}")

        qlib_main = Path(os.environ.get("QLIB_SOURCE", r"D:\quant_project\qlib-main"))
        scripts_dir = qlib_main / "scripts"
        dump_script = scripts_dir / "dump_bin.py"
        if not dump_script.is_file():
            logger.error(
                "未找到 qlib 源码中的 dump_bin.py: %s。请克隆 qlib 或设置环境变量 QLIB_SOURCE 指向 qlib 根目录",
                dump_script,
            )
            return False

        if str(scripts_dir) not in sys.path:
            sys.path.insert(0, str(scripts_dir))

        try:
            from dump_bin import DumpDataAll
        except ImportError as exc:
            logger.error(
                "无法导入 dump_bin（需要已安装 qlib: pip install qlib）。详情: %s",
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
            logger.exception("dump_bin 转换失败")
            return False

        logger.info("\n" + "="*80)
        logger.info("【阶段2完成】Qlib格式转换成功（官方布局: calendars/ + instruments/ + features/<代码>/）")
        logger.info("="*80)
        logger.info("📁 Qlib数据目录: %s", self.converter.output_dir)
        logger.info("初始化示例: qlib.init(provider_uri=r'%s')", out_dir)

        return True

    def run(self, stock_codes: List[str] = None):
        """
        运行完整的数据管道（两阶段）

        参数：
            stock_codes: 要处理的股票代码列表（None表示处理全部）
        """
        logger.info("\n" + "="*80)
        logger.info("开始执行完整数据管道（两阶段）")
        logger.info("="*80)

        # 阶段1：下载到CSV
        success, fail = self.download_all_to_csv(stock_codes)

        if success == 0 and fail == 0:
            logger.error("没有获取到任何数据！")
            return

        # 阶段2：转换为Qlib格式
        if success > 0:
            self.convert_csv_to_qlib()
        else:
            logger.warning("没有成功的下载，跳过Qlib转换阶段")

        # 打印最终统计
        self._print_final_stats(success, fail, None)

    def _print_rate_stats(self):
        """打印API调用统计信息"""
        stats = self.rate_limiter.get_stats()
        logger.info("\n📊 API调用统计（最近1分钟）:")
        for api_name, info in stats.items():
            logger.info(
                f"   {api_name:20s}: "
                f"{info['recent_requests']:3d}次 / "
                f"{info['rate_limit']:5.1f}次/分钟 "
                f"(利用率: {info['utilization']:5.1f}%)"
            )

    def _print_final_stats(self, success_count: int, fail_count: int, merged_df: Optional[pd.DataFrame]):
        """打印最终统计信息"""
        print("\n📊 数据统计:")
        print(f"  ✅ 成功处理股票: {success_count}")
        print(f"  ❌ 失败股票: {fail_count}")
        if merged_df is not None:
            print(f"  📈 总记录数: {len(merged_df)}")
            print(f"  🗂️  包含字段: {len(merged_df.columns)} 个")
        else:
            print("  📈 总记录数: (阶段2 已用 dump_bin，未保留合并 DataFrame)")
        print(f"  📁 输出目录: {self.converter.output_dir}")

        # 打印API调用统计
        stats = self.rate_limiter.get_stats()
        print(f"\n⏱️  API频率使用情况:")
        for api_name, info in stats.items():
            print(f"  {api_name}: {info['recent_requests']} 次/分钟 (限制: {info['rate_limit']:.0f}次/分钟)")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='从Tushare获取金融数据并转换为Qlib格式',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
环境变量支持：
  TUSHARE_API_KEY  - Tushare API Token（优先级最高）
  TUSHARE_TOKEN    - 兼容旧版环境变量名

示例用法：
  # 使用环境变量Token
  set TUSHARE_API_KEY=your_token
  python tushare_to_qlib.py --start_date 20200101 --end_date 20231231

  # 使用命令行参数Token
  python tushare_to_qlib.py --token YOUR_TOKEN --start_date 20200101 --end_date 20231231

  # 测试模式（只处理10只股票）
  python tushare_to_qlib.py --start_date 20230101 --end_date 20231231 --max_stocks 10

  # 高速模式（需要8000+积分）
  python tushare_to_qlib.py --start_date 20230101 --end_date 20231231 --high-speed

  # 自定义输出目录
  python tushare_to_qlib.py --token YOUR_TOKEN --output_dir D:/my_qlib_data

频率限制说明：
  标准模式 (默认): 适合5000积分用户
    - stk_factor_pro: 30次/分钟
    - 其他接口: 20-30次/分钟

  高速模式 (--high-speed): 需要8000+积分
    - stk_factor_pro: 500次/分钟
    - 其他接口: 40-120次/分钟
        """
    )

    parser.add_argument('--token', type=str, default=None,
                        help='Tushare Pro API Token（可选，也可使用环境变量 TUSHARE_API_KEY）')
    parser.add_argument('--start_date', type=str, default='20200101',
                        help='开始日期 (YYYYMMDD)，默认: 20200101')
    parser.add_argument('--end_date', type=str, default='20231231',
                        help='结束日期 (YYYYMMDD)，默认: 20231231')
    parser.add_argument('--output_dir', type=str,
                        default=r'D:\qlib_data\qlib_data',
                        help=f'Qlib数据输出目录，默认: D:\\qlib_data\\qlib_data')
    parser.add_argument('--csv_dir', type=str, default=r'D:\qlib_data\csv_data',
                        help='CSV文件保存目录（默认: D:\\qlib_data\\csv_data）')
    parser.add_argument('--max_stocks', type=int, default=None,
                        help='最大处理股票数量（用于测试），默认: 全部')
    parser.add_argument('--high-speed', action='store_true',
                        help='启用高速模式（需要8000+积分，大幅提高请求速率）')
    parser.add_argument('--download_workers', type=int, default=4,
                        help='下载阶段并发线程数，默认: 4（建议 2-8）')
    parser.add_argument('--stage', type=str, default='all',
                        choices=['all', 'download', 'convert'],
                        help='执行阶段: all(完整流程)/download(仅下载CSV)/convert(仅转Qlib)，默认: all')

    args = parser.parse_args()

    # 获取Token（优先级：命令行参数 > 环境变量 > 配置文件）
    token = args.token

    if not token:
        # 尝试从环境变量获取
        token = os.environ.get('TUSHARE_API_KEY', '').strip()
        if token:
            logger.info(f"✅ 从环境变量 TUSHARE_API_KEY 获取到Token")

    if not token:
        # 尝试备用环境变量
        token = os.environ.get('TUSHARE_TOKEN', '').strip()
        if token:
            logger.info("⚠️  从环境变量 TUSHARE_TOKEN 获取到Token（建议使用 TUSHARE_API_KEY）")

    if not token:
        # 尝试配置文件
        token = get_tushare_token()

    if not token:
        print("\n❌ 错误：未提供Tushare API Token！")
        print("\n请通过以下任一方式提供Token：")
        print("  1. 命令行参数: --token YOUR_TOKEN")
        print("  2. 环境变量: set TUSHARE_API_KEY=YOUR_TOKEN")
        print("  3. 配置文件: config/secrets.yaml")
        print("\n示例:")
        print("  set TUSHARE_API_KEY=your_token_here")
        print("  python tushare_to_qlib.py --start_date 20230101 --end_date 20231231\n")
        sys.exit(1)

    # 验证日期格式
    try:
        pd.to_datetime(args.start_date)
        pd.to_datetime(args.end_date)
    except ValueError:
        logger.error("日期格式错误，请使用 YYYYMMDD 格式")
        sys.exit(1)

    # 创建管道
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

    # 根据stage参数执行不同操作
    if args.stage == 'all':
        # 完整流程：下载 + 转换
        pipeline.run()
    elif args.stage == 'download':
        # 仅下载到CSV
        success, fail = pipeline.download_all_to_csv()
        print(f"\n📊 下载完成！成功: {success}, 失败: {fail}")
        print(f"📁 CSV目录: {pipeline.csv_dir}")
    elif args.stage == 'convert':
        # 仅从CSV转换为Qlib
        success = pipeline.convert_csv_to_qlib()
        if not success:
            sys.exit(1)


if __name__ == '__main__':
    main()
