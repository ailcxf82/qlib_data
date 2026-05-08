#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从 Tushare stock_st 接口拉取 ST 股票列表，保存为 CSV。

接口说明：
    - 接口名：stock_st
    - 描述：获取 ST 股票列表，可根据交易日期获取历史上每天的 ST 列表
    - 权限：3000 积分起
    - 更新：每天上午 9:20
    - 限量：单次最大返回 1000 行数据，可循环提取
    - 数据起始：20160101（更早历史无法补齐）
    - 输入参数：ts_code / trade_date / start_date / end_date
    - 输出字段：ts_code, name, trade_date, type, type_name

用法：
    python fetch_stock_st.py
    python fetch_stock_st.py --start_date 20200101 --end_date 20260506
    python fetch_stock_st.py --output D:/qlib_data/qlib_data/is_st.csv
    python fetch_stock_st.py --token YOUR_TOKEN

环境变量（任选其一）：
    TUSHARE_API_KEY
    TUSHARE_TOKEN

配置文件：
    config/secrets.yaml  ->  tushare.token
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

_MAX_ROWS_PER_REQUEST = 1000
_REQUEST_INTERVAL_SEC = 0.35
_RETRY_TIMES = 3
_RETRY_BASE_DELAY = 2.0
_DEFAULT_OUTPUT = Path(r"D:\qlib_data\qlib_data\is_st.csv")


def _resolve_token(cli_token: Optional[str]) -> Optional[str]:
    if cli_token:
        return cli_token.strip()
    for env_key in ("TUSHARE_API_KEY", "TUSHARE_TOKEN"):
        t = os.environ.get(env_key, "").strip()
        if t:
            logger.info("从环境变量 %s 获取 Token", env_key)
            return t
    try:
        import yaml

        config_path = Path("config/secrets.yaml")
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                if data and "tushare" in data:
                    t = data["tushare"].get("token", "").strip()
                    if t:
                        logger.info("从 config/secrets.yaml 获取 Token")
                        return t
    except Exception:
        pass
    return None


def _month_chunks(
    start_yyyymmdd: str, end_yyyymmdd: str
) -> List[Tuple[str, str]]:
    start = pd.to_datetime(start_yyyymmdd, format="%Y%m%d")
    end = pd.to_datetime(end_yyyymmdd, format="%Y%m%d")
    chunks: List[Tuple[str, str]] = []
    cur = pd.Timestamp(year=start.year, month=start.month, day=1)
    end_month_first = pd.Timestamp(year=end.year, month=end.month, day=1)
    while cur <= end_month_first:
        month_end = (cur + pd.offsets.MonthEnd(0)).normalize()
        chunk_start = max(start, cur)
        chunk_end = min(end, month_end)
        if chunk_start <= chunk_end:
            chunks.append(
                (chunk_start.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d"))
            )
        cur = cur + pd.DateOffset(months=1)
    return chunks


def _safe_request(pro, **kwargs) -> pd.DataFrame:
    for attempt in range(_RETRY_TIMES):
        try:
            df = pro.stock_st(**kwargs)
            return df if df is not None and not df.empty else pd.DataFrame()
        except Exception as e:
            err_msg = str(e).lower()
            is_rate_limit = any(
                kw in err_msg
                for kw in ("limit", "too many", "freq", "rate", "quick")
            )
            delay = _RETRY_BASE_DELAY * (2 ** attempt)
            if is_rate_limit:
                delay *= 2
            logger.warning(
                "请求失败 (第%d次): %s，等待 %.1f 秒后重试",
                attempt + 1,
                e,
                delay,
            )
            time.sleep(delay)
    logger.error("重试 %d 次后仍失败，跳过: %s", _RETRY_TIMES, kwargs)
    return pd.DataFrame()


def fetch_stock_st(
    pro,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
) -> pd.DataFrame:
    """
    按月分段拉取 stock_st 数据。

    若某月返回行数达到 _MAX_ROWS_PER_REQUEST 上限，
    则对该月按日逐天请求以确保数据完整。
    """
    chunks = _month_chunks(start_yyyymmdd, end_yyyymmdd)
    logger.info(
        "共 %d 个月段，从 %s 到 %s", len(chunks), start_yyyymmdd, end_yyyymmdd
    )

    all_frames: List[pd.DataFrame] = []

    for sd, ed in chunks:
        logger.info("拉取 stock_st %s ~ %s", sd, ed)
        df = _safe_request(pro, start_date=sd, end_date=ed)
        time.sleep(_REQUEST_INTERVAL_SEC)

        if len(df) >= _MAX_ROWS_PER_REQUEST:
            logger.info(
                "月段 %s~%s 返回 %d 行（达上限），改为按日请求",
                sd,
                ed,
                len(df),
            )
            all_frames.append(df)
            _fetch_by_day(pro, sd, ed, all_frames)
        elif not df.empty:
            all_frames.append(df)
        else:
            logger.debug("月段 %s~%s 无数据", sd, ed)

    if not all_frames:
        return pd.DataFrame(columns=["ts_code", "name", "trade_date", "type", "type_name"])

    result = pd.concat(all_frames, ignore_index=True)
    result = result.drop_duplicates(subset=["ts_code", "trade_date"], keep="first")
    result = result.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    logger.info("合计 %d 条 ST 记录", len(result))
    return result


def _fetch_by_day(
    pro,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    out_frames: List[pd.DataFrame],
) -> None:
    start = pd.to_datetime(start_yyyymmdd, format="%Y%m%d")
    end = pd.to_datetime(end_yyyymmdd, format="%Y%m%d")
    trade_cal = pro.trade_cal(
        exchange="SSE", start_date=start_yyyymmdd, end_date=end_yyyymmdd, is_open="1"
    )
    time.sleep(_REQUEST_INTERVAL_SEC)
    if trade_cal is None or trade_cal.empty:
        return
    trade_dates = sorted(trade_cal["cal_date"].tolist())
    for td in trade_dates:
        df = _safe_request(pro, trade_date=td)
        time.sleep(_REQUEST_INTERVAL_SEC)
        if not df.empty:
            out_frames.append(df)


def main() -> None:
    today = datetime.now().strftime("%Y%m%d")

    parser = argparse.ArgumentParser(
        description="从 Tushare stock_st 接口拉取 ST 股票列表，保存为 CSV"
    )
    parser.add_argument("--token", type=str, default=None, help="Tushare Token")
    parser.add_argument(
        "--start_date",
        type=str,
        default="20200101",
        help="开始日期 YYYYMMDD，默认 20200101",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=today,
        help=f"结束日期 YYYYMMDD，默认今天 {today}",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(_DEFAULT_OUTPUT),
        help=f"输出 CSV 路径，默认: {_DEFAULT_OUTPUT}",
    )
    args = parser.parse_args()

    token = _resolve_token(args.token)
    if not token:
        logger.error("未找到 Tushare Token，请设置 TUSHARE_API_KEY 或 --token")
        sys.exit(1)

    try:
        pd.to_datetime(args.start_date, format="%Y%m%d")
        pd.to_datetime(args.end_date, format="%Y%m%d")
    except ValueError:
        logger.error("日期格式须为 YYYYMMDD")
        sys.exit(1)

    start_dt = pd.to_datetime(args.start_date, format="%Y%m%d")
    if start_dt < pd.to_datetime("20160101", format="%Y%m%d"):
        logger.warning(
            "stock_st 接口数据从 20160101 开始，已自动调整起始日期为 20160101"
        )
        args.start_date = "20160101"

    import tushare as ts

    ts.set_token(token)
    pro = ts.pro_api()

    result = fetch_stock_st(pro, args.start_date, args.end_date)
    if result.empty:
        logger.warning("未获取到任何 ST 数据")
        sys.exit(1)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("已保存 %s，共 %d 行", output_path, len(result))

    date_range = result["trade_date"].agg(["min", "max"])
    logger.info(
        "日期范围: %s ~ %s，涉及 %d 只股票",
        date_range["min"],
        date_range["max"],
        result["ts_code"].nunique(),
    )


if __name__ == "__main__":
    main()
