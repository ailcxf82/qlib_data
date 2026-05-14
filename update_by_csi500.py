#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
根据 csi500.txt 文件更新数据的脚本

用法：
    python update_by_csi500.py
    python update_by_csi500.py --input_file D:/qlib_data/qlib_data/instruments/csi500.txt
    python update_by_csi500.py --start_date 20200101 --end_date 20260511
    python update_by_csi500.py --stage download   # 仅拉 CSV，便于中间校验
    python update_by_csi500.py --stage convert     # 仅 DumpDataUpdate / DumpDataAll
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_instruments_file(file_path: Path) -> Tuple[List[str], str, str]:
    """
    解析 instruments 文件（如 csi500.txt）
    返回：(股票代码列表, 最早开始日期, 最晚结束日期)
    """
    stock_codes = set()
    start_dates = []
    end_dates = []

    with open(file_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 3:
                logger.warning(f"第 {i} 行格式不正确，跳过: {line}")
                continue
            ts_code, start_date, end_date = parts
            stock_codes.add(ts_code.strip())
            start_dates.append(start_date.strip())
            end_dates.append(end_date.strip())

    if not stock_codes:
        raise ValueError(f"文件 {file_path} 中没有有效的股票代码")

    earliest_start = min(start_dates)
    latest_end = max(end_dates)

    logger.info(f"从 {file_path} 解析到 {len(stock_codes)} 只股票")
    logger.info(f"日期范围: {earliest_start} ~ {latest_end}")

    return sorted(list(stock_codes)), earliest_start, latest_end


def get_tushare_token() -> Optional[str]:
    """从环境变量或配置文件获取 Tushare Token"""
    token = os.environ.get("TUSHARE_API_KEY", "").strip()
    if token:
        logger.info("从环境变量 TUSHARE_API_KEY 获取 Token")
        return token
    token = os.environ.get("TUSHARE_TOKEN", "").strip()
    if token:
        logger.info("从环境变量 TUSHARE_TOKEN 获取 Token")
        return token
    try:
        import yaml
        config_path = Path("config/secrets.yaml")
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                if data and "tushare" in data:
                    token = data["tushare"].get("token", "").strip()
                    if token:
                        logger.info("从 config/secrets.yaml 获取 Token")
                        return token
    except Exception:
        pass
    return None


def main():
    default_input = Path(__file__).parent / "data" / "csi500.txt"
    default_output = Path(r"D:\qlib_data\qlib_data")
    default_csv = Path(r"D:\qlib_data\csv_data\csi500_update")
    today = datetime.now().strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description="根据 csi500.txt 更新数据")
    parser.add_argument("--token", type=str, default=None, help="Tushare Token")
    parser.add_argument(
        "--input_file",
        type=str,
        default=str(default_input),
        help=f"csi500.txt 文件路径，默认: {default_input}",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help="开始日期 YYYYMMDD，默认从文件中读取最早日期",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=today,
        help=f"结束日期 YYYYMMDD，默认今天 {today}",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=str(default_output),
        help=f"Qlib 数据输出目录，默认: {default_output}",
    )
    parser.add_argument(
        "--csv_dir",
        type=str,
        default=str(default_csv),
        help=f"CSV 临时目录，默认: {default_csv}",
    )
    parser.add_argument("--high_speed", action="store_true", default=True, help="高速模式（8000+ 积分），默认开启")
    parser.add_argument("--no_high_speed", dest="high_speed", action="store_false", help="关闭高速模式（5000 积分账号请加此项）")
    parser.add_argument("--download_workers", type=int, default=4, help="同步路径下载线程数（仅 fetch_mode=by_stock 同步实现使用）")
    parser.add_argument("--max_concurrent", type=int, default=16, help="异步并发数（async 流水线使用）")
    parser.add_argument(
        "--fetch_mode",
        type=str,
        default="by_date",
        choices=["by_date", "by_stock"],
        help="by_date: 按交易日批量拉全市场再分股票（增量更新强烈推荐，速度数十倍提升）;"
             " by_stock: 老逐股循环路径",
    )
    parser.add_argument(
        "--dump_mode",
        type=str,
        default="dump_update",
        choices=["dump_all", "dump_update"],
        help="dump_update: 增量追加（默认，配合 by_date 最快）; dump_all: 全量重建",
    )
    parser.add_argument(
        "--stage",
        type=str,
        default="all",
        choices=["all", "download", "convert"],
        help="all: 下载+转换; download: 仅更新 CSV; convert: 仅转 qlib（使用 dump_mode）",
    )
    args = parser.parse_args()

    input_path = Path(args.input_file)
    if not input_path.exists():
        logger.error(f"文件不存在: {input_path}")
        sys.exit(1)

    stock_codes, file_start_date, file_end_date = parse_instruments_file(input_path)

    effective_start_date = args.start_date or file_start_date.replace("-", "")
    effective_end_date = args.end_date

    try:
        import pandas as pd
        pd.to_datetime(effective_start_date, format="%Y%m%d")
        pd.to_datetime(effective_end_date, format="%Y%m%d")
    except ValueError:
        logger.error("日期格式须为 YYYYMMDD")
        sys.exit(1)

    token = args.token or get_tushare_token()
    if not token:
        logger.error("未找到 Tushare Token，请设置 TUSHARE_API_KEY 或 --token")
        sys.exit(1)

    logger.info(f"下载日期范围: {effective_start_date} ~ {effective_end_date}")
    logger.info(f"输出目录: {args.output_dir}")
    logger.info(f"CSV目录: {args.csv_dir}")
    logger.info(
        f"fetch_mode={args.fetch_mode}, dump_mode={args.dump_mode}, "
        f"high_speed={args.high_speed}, stage={args.stage}"
    )

    t0 = time.time()
    from tushare_to_qlib_async import AsyncDataPipeline

    pipeline = AsyncDataPipeline(
        token=token,
        output_dir=args.output_dir,
        start_date=effective_start_date,
        end_date=effective_end_date,
        high_speed=args.high_speed,
        csv_dir=args.csv_dir,
        max_concurrent=args.max_concurrent,
        fetch_mode=args.fetch_mode,
    )

    if args.stage == "all":
        asyncio.run(pipeline.run(stock_codes, dump_mode=args.dump_mode))
    elif args.stage == "download":
        if args.fetch_mode == "by_date":
            success, fail = asyncio.run(pipeline.download_all_to_csv_by_date(stock_codes))
        else:
            success, fail = asyncio.run(pipeline.download_all_to_csv(stock_codes))
        logger.info(f"Download stage done: success={success}, fail={fail}")
    elif args.stage == "convert":
        ok = pipeline.convert_csv_to_qlib(dump_mode=args.dump_mode)
        if not ok:
            sys.exit(1)

    logger.info(f"Pipeline finished in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
