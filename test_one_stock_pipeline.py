#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
单股票完整流程测试脚本：
1) 下载该股票 CSV
2) 转换为 Qlib 格式（调用官方 dump_bin）
3) 使用 qlib.D.features 回读验证

示例：
  conda run -n qlib_zhengshi python test_one_stock_pipeline.py \
    --stock 000001.SZ \
    --start_date 20230101 \
    --end_date 20231231 \
    --output_dir D:\\qlib_data\\qlib_data_one_stock_test \
    --csv_dir D:\\qlib_data\\csv_data
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

import pandas as pd

from tushare_to_qlib import DataPipeline


def get_token(cli_token: str | None) -> str:
    if cli_token and cli_token.strip():
        return cli_token.strip()

    token = os.environ.get("TUSHARE_API_KEY", "").strip()
    if token:
        return token

    token = os.environ.get("TUSHARE_TOKEN", "").strip()
    if token:
        return token

    raise RuntimeError("未找到 Tushare Token，请使用 --token 或环境变量 TUSHARE_API_KEY")


def parse_args():
    parser = argparse.ArgumentParser(description="单股票下载+转换+读取验证")
    parser.add_argument("--token", type=str, default=None, help="Tushare Token（可选）")
    parser.add_argument("--stock", type=str, required=True, help="股票代码，例如 000001.SZ")
    parser.add_argument("--start_date", type=str, default="20230101", help="开始日期 YYYYMMDD")
    parser.add_argument("--end_date", type=str, default="20231231", help="结束日期 YYYYMMDD")
    parser.add_argument(
        "--output_dir",
        type=str,
        default=r"D:\qlib_data\qlib_data_one_stock_test",
        help="Qlib 输出目录",
    )
    parser.add_argument(
        "--csv_dir",
        type=str,
        default=r"D:\qlib_data\csv_data",
        help="CSV 目录（默认 D:\\qlib_data\\csv_data）",
    )
    parser.add_argument(
        "--keep_old",
        action="store_true",
        help="保留旧输出目录（默认会清空输出目录）",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    token = get_token(args.token)
    stock = args.stock.strip().upper()

    # 日期格式快速校验
    pd.to_datetime(args.start_date)
    pd.to_datetime(args.end_date)

    output_dir = Path(args.output_dir)
    csv_dir = Path(args.csv_dir)

    if output_dir.exists() and not args.keep_old:
        print(f"🧹 清理旧目录: {output_dir}")
        shutil.rmtree(output_dir, ignore_errors=True)

    print("\n" + "=" * 80)
    print("单股票完整流程测试")
    print("=" * 80)
    print(f"股票: {stock}")
    print(f"区间: {args.start_date} ~ {args.end_date}")
    print(f"输出: {output_dir}")

    csv_dir.mkdir(parents=True, exist_ok=True)

    pipeline = DataPipeline(
        token=token,
        output_dir=str(output_dir),
        start_date=args.start_date,
        end_date=args.end_date,
        max_stocks=None,
        csv_dir=str(csv_dir),
    )

    # 1) 下载单只股票
    print("\n[1/3] 下载 CSV ...")
    success, fail = pipeline.download_all_to_csv(stock_codes=[stock])
    if success <= 0:
        print(f"❌ 下载失败，success={success}, fail={fail}")
        sys.exit(1)

    csv_path = csv_dir / f"{stock.lower()}.csv"
    if not csv_path.exists():
        print(f"❌ 未找到下载结果 CSV: {csv_path}")
        sys.exit(1)

    print(f"✅ CSV 已生成: {csv_path} ({csv_path.stat().st_size / 1024:.1f} KB)")

    # 2) 转换
    print("\n[2/3] 转换为 Qlib ...")
    ok = pipeline.convert_csv_to_qlib()
    if not ok:
        print("❌ Qlib 转换失败")
        sys.exit(1)

    # 检查关键目录
    cal = output_dir / "calendars" / "day.txt"
    inst = output_dir / "instruments" / "all.txt"
    feat_dir = output_dir / "features" / stock.lower()
    if not cal.exists() or not inst.exists() or not feat_dir.exists():
        print("❌ Qlib 目录结构不完整")
        print(f"  calendars/day.txt 存在: {cal.exists()}")
        print(f"  instruments/all.txt 存在: {inst.exists()}")
        print(f"  features/{stock.lower()} 存在: {feat_dir.exists()}")
        sys.exit(1)

    print(f"✅ 目录结构正常: {output_dir}")

    # 3) 回读验证
    print("\n[3/3] 使用 qlib 回读验证 ...")
    import qlib
    from qlib.data import D

    qlib.init(provider_uri=str(output_dir))
    df = D.features(
        [stock],
        ["$close_qfq", "$roe"],
        start_time=pd.to_datetime(args.start_date).strftime("%Y-%m-%d"),
        end_time=pd.to_datetime(args.end_date).strftime("%Y-%m-%d"),
    )

    if df is None or len(df) == 0:
        print("❌ 回读失败：D.features 返回为空")
        sys.exit(1)

    print(f"✅ 回读成功，记录数: {len(df)}")
    print(df.head(10).to_string())
    print("\n🎉 单股票下载+转换+回读 全流程通过")


if __name__ == "__main__":
    main()

