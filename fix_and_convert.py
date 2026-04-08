#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复脚本：重新下载单只股票数据（含ROE）并转换为Qlib

使用方法：
  python fix_and_convert.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from tushare_to_qlib import DataPipeline


def main():
    print("\n" + "="*80)
    print("🔧 修复脚本：重新生成CSV（含ROE）+ 转换为Qlib")
    print("="*80)

    # 获取Token
    token = os.environ.get('TUSHARE_API_KEY', '').strip()
    if not token:
        token = os.environ.get('TUSHARE_TOKEN', '').strip()

    if not token:
        print("\n❌ 错误：未找到环境变量 TUSHARE_API_KEY")
        print("请运行: $env:TUSHARE_API_KEY='your_token'")
        return

    print(f"✅ Token已获取")

    # 配置
    TEST_STOCK = "000001.SZ"
    START_DATE = "20230101"
    END_DATE = "20231231"
    OUTPUT_DIR = r"D:\qlib_data\qlib_data_test"
    CSV_DIR = r"D:\qlib_data\qlib_data_test\csv_data"

    # 步骤1: 删除旧的CSV文件（如果存在）
    old_csv = Path(CSV_DIR) / "000001.csv"
    if old_csv.exists():
        print(f"\n🗑️  删除旧CSV文件: {old_csv}")
        old_csv.unlink()

    # 创建管道
    pipeline = DataPipeline(
        token=token,
        output_dir=OUTPUT_DIR,
        start_date=START_DATE,
        end_date=END_DATE,
        max_stocks=None,  # 不限制，但我们会指定stock_codes
        csv_dir=CSV_DIR
    )

    # 步骤2: 重新下载这只股票的数据（现在会包含ROE等）
    print(f"\n【步骤1/2】重新下载 {TEST_STOCK} 的数据（已修复ROE填充逻辑）")
    print("-" * 60)

    success, fail = pipeline.download_all_to_csv(stock_codes=[TEST_STOCK])

    if success == 0:
        print(f"\n❌ 下载失败！")
        return

    print(f"\n✅ 下载成功！")

    # 步骤3: 检查新生成的CSV文件
    new_csv = Path(CSV_DIR) / "000001.csv"
    if new_csv.exists():
        print(f"\n【验证】检查新生成的CSV文件:")
        print("-" * 60)

        import pandas as pd
        df_check = pd.read_csv(new_csv, encoding='utf-8-sig')

        print(f"   文件大小: {new_csv.stat().st_size / 1024:.1f} KB")
        print(f"   数据形状: {df_check.shape}")
        print(f"   总字段数: {len(df_check.columns)}")

        # 检查ROE等字段
        fina_fields = ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',
                       'q_profit_yoy', 'q_eps', 'assets_turn']

        print(f"\n   📊 财务指标字段检查:")
        for field in fina_fields:
            if field in df_check.columns:
                non_null = df_check[field].notna().sum()
                total = len(df_check)
                pct = non_null / total * 100
                print(f"      {field:15s}: {non_null:5d}/{total:<5d} ({pct:5.1f}%) 非空")
            else:
                print(f"      {field:15s}: ❌ 字段不存在")

        # 显示前几行数据
        print(f"\n   📝 数据预览（前5行，最后7列是财务指标）:")
        display_cols = ['datetime'] + fina_fields
        available_cols = [c for c in display_cols if c in df_check.columns]
        if available_cols:
            print(df_check[available_cols].head())
    else:
        print(f"\n❌ CSV文件不存在: {new_csv}")
        return

    # 步骤4: 转换为Qlib格式
    print(f"\n\n【步骤2/2】转换为Qlib格式")
    print("-" * 60)

    convert_success = pipeline.convert_csv_to_qlib()

    if convert_success:
        print(f"\n✅ Qlib转换成功！")

        # 验证生成的Qlib文件
        qlib_dir = Path(OUTPUT_DIR)
        features_dir = qlib_dir / 'features'

        if features_dir.exists():
            bin_files = list(features_dir.glob('*.bin'))
            print(f"\n📁 生成的Qlib特征文件 ({len(bin_files)} 个):")

            # 特别关注ROE等财务指标的bin文件
            important_files = ['roe.day.bin', 'roa.day.bin', 'close_qfq.day.bin']
            for fname in important_files:
                fpath = features_dir / fname
                if fpath.exists():
                    size_kb = fpath.stat().st_size / 1024
                    print(f"   ✅ {fname:20s} ({size_kb:.1f} KB)")
                else:
                    print(f"   ❌ {fname:20s} (不存在)")

        print(f"\n🎉 全部完成！")
        print(f"   CSV文件: {new_csv}")
        print(f"   Qlib目录: {OUTPUT_DIR}")

    else:
        print(f"\n❌ Qlib转换失败！")
        return


if __name__ == '__main__':
    main()
