#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tushare到Qlib数据转换 - 测试脚本（支持两阶段流程）

用于快速验证数据获取和转换功能
只处理少量股票，适合开发调试使用

支持环境变量：
  TUSHARE_API_KEY - Tushare API Token（推荐）
  TUSHARE_TOKEN   - 兼容旧版

使用方法：
  # 完整测试（下载+转换）
  python test_tushare_to_qlib.py --mode single

  # 仅测试下载（生成CSV）
  python test_tushare_to_qlib.py --mode download_only

  # 从已有CSV转换为Qlib
  python test_tushare_to_qlib.py --mode convert_only

  # 验证生成的Qlib数据
  python test_tushare_to_qlib.py --mode verify
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from tushare_to_qlib import DataPipeline


def get_token():
    """
    获取Tushare Token

    优先级：
    1. 环境变量 TUSHARE_API_KEY
    2. 环境变量 TUSHARE_TOKEN
    """
    # 方法1: 环境变量 TUSHARE_API_KEY（推荐）
    token = os.environ.get('TUSHARE_API_KEY', '').strip()
    if token:
        print("✅ 从环境变量 TUSHARE_API_KEY 获取到Token")
        return token

    # 方法2: 环境变量 TUSHARE_TOKEN（兼容）
    token = os.environ.get('TUSHARE_TOKEN', '').strip()
    if token:
        print("⚠️  从环境变量 TUSHARE_TOKEN 获取到Token")
        return token

    return None


def test_single_stock_full():
    """测试单只股票的完整流程（下载CSV + 转换Qlib）"""
    print("\n" + "="*80)
    print("🧪 测试模式：单只股票完整流程")
    print("="*80)

    TOKEN = get_token()

    if not TOKEN:
        print("\n❌ 未检测到 Tushare Token！")
        print("\n请设置环境变量：")
        print("  PowerShell: $env:TUSHARE_API_KEY='your_token'")
        print("  CMD:       set TUSHARE_API_KEY=your_token")
        return

    TEST_STOCK = "000001.SZ"
    START_DATE = "20230101"
    END_DATE = "20231231"
    OUTPUT_DIR = r"D:\qlib_data\qlib_data_test"

    pipeline = DataPipeline(
        token=TOKEN,
        output_dir=OUTPUT_DIR,
        start_date=START_DATE,
        end_date=END_DATE,
        max_stocks=1
    )

    # 执行完整流程
    pipeline.run(stock_codes=[TEST_STOCK])

    print(f"\n✅ 测试完成！")
    print(f"📁 CSV文件保存在: {pipeline.csv_dir}")
    print(f"📁 Qlib数据保存在: {OUTPUT_DIR}")


def test_download_only():
    """仅测试下载阶段（生成CSV文件）"""
    print("\n" + "="*80)
    print("📥 测试模式：仅下载CSV")
    print("="*80)

    TOKEN = get_token()

    if not TOKEN:
        print("❌ 请先设置环境变量 TUSHARE_API_KEY")
        return

    START_DATE = "20230101"
    END_DATE = "20231231"
    OUTPUT_DIR = r"D:\qlib_data\qlib_data_test"
    CSV_DIR = r"D:\qlib_data\csv_data_test"

    pipeline = DataPipeline(
        token=TOKEN,
        output_dir=OUTPUT_DIR,
        start_date=START_DATE,
        end_date=END_DATE,
        max_stocks=5,  # 下载5只股票
        csv_dir=CSV_DIR
    )

    success, fail = pipeline.download_all_to_csv()

    print(f"\n📊 下载统计:")
    print(f"  ✅ 成功: {success} 只股票")
    print(f"  ❌ 失败: {fail} 只股票")
    print(f"\n📁 CSV目录: {pipeline.csv_dir}")

    # 列出生成的文件
    csv_files = list(pipeline.csv_dir.glob('*.csv'))
    print(f"\n📄 生成的CSV文件 ({len(csv_files)} 个):")
    for f in sorted(csv_files)[:10]:
        size_kb = f.stat().st_size / 1024
        print(f"   📄 {f.name} ({size_kb:.1f} KB)")


def test_convert_only():
    """仅测试从CSV转换为Qlib格式"""
    print("\n" + "="*80)
    print("🔄 测试模式：从CSV转换为Qlib格式")
    print("="*80)

    OUTPUT_DIR = r"D:\qlib_data\qlib_data_test"
    CSV_DIR = r"D:\qlib_data\csv_data_test"

    # 需要Token来获取stock_list（instruments需要）
    TOKEN = get_token()

    if not TOKEN:
        print("❌ 请先设置环境变量 TUSHARE_API_KEY（需要获取股票列表）")
        return

    pipeline = DataPipeline(
        token=TOKEN,
        output_dir=OUTPUT_DIR,
        start_date='20230101',
        end_date='20231231',
        csv_dir=CSV_DIR
    )

    success = pipeline.convert_csv_to_qlib()

    if success:
        print(f"\n✅ Qlib格式转换成功！")
        print(f"📁 数据保存在: {OUTPUT_DIR}")
    else:
        print(f"\n❌ 转换失败")


def test_multiple_stocks():
    """测试多只股票的数据获取"""
    print("\n" + "="*80)
    print("🧪 测试模式：多只股票 (前10只)")
    print("="*80)

    TOKEN = get_token()

    if not TOKEN:
        print("❌ 请先设置环境变量 TUSHARE_API_KEY")
        return

    START_DATE = "20230101"
    END_DATE = "20231231"
    OUTPUT_DIR = r"D:\qlib_data\qlib_data_test"

    pipeline = DataPipeline(
        token=TOKEN,
        output_dir=OUTPUT_DIR,
        start_date=START_DATE,
        end_date=END_DATE,
        max_stocks=10
    )

    pipeline.run()

    print(f"\n✅ 多股票测试完成！")


def verify_qlib_data():
    """验证生成的Qlib数据和CSV数据"""
    import numpy as np
    from pathlib import Path

    DATA_DIR = Path(r"D:\qlib_data\qlib_data_test")
    CSV_DIR = Path(r"D:\qlib_data\csv_data_test")

    print("\n" + "="*80)
    print("🔍 验证数据格式")
    print("="*80)

    # 1. 检查CSV文件
    print("\n【1/2】检查CSV文件:")
    if CSV_DIR.exists():
        csv_files = list(CSV_DIR.glob('*.csv'))
        print(f"  ✅ CSV目录存在: {CSV_DIR}")
        print(f"  📊 CSV文件数量: {len(csv_files)}")

        if csv_files:
            total_size = sum(f.stat().st_size for f in csv_files) / (1024 * 1024)
            print(f"  💾 总大小: {total_size:.2f} MB")

            print(f"\n  📄 示例CSV文件:")
            for f in sorted(csv_files)[:3]:
                try:
                    df = pd.read_csv(f, encoding='utf-8-sig', nrows=3)
                    print(f"     {f.name}: {len(df.columns)}列 x {pd.read_csv(f, encoding='utf-8-sig').shape[0]}行")
                    print(f"        列名: {list(df.columns)[:8]}...")
                except Exception as e:
                    print(f"     {f.name}: 读取失败 - {e}")

            # 显示第一个文件的详细内容
            sample_file = csv_files[0]
            print(f"\n  📝 文件内容示例 ({sample_file.name}):")
            df_sample = pd.read_csv(sample_file, encoding='utf-8-sig')
            print(df_sample.head())
    else:
        print(f"  ❌ CSV目录不存在: {CSV_DIR}")

    # 2. 检查Qlib数据
    print("\n【2/2】检查Qlib数据:")
    if not DATA_DIR.exists():
        print(f"  ❌ Qlib数据目录不存在: {DATA_DIR}")
        print("\n提示：请先运行完整测试或转换测试")
        return

    required_dirs = ['calendars', 'features', 'instruments']
    for dir_name in required_dirs:
        dir_path = DATA_DIR / dir_name
        if dir_path.exists():
            print(f"  ✅ {dir_name}/ 目录存在")
        else:
            print(f"  ❌ {dir_name}/ 目录缺失")

    # 检查交易日历
    calendar_path = DATA_DIR / 'calendars' / 'day.txt'
    if calendar_path.exists():
        with open(calendar_path, 'r') as f:
            dates = f.readlines()
        print(f"\n  📅 交易日历: {len(dates)} 个交易日")
        if dates:
            print(f"     起始日期: {dates[0].strip()}")
            print(f"     结束日期: {dates[-1].strip()}")

    # 检查bin文件
    features_dir = DATA_DIR / 'features'
    if features_dir.exists():
        bin_files = list(features_dir.glob('*.bin'))
        print(f"\n  📊 特征文件: {len(bin_files)} 个 .bin 文件")

        if bin_files:
            print("\n  生成的特征文件列表:")
            for f in sorted(bin_files):
                data = np.fromfile(str(f), dtype='<f')
                print(f"  📄 {f.name}: {len(data)//2} 条记录")

    # 检查instruments
    instruments_path = DATA_DIR / 'instruments' / 'all.txt'
    if instruments_path.exists():
        with open(instruments_path, 'r') as f:
            lines = f.readlines()
        print(f"\n  🏢 股票池: {len(lines)} 只股票")

    print("\n✅ 验证完成！")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Tushare-Qlib转换测试工具（支持两阶段）')
    parser.add_argument('--mode', type=str, default='single',
                       choices=['single', 'multiple', 'download_only',
                               'convert_only', 'verify'],
                       help='测试模式:\n'
                            'single      - 单只股票完整流程\n'
                            'multiple    - 多只股票(前10只)\n'
                            'download_only - 仅下载CSV\n'
                            'convert_only - 仅转Qlib\n'
                            'verify      - 验证数据')

    args = parser.parse_args()

    if args.mode == 'single':
        test_single_stock_full()
    elif args.mode == 'multiple':
        test_multiple_stocks()
    elif args.mode == 'download_only':
        test_download_only()
    elif args.mode == 'convert_only':
        test_convert_only()
    elif args.mode == 'verify':
        verify_qlib_data()
