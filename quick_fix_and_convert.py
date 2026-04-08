#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
快速修复并转换脚本
1. 重新下载000001.SZ（确保ROE填充正确）
2. 使用qlib官方dump_bin.py转换
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
import tushare as ts


def fix_roe_in_csv(ts_token: str, csv_path: str, output_path: str = None):
    """
    修复CSV中的ROE等财务指标
    使用merge as of方式正确填充
    """
    print("\n" + "="*80)
    print("🔧 修复CSV中的ROE等财务指标")
    print("="*80)

    # 1. 读取已有CSV
    df = pd.read_csv(csv_path)
    print(f"\n✅ 读取CSV成功: {len(df)} 条记录")
    print(f"   列名: {list(df.columns)}")

    # 2. 获取财务指标数据
    print(f"\n📊 从Tushare获取财务指标...")
    pro = ts.pro_api(ts_token)

    ts_code = df['instrument'].iloc[0]
    start_date = df['datetime'].min().replace('-', '')
    end_date = df['datetime'].max().replace('-', '')

    print(f"   股票: {ts_code}")
    print(f"   时间: {start_date} ~ {end_date}")

    fina_df = pro.fina_indicator(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields='ts_code,end_date,roe,roa,roa2_yearly,profit_to_gr,q_profit_yoy,q_eps,assets_turn'
    )

    if fina_df is None or len(fina_df) == 0:
        print("❌ 无法获取财务指标数据！")
        return False

    print(f"   ✅ 获取到 {len(fina_df)} 条财务指标")
    print(fina_df)

    # 3. 准备merge
    df['datetime'] = pd.to_datetime(df['datetime'])
    fina_df['datetime'] = pd.to_datetime(fina_df['end_date'])

    # 按日期排序
    df = df.sort_values('datetime')
    fina_df = fina_df.sort_values('datetime')

    print(f"\n🔄 使用merge as of填充财务指标...")

    # 使用merge_asof：找到<=当前日期的最近财务报告
    merged = pd.merge_asof(
        df,
        fina_df[['datetime', 'roe', 'roa', 'roa2_yearly', 'profit_to_gr',
                  'q_profit_yoy', 'q_eps', 'assets_turn']],
        on='datetime',
        direction='backward'  # 使用<=当前日期的最近值
    )

    # 移除可能存在的旧列（带后缀的）
    for col in ['roe_x', 'roa_x', 'roa2_yearly_x', 'profit_to_gr_x',
                'q_profit_yoy_x', 'q_eps_x', 'assets_turn_x']:
        if col in merged.columns:
            merged.drop(col, axis=1, inplace=True)

    # 重命名新列（移除_y后缀）
    rename_map = {
        'roe_y': 'roe',
        'roa_y': 'roa',
        'roa2_yearly_y': 'roa2_yearly',
        'profit_to_gr_y': 'profit_to_gr',
        'q_profit_yoy_y': 'q_profit_yoy',
        'q_eps_y': 'q_eps',
        'assets_turn_y': 'assets_turn'
    }
    merged.rename(columns=rename_map, inplace=True)

    # 检查结果
    print(f"\n✅ 修复完成！")
    fina_cols = ['roe', 'roa', 'roa2_yearly', 'profit_to_gr',
                 'q_profit_yoy', 'q_eps', 'assets_turn']
    for col in fina_cols:
        if col in merged.columns:
            non_null = merged[col].notna().sum()
            print(f"   {col:20s}: {non_null:4d}/{len(merged):4d} ({non_null/len(merged)*100:.1f}%) 非空")

    # 显示前5行的ROE
    print(f"\n📊 前5行数据（ROE列）:")
    print(merged[['datetime', 'close_qfq', 'roe']].head())

    # 保存
    if output_path is None:
        output_path = csv_path.replace('.csv', '_fixed.csv')

    merged.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n💾 已保存到: {output_path}")

    return output_path


def convert_with_qlib_dumpbin(csv_dir: str, qlib_dir: str):
    """
    使用qlib官方的dump_bin.py转换
    """
    print("\n" + "="*80)
    print("📦 使用QLIB官方dump_bin.py转换")
    print("="*80)

    # 添加qlib-main/scripts到路径
    qlib_main = Path(r"D:\quant_project\qlib-main")
    scripts_dir = qlib_main / "scripts"
    sys.path.insert(0, str(scripts_dir))

    try:
        from dump_bin import DumpDataAll
    except Exception as e:
        print(f"❌ 无法导入dump_bin: {e}")
        print(f"   请确认qlib-main在: {qlib_main}")
        return False

    # 检查CSV文件
    csv_path = Path(csv_dir)
    csv_files = list(csv_path.glob("*_fixed.csv"))

    if not csv_files:
        csv_files = list(csv_path.glob("*.csv"))

    if not csv_files:
        print(f"❌ 没有找到CSV文件在: {csv_dir}")
        return False

    print(f"\n✅ 找到 {len(csv_files)} 个CSV文件")

    # 准备字段列表
    standard_fields = [
        'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq',
        'vol', 'amount', 'turnover_rate', 'volume_ratio',
        'pe', 'pb', 'ps', 'dv_ratio', 'ps_ttm', 'pe_ttm', 'dv_ttm',
        'total_mv', 'kdj_qfq', 'kdj_d_qfq', 'kdj_k_qfq',
        'rsi_qfq_12', 'macd_qfq', 'macd_dea_qfq', 'macd_dif_qfq',
        'atr_qfq', 'mtmma_qfq', 'turnover_rate_f',
        'net_amount', 'buy_elg_amount', 'buy_lg_amount',
        'buy_md_amount', 'buy_sm_amount',
        'rzye', 'rqye',
        'roe', 'roa', 'roa2_yearly', 'profit_to_gr',
        'q_profit_yoy', 'q_eps', 'assets_turn'
    ]
    include_fields = ','.join(standard_fields)

    # 清理输出目录
    qlib_path = Path(qlib_dir)
    if qlib_path.exists():
        import shutil
        shutil.rmtree(qlib_path)
    qlib_path.mkdir(parents=True, exist_ok=True)

    print(f"\n📂 输出目录: {qlib_dir}")

    # 执行转换
    try:
        dumper = DumpDataAll(
            data_path=str(csv_path),
            qlib_dir=str(qlib_path),
            date_field_name='datetime',
            symbol_field_name='instrument',
            include_fields=include_fields,
            freq='day',
            max_workers=4
        )
        dumper.dump()
    except Exception as e:
        print(f"\n❌ 转换失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    # 验证结果
    print(f"\n✅ 转换成功！")
    print(f"\n📁 生成的文件:")

    calendars_dir = qlib_path / "calendars"
    if calendars_dir.exists():
        print(f"   ✅ calendars/")
        for f in calendars_dir.glob("*.txt"):
            print(f"      - {f.name}")

    instruments_dir = qlib_path / "instruments"
    if instruments_dir.exists():
        print(f"   ✅ instruments/")
        for f in instruments_dir.glob("*.txt"):
            print(f"      - {f.name}")

    features_dir = qlib_path / "features"
    if features_dir.exists():
        stock_dirs = [d for d in features_dir.iterdir() if d.is_dir()]
        print(f"   ✅ features/ ({len(stock_dirs)} 只股票)")
        if stock_dirs:
            first_stock = stock_dirs[0]
            bin_files = list(first_stock.glob("*.bin"))
            print(f"      - {first_stock.name}/ 包含 {len(bin_files)} 个特征:")
            for bf in sorted(bin_files)[:15]:
                size = bf.stat().st_size / 1024
                print(f"          {bf.name:25s} ({size:.1f} KB)")

    return True


def main():
    # 配置
    TUSHARE_TOKEN = os.environ.get('TUSHARE_API_KEY', '')
    CSV_FILE = r"D:\qlib_data\qlib_data_test\csv_data\000001.csv"
    CSV_DIR = r"D:\qlib_data\qlib_data_test\csv_data"
    QLIB_DIR = r"D:\qlib_data\qlib_data_test"

    if not TUSHARE_TOKEN:
        print("❌ 请设置环境变量 TUSHARE_API_KEY")
        return

    # 步骤1：修复ROE
    fixed_csv = fix_roe_in_csv(TUSHARE_TOKEN, CSV_FILE)
    if not fixed_csv:
        print("❌ 修复ROE失败！")
        return

    # 步骤2：用修复后的CSV替换原文件
    import shutil
    backup_csv = CSV_FILE.replace('.csv', '_original.csv')
    shutil.copy2(CSV_FILE, backup_csv)
    shutil.copy2(fixed_csv, CSV_FILE)
    print(f"\n✅ 已替换原文件: {CSV_FILE}")
    print(f"   备份文件: {backup_csv}")

    # 步骤3：使用qlib官方dump_bin转换
    success = convert_with_qlib_dumpbin(CSV_DIR, QLIB_DIR)

    if success:
        print("\n" + "="*80)
        print("🎉 全部完成！")
        print("="*80)
        print(f"\n现在可以使用Qlib读取数据了:")
        print(f"  import qlib")
        print(f"  from qlib.data import D")
        print(f"  qlib.init(provider_uri=r'{QLIB_DIR}')")
        print(f"  data = D.features(['000001.SZ'], ['$close_qfq', '$roe'])")
        print(f"  print(data)")
    else:
        print("\n❌ 转换失败！")


if __name__ == '__main__':
    main()
