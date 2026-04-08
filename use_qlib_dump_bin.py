#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
使用qlib官方的dump_bin.py脚本转换数据

这个脚本直接调用qlib-main/scripts/dump_bin.py的DumpDataAll类
来正确转换我们的CSV数据为Qlib格式

用法：
    python use_qlib_dump_bin.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# 确保可以找到qlib-main/scripts目录
QLIB_MAIN_DIR = Path(r"D:\quant_project\qlib-main")
SCRIPTS_DIR = QLIB_MAIN_DIR / "scripts"

sys.path.insert(0, str(SCRIPTS_DIR))


def dump_csv_to_qlib_official(
    csv_dir: str,
    qlib_dir: str,
    date_field: str = "datetime",
    symbol_field: str = "instrument",
    include_fields: str = "",
    freq: str = "day"
):
    """
    使用qlib官方的dump_bin.py转换CSV数据

    参数：
        csv_dir: CSV文件所在目录
        qlib_dir: Qlib数据输出目录
        date_field: CSV中日期字段名
        symbol_field: CSV中股票代码字段名
        include_fields: 要包含的字段列表（逗号分隔）
        freq: 数据频率（day或1min）
    """
    print("\n" + "="*80)
    print("📦 使用QLIB官方脚本转换数据")
    print("="*80)

    # 导入qlib官方的dump_bin模块
    from dump_bin import DumpDataAll

    # 检查CSV目录
    csv_path = Path(csv_dir)
    if not csv_path.exists():
        print(f"❌ CSV目录不存在: {csv_dir}")
        return False

    csv_files = list(csv_path.glob("*.csv"))
    print(f"\n✅ 找到 {len(csv_files)} 个CSV文件")

    if not csv_files:
        print("❌ CSV目录中没有发现CSV文件！")
        return False

    # 查看第一个CSV文件的结构
    first_csv = csv_files[0]
    print(f"\n📊 查看第一个CSV文件: {first_csv.name}")
    import pandas as pd
    df_sample = pd.read_csv(first_csv, nrows=3)
    print("   列名:", list(df_sample.columns))
    print("   示例数据:")
    print(df_sample.to_string(index=False))

    # 创建DumpDataAll实例
    print(f"\n📂 输出目录: {qlib_dir}")

    # 如果没有指定include_fields，使用我们的标准字段
    if not include_fields:
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
        print(f"\n✅ 使用标准字段列表（共{len(standard_fields)}个字段）")

    # 准备转换
    print(f"\n🔄 开始转换...")
    print(f"   日期字段: {date_field}")
    print(f"   股票字段: {symbol_field}")
    print(f"   数据频率: {freq}")

    try:
        dumper = DumpDataAll(
            data_path=str(csv_path),
            qlib_dir=qlib_dir,
            date_field_name=date_field,
            symbol_field_name=symbol_field,
            include_fields=include_fields,
            freq=freq,
            max_workers=4  # 使用较少的worker防止内存问题
        )

        # 执行转换
        dumper.dump()

        print(f"\n✅ 转换成功！")

        # 验证结果
        qlib_path = Path(qlib_dir)
        calendars_dir = qlib_path / "calendars"
        features_dir = qlib_path / "features"
        instruments_dir = qlib_path / "instruments"

        print(f"\n📁 生成的Qlib数据目录:")

        if calendars_dir.exists():
            cal_files = list(calendars_dir.glob("*.txt"))
            print(f"   ✅ calendars/  - 包含 {len(cal_files)} 个日历文件")
            for f in cal_files:
                print(f"      - {f.name}")

        if instruments_dir.exists():
            inst_files = list(instruments_dir.glob("*.txt"))
            print(f"   ✅ instruments/ - 包含 {len(inst_files)} 个股票池文件")
            for f in inst_files:
                print(f"      - {f.name}")

        if features_dir.exists():
            stock_dirs = [d for d in features_dir.iterdir() if d.is_dir()]
            print(f"   ✅ features/    - 包含 {len(stock_dirs)} 只股票的数据")

            # 查看第一只股票的bin文件
            if stock_dirs:
                first_stock = stock_dirs[0]
                bin_files = list(first_stock.glob("*.bin"))
                print(f"      - {first_stock.name}/ 包含 {len(bin_files)} 个特征文件")
                if bin_files:
                    for bf in sorted(bin_files)[:10]:  # 只显示前10个
                        size_kb = bf.stat().st_size / 1024
                        print(f"          {bf.name} ({size_kb:.1f} KB)")
                    if len(bin_files) > 10:
                        print(f"          ... 还有 {len(bin_files) - 10} 个文件")

        return True

    except Exception as e:
        print(f"\n❌ 转换失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    # 配置路径 - 使用和之前一致的路径
    CSV_DIR = r"D:\qlib_data\qlib_data_test\csv_data"
    QLIB_DIR = r"D:\qlib_data\qlib_data_test"

    # 执行转换
    success = dump_csv_to_qlib_official(
        csv_dir=CSV_DIR,
        qlib_dir=QLIB_DIR,
        date_field="datetime",
        symbol_field="instrument",
        freq="day"
    )

    if success:
        print("\n" + "="*80)
        print("🎉 完成！现在可以使用Qlib读取数据了")
        print("="*80)
        print(f"\n使用方式:")
        print(f"  import qlib")
        print(f"  from qlib.data import D")
        print(f"  qlib.init(provider_uri=r'{QLIB_DIR}')")
        print(f"  data = D.features(['000001.SZ'], ['$close_qfq', '$roe'])")
        print(f"  print(data)")
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
