#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
使用qlib官方的dump_bin.py脚本转换数据

这个脚本直接调用qlib-main/scripts/dump_bin.py的DumpDataAll类
来正确转换我们的CSV数据为Qlib格式

用法：
    python use_qlib_dump_bin.py
    python use_qlib_dump_bin.py --csv-dir D:\\data\\csv_data --qlib-dir D:\\data\\qlib_data

环境变量（可选，优先于下面默认）：
    QLIB_CSV_DIR  - CSV 目录
    QLIB_DATA_DIR - Qlib 数据输出根目录（与 tushare_to_qlib 的 --output_dir 一致时常用 D:\\qlib_data\\qlib_data）

默认与 tushare_to_qlib 一致：CSV 为 D:\\qlib_data\\csv_data，输出为 D:\\qlib_data\\qlib_data。
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# 确保可以找到qlib-main/scripts目录
QLIB_MAIN_DIR = Path(r"D:\quant_project\qlib-main")
SCRIPTS_DIR = QLIB_MAIN_DIR / "scripts"

sys.path.insert(0, str(SCRIPTS_DIR))

# 与 tushare_to_qlib 一致：在 CSV 中补充 open/high/low/close/volume 列
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
from tushare_to_qlib import add_qlib_standard_ohlcv_columns

STD_OHLCV_MAP = {
    "open": "open_qfq",
    "high": "high_qfq",
    "low": "low_qfq",
    "close": "close_qfq",
    "volume": "volume_ratio",
}


def force_copy_standard_ohlcv(df):
    """
    强制复制标准OHLCV列。
    例如 open = open_qfq（覆盖同名旧值，确保与源列一致）。
    """
    copied = {}
    for dst, src in STD_OHLCV_MAP.items():
        if src in df.columns:
            df[dst] = df[src]
            copied[dst] = int(df[dst].notna().sum())
    return copied


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
        include_fields: 要包含的字段列表（逗号分隔）；未指定时包含 open/high/low/close/volume
            （由 open_qfq 等与 volume_ratio 自动写入 CSV 后一并 dump）
        freq: 数据频率（day或1min）
    """
    print("\n" + "="*80)
    print("[INFO] 使用QLIB官方脚本转换数据")
    print("="*80)

    # 导入qlib官方的dump_bin模块
    try:
        from dump_bin import DumpDataAll
    except Exception as e:
        print(f"[ERR] 导入 dump_bin 失败: {e}")
        if isinstance(e, ModuleNotFoundError) and getattr(e, "name", None):
            print(f"      建议安装缺失依赖: pip install {e.name}")
        else:
            print("      请安装 qlib 相关依赖（至少 fire、loguru）")
        return False

    # 检查CSV目录
    csv_path = Path(csv_dir)
    if not csv_path.exists():
        print(f"❌ CSV目录不存在: {csv_dir}")
        print(
            "   可设置环境变量 QLIB_CSV_DIR，或使用: "
            "python use_qlib_dump_bin.py --csv-dir <目录>"
        )
        print("   下载 CSV 请用 tushare_to_qlib 阶段1，默认会写入 D:\\qlib_data\\csv_data 。")
        return False

    csv_files = list(csv_path.glob("*.csv"))
    print(f"\n[OK] 找到 {len(csv_files)} 个CSV文件")

    if not csv_files:
        print("[ERR] CSV目录中没有发现CSV文件！")
        return False

    import pandas as pd

    copied_file_count = 0
    copied_preview = None
    for one_csv in sorted(csv_files):
        try:
            df_pre = pd.read_csv(one_csv, encoding="utf-8-sig")
            # 兼容已有公共函数 + 本地强制覆盖复制，确保 open=open_qfq 等一定生效
            add_qlib_standard_ohlcv_columns(df_pre)
            copied = force_copy_standard_ohlcv(df_pre)
            if copied:
                copied_file_count += 1
                if copied_preview is None:
                    copied_preview = {"file": one_csv.name, "nonnull": copied}
            df_pre.to_csv(one_csv, index=False, encoding="utf-8-sig")
        except Exception as e:
            print(f"⚠️  为 {one_csv.name} 补充标准 OHLCV 列失败: {e}")

    print(f"[INFO] 已强制复制OHLCV列的CSV数量: {copied_file_count}/{len(csv_files)}")
    if copied_preview is not None:
        print(f"[INFO] 复制示例: {copied_preview['file']} -> {copied_preview['nonnull']}")

    # 查看第一个CSV文件的结构
    first_csv = csv_files[0]
    print(f"\n[INFO] 查看第一个CSV文件: {first_csv.name}")
    df_sample = pd.read_csv(first_csv, nrows=3)
    print("   列名:", list(df_sample.columns))
    print("   示例数据:")
    print(df_sample.to_string(index=False))

    # 创建DumpDataAll实例
    print(f"\n[INFO] 输出目录: {qlib_dir}")

    # 如果没有指定include_fields，使用我们的标准字段
    if not include_fields:
        standard_fields = [
            'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq',
            'open', 'high', 'low', 'close', 'volume',
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
        print(f"\n[OK] 使用标准字段列表（共{len(standard_fields)}个字段）")

    # 准备转换
    print(f"\n[INFO] 开始转换...")
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

        print(f"\n[OK] 转换成功！")

        # 验证结果
        qlib_path = Path(qlib_dir)
        calendars_dir = qlib_path / "calendars"
        features_dir = qlib_path / "features"
        instruments_dir = qlib_path / "instruments"

        print(f"\n[INFO] 生成的Qlib数据目录:")

        if calendars_dir.exists():
            cal_files = list(calendars_dir.glob("*.txt"))
            print(f"   [OK] calendars/  - 包含 {len(cal_files)} 个日历文件")
            for f in cal_files:
                print(f"      - {f.name}")

        if instruments_dir.exists():
            inst_files = list(instruments_dir.glob("*.txt"))
            print(f"   [OK] instruments/ - 包含 {len(inst_files)} 个股票池文件")
            for f in inst_files:
                print(f"      - {f.name}")

        if features_dir.exists():
            stock_dirs = [d for d in features_dir.iterdir() if d.is_dir()]
            print(f"   [OK] features/    - 包含 {len(stock_dirs)} 只股票的数据")

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
        print(f"\n[ERR] 转换失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def run(
    csv_dir: str = None,
    qlib_dir: str = None,
):
    """
    直接调用入口：CSV → Qlib 转换（含 open/high/low/close/volume 别名生成）。

    路径优先级：参数 > 环境变量 QLIB_CSV_DIR / QLIB_DATA_DIR > 项目默认路径。

    用法（Python 代码中）：
        from use_qlib_dump_bin import run
        run()                                          # 使用默认路径
        run(csv_dir=r"D:\\my\\csv", qlib_dir=r"D:\\my\\qlib")  # 自定义路径

    用法（命令行）：
        python use_qlib_dump_bin.py
        python use_qlib_dump_bin.py --csv-dir D:\\my\\csv --qlib-dir D:\\my\\qlib
    """
    _default_csv = r"D:\qlib_data\csv_data"
    _default_qlib = r"D:\qlib_data\qlib_data"

    CSV_DIR  = csv_dir  or os.environ.get("QLIB_CSV_DIR")  or _default_csv
    QLIB_DIR = qlib_dir or os.environ.get("QLIB_DATA_DIR") or _default_qlib

    success = dump_csv_to_qlib_official(
        csv_dir=CSV_DIR,
        qlib_dir=QLIB_DIR,
        date_field="datetime",
        symbol_field="instrument",
        freq="day",
    )

    if success:
        print("\n" + "=" * 80)
        print("[OK] 完成！现在可以使用 Qlib 读取数据了")
        print("=" * 80)
        print(f"\n示例：")
        print(f"  import qlib; from qlib.data import D")
        print(f"  qlib.init(provider_uri=r'{QLIB_DIR}')")
        print(f"  D.features(['000001.SZ'], ['$open','$close','$close_qfq','$roe'])")
    else:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="CSV → Qlib（官方 dump_bin，含 OHLCV 别名）")
    parser.add_argument("--csv-dir",  type=str, default=None, help="CSV 目录")
    parser.add_argument("--qlib-dir", type=str, default=None, help="Qlib 输出根目录")
    args = parser.parse_args()
    run(csv_dir=args.csv_dir, qlib_dir=args.qlib_dir)


if __name__ == "__main__":
    main()
