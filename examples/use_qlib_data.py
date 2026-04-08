#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
在Qlib中使用转换后的数据 - 示例代码

本文件展示了如何加载和使用通过 tushare_to_qlib.py 生成的数据
"""

from __future__ import annotations

import qlib
from qlib.data import D
from qlib.config import REG_CN


def init_qlib_with_custom_data(data_dir: str):
    """
    使用自定义数据初始化Qlib

    参数：
        data_dir: 通过tushare_to_qlib.py生成的数据目录
                例如: "D:/qlib_data/qlib_data"
    """
    # 初始化Qlib，指定数据目录
    qlib.init(
        provider_uri=data_dir,
        region=REG_CN,
    )
    print(f"✅ Qlib已初始化，数据源: {data_dir}")


def example_basic_usage():
    """基础用法示例：读取数据和基本统计"""

    # 1. 初始化Qlib（请修改为你的实际路径）
    DATA_DIR = r"D:\qlib_data\qlib_data"
    init_qlib_with_custom_data(DATA_DIR)

    # 2. 获取交易日历
    print("\n📅 获取交易日历:")
    calendar = D.calendar()
    print(f"   总交易日数: {len(calendar)}")
    print(f"   起始日期: {calendar[0]}")
    print(f"   结束日期: {calendar[-1]}")
    print(f"   前5个交易日: {calendar[:5]}")

    # 3. 获取股票列表
    print("\n🏢 获取股票列表:")
    instruments = D.instruments('all')
    stock_list = D.list_instruments(instruments=instruments, as_list=True)
    print(f"   总股票数: {len(stock_list)}")
    print(f"   前10只股票: {stock_list[:10]}")

    # 4. 读取单只股票的特征数据
    print("\n📊 读取单只股票特征 (000001.SZ):")
    data = D.features(
        instruments=['000001.SZ'],
        start_time='2023-06-01',
        end_time='2023-06-30',
        fields=['$open_qfq', '$close_qfq', '$high_qfq', '$low_qfq', '$vol']
    )
    print(data.head(10))
    print(f"\n   数据形状: {data.shape}")
    print(f"   字段列表: {data.columns.tolist()}")

    # 5. 读取多只股票的数据
    print("\n📈 读取多只股票数据:")
    multi_data = D.features(
        instruments=['000001.SZ', '600519.SH', '000858.SZ'],
        start_time='2023-01-01',
        end_time='2023-01-31',
        fields=['$close_qfq', '$vol', '$amount', '$turnover_rate']
    )
    print(multi_data.head(15))
    print(f"\n   数据形状: {multi_data.shape}")


def example_technical_indicators():
    """技术指标使用示例"""

    DATA_DIR = r"D:\qlib_data\qlib_data"
    init_qlib_with_custom_data(DATA_DIR)

    print("\n" + "="*80)
    print("📊 技术指标数据示例")
    print("="*80)

    # 读取技术指标
    tech_fields = [
        '$kdj_k_qfq', '$kdj_d_qfq', '$kdj_qfq',
        '$rsi_qfq_12',
        '$macd_dif_qfq', '$macd_dea_qfq', '$macd_qfq',
        '$atr_qfq'
    ]

    data = D.features(
        instruments=['000001.SZ'],
        start_time='2023-01-01',
        end_time='2023-03-31',
        fields=tech_fields
    )

    print("\nKDJ/RSI/MACD/ATR 指标数据:")
    print(data.head(20))
    print(f"\n数据统计:")
    print(data.describe())


def example_fundamental_data():
    """财务指标和资金流向使用示例"""

    DATA_DIR = r"D:\qlib_data\qlib_data"
    init_qlib_with_custom_data(DATA_DIR)

    print("\n" + "="*80)
    print("💰 财务指标与资金流向示例")
    print("="*80)

    # 1. 财务指标
    print("\n1️⃣ 财务指标 (ROE, ROA等):")
    fina_fields = ['$roe', '$roa', '$roa2_yearly', '$profit_to_gr', '$assets_turn']

    fina_data = D.features(
        instruments=['000001.SZ', '600519.SH'],
        start_time='2022-12-01',
        end_time='2023-12-31',
        fields=fina_fields
    )

    print(fina_data.dropna(how='all').head(20))

    # 2. 资金流向
    print("\n2️⃣ 资金流向数据:")
    money_fields = ['$net_amount', '$buy_elg_amount', '$buy_lg_amount',
                    '$buy_md_amount', '$buy_sm_amount']

    money_data = D.features(
        instruments=['000001.SZ'],
        start_time='2023-09-11',  # moneyflow_dc从该日期开始有数据
        end_date='2023-12-31',
        fields=money_fields
    )

    if not money_data.empty:
        print(money_data.head(20))

    # 3. 融资融券
    print("\n3️⃣ 融资融券数据:")
    margin_fields = ['$rzye', '$rqye']

    margin_data = D.features(
        instruments=['000001.SZ'],
        start_time='2023-01-01',
        end_time='2023-12-31',
        fields=margin_fields
    )

    if not margin_data.empty:
        print(margin_data.dropna(how='all').head(20))


def example_valuation_metrics():
    """估值指标使用示例"""

    DATA_DIR = r"D:\qlib_data\qlib_data"
    init_qlib_with_custom_data(DATA_DIR)

    print("\n" + "="*80)
    print("💎 估值指标示例 (PE/PB/PS等)")
    print("="*80)

    valuation_fields = [
        '$pe', '$pe_ttm', '$pb', '$ps', '$ps_ttm',
        '$dv_ratio', '$dv_ttm', '$total_mv'
    ]

    data = D.features(
        instruments=['000001.SZ', '600519.SH', '000858.SZ'],
        start_time='2023-01-01',
        end_time='2023-06-30',
        fields=valuation_fields
    )

    print("\n估值指标数据:")
    print(data.dropna(how='all').head(20))

    print("\n估值指标统计:")
    print(data.describe())


def example_build_alpha_factors():
    """
    构建Alpha因子示例（使用Qlib表达式引擎）

    Qlib支持丰富的表达式操作符，可以基于原始字段构建复杂因子
    """

    DATA_DIR = r"D:\qlib_data\qlib_data"
    init_qlib_with_custom_data(DATA_DIR)

    print("\n" + "="*80)
    print("🔬 Alpha因子构建示例")
    print("="*80)

    # 示例1：动量因子（过去20日收益率）
    print("\n示例1: 动量因子 (20日收益率)")
    momentum = D.features(
        instruments=['000001.SZ'],
        start_time='2023-01-01',
        end_time='2023-12-31',
        fields=["Ref($close_qfq, 20) / $close_qfq - 1"]
    )
    print(momentum.head(10))

    # 示例2：波动率因子（20日收益率标准差）
    print("\n示例2: 波动率因子 (20日标准差)")
    volatility = D.features(
        instruments=['000001.SZ'],
        start_time='2023-02-01',  # 需要足够的历史数据计算标准差
        end_time='2023-12-31',
        fields=["Stddev($close_qfq / Ref($close_qfq, 1) - 1, 20)"]
    )
    print(volatility.head(10))

    # 示例3：资金流入强度因子
    print("\n示例3: 资金流入强度 (主力净流入/成交额)")
    capital_strength = D.features(
        instruments=['000001.SZ'],
        start_time='2023-09-11',
        end_time='2023-12-31',
        fields=["$net_amount / $amount * 100"]  # 单位：%
    )
    if not capital_strength.empty:
        print(capital_strength.head(10))

    # 示例4：估值偏离度因子
    print("\n示例4: PE历史分位数")
    pe_percentile = D.features(
        instruments=['000001.SZ'],
        start_time='2023-06-01',
        end_time='2023-12-31',
        fields=["Rank($pe, 120) / 120"]  # 过去120日PE排名百分位
    )
    print(pe_percentile.head(10))

    print("\n💡 提示: 更多表达式操作符请参考Qlib官方文档")


def example_full_workflow():
    """
    完整工作流示例：从数据读取到简单回测
    """

    DATA_DIR = r"D:\qlib_data\qlib_data"
    init_qlib_with_custom_data(DATA_DIR)

    print("\n" + "="*80)
    print("🔄 完整工作流示例")
    print("="*80)

    # 步骤1: 定义数据集参数
    print("\n步骤1: 准备数据...")
    train_start = '2020-01-01'
    train_end = '2022-12-31'
    test_start = '2023-01-01'
    test_end = '2023-12-31'

    # 步骤2: 定义特征和标签
    print("步骤2: 定义特征和标签...")

    # 特征字段（可自定义）
    features = [
        '$open_qfq', '$high_qfq', '$low_qfq', '$close_qfq',
        '$vol', '$amount', '$turnover_rate',
        '$pe', '$pb', '$total_mv',
        '$kdj_k_qfq', '$rsi_qfq_12', '$macd_dif_qfq',
        '$roe', '$roa',
        '$net_amount'  # 资金流向
    ]

    # 标签：未来5日收益
    label = "Ref($close_qfq, -5) / $close_qfq - 1"

    # 步骤3: 加载训练数据
    print(f"步骤3: 加载训练数据 ({train_start} ~ {train_end})...")
    train_data = D.features(
        instruments=D.instruments('all'),
        start_time=train_start,
        end_time=train_end,
        fields=features + [label]
    )
    print(f"   训练数据形状: {train_data.shape}")

    # 步骤4: 加载测试数据
    print(f"步骤4: 加载测试数据 ({test_start} ~ {test_end})...")
    test_data = D.features(
        instruments=D.instruments('all'),
        start_time=test_start,
        end_time=test_end,
        fields=features + [label]
    )
    print(f"   测试数据形状: {test_data.shape}")

    # 步骤5: 数据预览
    print("\n步骤5: 数据预览:")
    print(train_data.head())
    print(f"\n缺失值统计:")
    print(train_data.isnull().sum())

    print("\n✅ 工作流完成！数据已准备就绪，可用于模型训练。")
    print("\n💡 下一步:")
    print("   - 使用Qlib的Dataset类创建标准化数据集")
    print("   - 选择模型（如LightGBM、GRU、LSTM）进行训练")
    print("   - 使用回测框架评估策略表现")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Qlib数据使用示例')
    parser.add_argument('--example', type=str, default='basic',
                       choices=['basic', 'technical', 'fundamental',
                               'valuation', 'alpha', 'workflow'],
                       help='要运行的示例')

    args = parser.parse_args()

    examples = {
        'basic': example_basic_usage,
        'technical': example_technical_indicators,
        'fundamental': example_fundamental_data,
        'valuation': example_valuation_metrics,
        'alpha': example_build_alpha_factors,
        'workflow': example_full_workflow
    }

    print("="*80)
    print("🚀 Qlib数据使用示例")
    print("="*80)
    print(f"\n运行示例: {args.example}\n")

    try:
        examples[args.example]()
    except Exception as e:
        print(f"\n❌ 运行出错: {e}")
        print("\n可能的原因:")
        print("  1. 数据目录不存在或格式不正确")
        print("  2. 未安装qlib: pip install qlib")
        print("  3. 请先运行 tushare_to_qlib.py 生成数据")
