#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
诊断脚本：检查为什么ROE等财务指标为空
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd


def diagnose_fina_indicator():
    """诊断财务指标获取问题"""

    print("\n" + "="*80)
    print("🔍 诊断：ROE等财务指标为空的原因")
    print("="*80)

    # 获取Token
    token = os.environ.get('TUSHARE_API_KEY', '').strip()
    if not token:
        token = os.environ.get('TUSHARE_TOKEN', '').strip()

    if not token:
        print("❌ 未找到Token")
        return

    try:
        import tushare as ts

        pro = ts.pro_api(token)
        ts_code = '000001.SZ'
        start_date = '20230101'
        end_date = '20231231'

        # 测试1: 直接调用fina_indicator接口
        print(f"\n【测试1】直接调用 fina_indicator 接口")
        print("-" * 60)

        try:
            df_fina = pro.fina_indicator(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,end_date,roe,roa,roa2_yearly,profit_to_gr,q_profit_yoy,q_eps,assets_turn'
            )

            print(f"✅ 接口调用成功")
            print(f"   返回类型: {type(df_fina)}")
            print(f"   数据形状: {df_fina.shape if hasattr(df_fina, 'shape') else 'N/A'}")

            if hasattr(df_fina, 'empty') and not df_fina.empty:
                print(f"\n   📊 返回数据预览:")
                print(df_fina.head(10))
                print(f"\n   列名: {list(df_fina.columns)}")
                print(f"   日期范围: {df_fina['end_date'].min()} ~ {df_fina['end_date'].max()}")
            else:
                print(f"   ⚠️  返回数据为空！")

        except Exception as e:
            print(f"❌ 接口调用失败: {e}")
            import traceback
            traceback.print_exc()

        # 测试2: 检查stk_factor_pro的日期格式
        print(f"\n\n【测试2】检查 stk_factor_pro 的日期格式")
        print("-" * 60)

        try:
            df_factor = pro.stk_factor_pro(
                ts_code=ts_code,
                start_date=start_date,
                end_date=start_date,  # 只取1天
                fields='ts_code,trade_date,close_qfq'
            )

            if not df_factor.empty:
                print(f"✅ stk_factor_pro 数据:")
                print(df_factor.head())
                print(f"\n   trade_date 示例: {df_factor['trade_date'].iloc[0]}")
                print(f"   trade_date 类型: {type(df_factor['trade_date'].iloc[0])}")

        except Exception as e:
            print(f"❌ 失败: {e}")

        # 测试3: 分析日期匹配问题
        print(f"\n\n【测试3】分析日期匹配问题（这是核心问题！）")
        print("-" * 60)

        print("""
⚠️  发现潜在问题：

1. **stk_factor_pro** 使用字段: trade_date (交易日，如 20230106)
2. **fina_indicator** 使用字段: end_date (报告期，如 20230930)

这两个字段的含义不同：
- trade_date: 每个交易日都有数据（日线）
- end_date: 只有季度末有数据（季报：3/6/9/12月最后一天）

当使用 join 合并时：
- 如果以 trade_date 为索引 → 季度数据无法匹配到每日数据
- 如果以 end_date 为索引 → 需要特殊处理

💡 解决方案：
- 方案A: 将财务指标按 end_date 设置索引后 join，会自动填充到对应日期
- 方案B: 使用 merge 而不是 join
- 方案C: 将财务指标 forward fill 到每个交易日
""")

        # 测试4: 模拟正确的合并方式
        print(f"\n【测试4】模拟正确的合并方式")
        print("-" * 60)

        try:
            # 获取技术因子数据（少量）
            df_factor = pro.stk_factor_pro(
                ts_code=ts_code,
                start_date='20230901',
                end_date='20231031',
                fields='ts_code,trade_date,close_qfq'
            )
            df_factor['datetime'] = pd.to_datetime(df_factor['trade_date'])
            df_factor.set_index('datetime', inplace=True)

            # 获取财务指标
            df_fina = pro.fina_indicator(
                ts_code=ts_code,
                start_date='20230101',
                end_date='20231231',
                fields='ts_code,end_date,roe,roa'
            )

            if not df_fina.empty and not df_factor.empty:
                df_fina['datetime'] = pd.to_datetime(df_fina['end_date'])
                df_fina.set_index('datetime', inplace=True)

                # 方法1: 直接join（当前代码的方式）
                print("\n方法1: 直接 join（当前代码）")
                merged1 = df_factor.join(df_fina[['roe', 'roa']], how='left')
                print(f"   合并后形状: {merged1.shape}")
                print(f"   ROE非空数量: {merged1['roe'].notna().sum()} / {len(merged1)}")
                print(merged1.head())

                # 方法2: 先排序再ffill（推荐）
                print("\n方法2: join + ffill（推荐）")
                merged2 = df_factor.join(df_fina[['roe', 'roa']], how='left')
                merged2 = merged2.sort_index()
                merged2[['roe', 'roa']] = merged2[['roe', 'roa']].ffill()
                print(f"   合并后形状: {merged2.shape}")
                print(f"   ROE非空数量: {merged2['roe'].notna().sum()} / {len(merged2)}")
                print(merged2.head())

        except Exception as e:
            print(f"❌ 测试4失败: {e}")

    except ImportError as e:
        print(f"❌ 导入tushare失败: {e}")


if __name__ == '__main__':
    diagnose_fina_indicator()
