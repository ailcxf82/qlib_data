#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调试脚本：测试Tushare API调用

用于诊断数据获取问题
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def test_api_directly():
    """直接测试Tushare API调用，不经过频率限制器"""

    print("\n" + "="*80)
    print("🔍 Tushare API 调试工具")
    print("="*80)

    # 获取Token
    token = os.environ.get('TUSHARE_API_KEY', '').strip()
    if not token:
        token = os.environ.get('TUSHARE_TOKEN', '').strip()

    if not token:
        print("❌ 错误：未找到环境变量 TUSHARE_API_KEY 或 TUSHARE_TOKEN")
        return

    print(f"✅ Token已获取（长度: {len(token)}）")

    try:
        import tushare as ts
        import pandas as pd

        # 初始化API
        print("\n📡 初始化Tushare Pro API...")
        pro = ts.pro_api(token)
        print("✅ API初始化成功\n")

        # 测试1：获取股票列表
        print("="*60)
        print("测试1: stock_basic (股票列表)")
        print("="*60)
        try:
            df_stock = pro.stock_basic(exchange='', list_status='L',
                                       fields='ts_code,symbol,name,list_date')
            print(f"✅ 成功！返回类型: {type(df_stock)}")
            print(f"   数据形状: {df_stock.shape}")
            print(f"   列名: {list(df_stock.columns)}")
            print(f"   前3行:\n{df_stock.head(3)}\n")
        except Exception as e:
            print(f"❌ 失败: {e}\n")

        # 测试2：stk_factor_pro (技术因子) - 这是出问题的接口
        print("="*60)
        print("测试2: stk_factor_pro (技术因子) - 000001.SZ")
        print("="*60)

        test_fields = [
            'ts_code', 'trade_date', 'open_qfq', 'high_qfq', 'low_qfq',
            'close_qfq', 'vol', 'amount'
        ]

        try:
            df_factor = pro.stk_factor_pro(
                ts_code='000001.SZ',
                start_date='20230101',
                end_date='20230110',
                fields=','.join(test_fields)
            )

            print(f"✅ 成功！返回类型: {type(df_factor)}")
            print(f"   数据形状: {df_factor.shape if hasattr(df_factor, 'shape') else 'N/A'}")
            print(f"   是否为None: {df_factor is None}")

            if hasattr(df_factor, 'empty'):
                print(f"   是否为空: {df_factor.empty}")
                print(f"   长度: {len(df_factor)}")

            if hasattr(df_factor, 'columns'):
                print(f"   列名: {list(df_factor.columns)}")

            if len(df_factor) > 0:
                print(f"   前5行:\n{df_factor.head()}\n")
            else:
                print("   ⚠️  数据为空\n")

        except Exception as e:
            print(f"❌ 失败: {e}")
            import traceback
            print(f"\n详细错误:\n{traceback.format_exc()}\n")

        # 测试3：检查字段是否正确
        print("="*60)
        print("测试3: 检查stk_factor_pro支持的字段")
        print("="*60)
        try:
            # 不指定fields参数，获取默认字段
            df_default = pro.stk_factor_pro(
                ts_code='000001.SZ',
                start_date='20230106',
                end_date='20230106'  # 只取1天
            )

            print(f"✅ 默认字段查询成功！")
            print(f"   返回列数: {len(df_default.columns) if hasattr(df_default, 'columns') else 'N/A'}")
            print(f"   列名: {list(df_default.columns) if hasattr(df_default, 'columns') else 'N/A'}\n")

        except Exception as e:
            print(f"❌ 默认字段查询失败: {e}\n")

        print("="*80)
        print("🔚 调试完成")
        print("="*80)

    except ImportError as e:
        print(f"❌ 导入tushare失败: {e}")
        print("   请先安装: pip install tushare")


if __name__ == '__main__':
    test_api_directly()
