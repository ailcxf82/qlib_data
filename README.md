# Tushare数据转Qlib格式工具

## 📋 功能概述

本工具用于从Tushare Pro API获取多维度金融数据，并将其转换为微软Qlib量化框架所需的标准.bin格式。

### 数据源接口

1. **stk_factor_pro** - 技术面因子（OHLCV、技术指标等）
2. **moneyflow_dc** - 个股资金流向（主力/超大单/大单/中单/小单净流入）
3. **margin_detail** - 融资融券交易明细（融资余额、融券余额）
4. **fina_indicator** - 财务指标（ROE、ROA、净利润率等）

## 🚀 快速开始

### 1. 安装依赖

```bash
cd d:\quant_project\qlib_Data
pip install -r requirements.txt
```

### 2. 获取Tushare Token

- 访问 https://tushare.pro 注册账号
- 在个人中心获取API Token
- Token积分要求：建议5000+积分（部分接口需要）

### 3. 配置Token（3种方式，任选其一）

#### 方式一：环境变量（推荐✅）

```bash
# Windows PowerShell
$env:TUSHARE_API_KEY="your_token_here"
python tushare_to_qlib.py --start_date 20200101 --end_date 20231231

# Windows CMD
set TUSHARE_API_KEY=your_token_here
python tushare_to_qlib.py --start_date 20200101 --end_date 20231231

# Linux/Mac
export TUSHARE_API_KEY=your_token_here
python tushare_to_qlib.py --start_date 20200101 --end_date 20231231
```

#### 方式二：命令行参数

```bash
python tushare_to_qlib.py --token YOUR_TOKEN --start_date 20200101 --end_date 20231231
```

#### 方式三：配置文件

```bash
cp config/secrets.yaml.example config/secrets.yaml
# 编辑config/secrets.yaml，填入Token
```

**优先级顺序**: 命令行参数 > 环境变量 TUSHARE_API_KEY > 环境变量 TUSHARE_TOKEN > 配置文件

### 4. 运行数据转换

#### 标准模式（适合5000积分用户）

```bash
python tushare_to_qlib.py \
    --start_date 20200101 \
    --end_date 20231231 \
    --output_dir D:/qlib_data/qlib_data
```

#### 高速模式（需要8000+积分⚡）

```bash
python tushare_to_qlib.py \
    --start_date 20200101 \
    --end_date 20231231 \
    --high-speed
```

#### 测试模式（只处理10只股票）

```bash
python tushare_to_qlib.py \
    --start_date 20230101 \
    --end_date 20231231 \
    --max_stocks 10
```

#### 使用测试脚本（推荐新手使用）

```bash
# 设置环境变量后运行
$env:TUSHARE_API_KEY="your_token"

# 测试单只股票
python test_tushare_to_qlib.py --mode single

# 测试多只股票（前10只）
python test_tushare_to_qlib.py --mode multiple

# 验证生成的数据
python test_tushare_to_qlib.py --mode verify
```

## 📊 输出字段说明

### 基础行情字段 (stk_factor_pro)

| 字段名 | 说明 | 来源 |
|--------|------|------|
| open_qfq | 开盘价（前复权） | stk_factor_pro |
| high_qfq | 最高价（前复权） | stk_factor_pro |
| low_qfq | 最低价（前复权） | stk_factor_pro |
| close_qfq | 收盘价（前复权） | stk_factor_pro |
| vol | 成交量（手） | stk_factor_pro |
| amount | 成交额（千元） | stk_factor_pro |

### 技术指标字段 (stk_factor_pro)

| 字段名 | 说明 |
|--------|------|
| turnover_rate | 换手率（%） |
| volume_ratio | 量比 |
| pe | 市盈率 |
| pb | 市净率 |
| ps | 市销率 |
| dv_ratio | 股息率（%） |
| ps_ttm | 市销率（TTM） |
| pe_ttm | 市盈率（TTM） |
| dv_ttm | 股息率（TTM）（%） |
| total_mv | 总市值（万元） |
| kdj_qfq / kdj_k_qfq / kdj_d_qfq | KDJ指标 |
| rsi_qfq_12 | RSI指标（12日） |
| macd_qfq / macd_dea_qfq / macd_dif_qfq | MACD指标 |
| atr_qfq | 真实波幅（ATR） |
| mtmma_qfq | MTMMA指标 |
| turnover_rate_f | 换手率（自由流通股） |

### 资金流向字段 (moneyflow_dc)

| 字段名 | 说明 |
|--------|------|
| net_amount | 主力净流入额（万元） |
| buy_elg_amount | 超大单净流入额（万元） |
| buy_lg_amount | 大单净流入额（万元） |
| buy_md_amount | 中单净流入额（万元） |
| buy_sm_amount | 小单净流入额（万元） |

### 融资融券字段 (margin_detail)

| 字段名 | 说明 |
|--------|------|
| rzye | 融资余额（元） |
| rqye | 融券余额（元） |

### 财务指标字段 (fina_indicator)

| 字段名 | 说明 |
|--------|------|
| roe | 净资产收益率 |
| roa | 总资产报酬率 |
| roa2_yearly | 年化总资产报酬率 |
| profit_to_gr | 净利润/营业总收入 |
| q_profit_yoy | 净利润同比增长率（%） |
| q_eps | 每股收益 |
| assets_turn | 总资产周转率 |

## 📁 输出目录结构

```
D:/qlib_data/qlib_data/
├── calendars/
│   └── day.txt              # 交易日历
├── features/
│   ├── open_qfq.day.bin     # 开盘价（前复权）
│   ├── close_qfq.day.bin    # 收盘价（前复权）
│   ├── vol.day.bin          # 成交量
│   ├── ...                  # 其他特征文件
│   └── roe.day.bin          # ROE财务指标
└── instruments/
    └── all.txt              # 股票池信息
```

## 🔧 Qlib数据格式说明

### Bin文件格式

- **存储格式**: 二进制文件（float32，小端序）
- **数据排列**: 每行包含 [日期索引, 特征值]
- **日期索引**: 对应calendars/day.txt中的位置（从0开始）
- **读取方式**: 使用 `numpy.fromfile(file, dtype='<f')` 读取

### 示例代码：读取Qlib数据

```python
import qlib
from qlib.data import D

# 初始化Qlib
qlib.init(provider_uri="D:/qlib_data/qlib_data")

# 获取交易日历
calendar = D.calendar()

# 获取股票列表
instruments = D.instruments('all')

# 读取特征数据
data = D.features(
    instruments=['000001.SZ'],
    start_time='2023-01-01',
    end_time='2023-12-31',
    fields=['$open_qfq', '$close_qfq', '$vol', '$roe']
)

print(data.head())
```

## ⚠️ 注意事项

### 🔄 API频率限制（重要！）

本工具已内置**智能API频率控制**，根据Tushare官方文档自动管理请求速率：

#### 标准模式（默认）- 适合5000积分用户

| 接口 | 频率限制 | 请求间隔 | 说明 |
|------|---------|---------|------|
| **stk_factor_pro** | 30次/分钟 | 2.2秒 | 技术因子接口 |
| **moneyflow_dc** | 30次/分钟 | 2.2秒 | 资金流向接口 |
| **margin_detail** | 30次/分钟 | 2.2秒 | 融资融券接口 |
| **fina_indicator** | 20次/分钟 | 3.3秒 | 财务指标接口（更严格）|
| **stock_basic** | 60次/分钟 | 1.1秒 | 基础接口 |

#### 高速模式 (`--high-speed`) - 需要8000+积分

| 接口 | 频率限制 | 请求间隔 | 提升倍数 |
|------|---------|---------|---------|
| **stk_factor_pro** | **500次/分钟** | **0.13秒** | **16.7x** ⚡ |
| **moneyflow_dc** | 100次/分钟 | 0.66秒 | 3.3x |
| **margin_detail** | 60次/分钟 | 1.1秒 | 2x |
| **fina_indicator** | 40次/分钟 | 1.65秒 | 2x |
| **stock_basic** | 120次/分钟 | 0.55秒 | 2x |

#### ✨ 智能特性

1. **自动速率控制**: 根据每个接口的限制独立控制请求速率
2. **动态调整**: 检测到限流错误时自动降低速率（降低30%）
3. **指数退避重试**: 失败后等待1s → 2s → 4s再重试
4. **安全余量**: 实际间隔比理论值多10%，避免触发限制
5. **实时监控**: 每50只股票打印一次API使用统计
6. **最终报告**: 任务完成后显示所有接口的调用统计

#### 📊 使用示例

```bash
# 标准模式（安全稳定）
python tushare_to_qlib.py --start_date 20230101 --end_date 20231231

# 高速模式（需要8000+积分）
python tushare_to_qlib.py --start_date 20230101 --end_date 20231231 --high-speed
```

#### 💡 性能预估

假设处理1000只股票（每只股票调用4个接口）：

- **标准模式**: 约 4000次请求 ÷ 25次/分钟 ≈ **160分钟 (2.7小时)**
- **高速模式**: 约 4000次请求 ÷ 144次/分钟 ≈ **28分钟**

### Tushare API要求

- **积分要求**:
  - stk_factor_pro: 5000积分（30次/分钟）
  - moneyflow_dc: 5000积分
  - margin_detail: 2000积分
  - fina_indicator: 2000积分（按股票获取）

- **频率限制**: 本脚本已内置0.15秒延迟，约400次/分钟

- **数据量限制**:
  - stk_factor_pro: 单次最多10000条
  - moneyflow_dc: 单次最多6000条
  - margin_detail: 单次最多6000条
  - fina_indicator: 单次最多100条（按股票）

### 数据时间范围

- **stk_factor_pro**: 全历史数据
- **moneyflow_dc**: 2023年9月11日起
- **margin_detail**: 2014年起（逐步完善）
- **fina_indicator**: 季度数据（报告期）

### 性能优化建议

1. **首次运行**: 建议使用 `--max_stocks 10` 测试
2. **缓存机制**: 已实现本地缓存，重复运行会更快
3. **增量更新**: 可调整start_date只获取新数据
4. **并行处理**: 大规模数据可考虑多进程（需注意API限制）

## 🐛 常见问题

### Q1: 提示"未安装tushare"

```bash
pip install tushare
```

### Q2: Token无效或积分不足

- 检查Token是否正确复制（无空格）
- 确认积分是否满足接口要求
- 访问 https://tushare.pro 查看积分详情

### Q3: 某些股票无数据

- 正常现象：新股、退市股可能缺少历史数据
- 部分接口有时间范围限制（如moneyflow_dc从20230911开始）

### Q4: 转换后Qlib无法读取

- 确认目录结构完整（calendars/features/instruments）
- 检查bin文件是否存在且非空
- 使用test脚本验证数据格式

## 📝 更新日志

### v1.0.0 (2026-04-07)
- 初始版本
- 支持4个Tushare数据接口
- 实现Qlib bin格式转换
- 支持缓存和断点续传

## 📄 许可证

MIT License

## 🙏 致谢

- [Tushare](https://tushare.pro) - 金融数据接口
- [Microsoft Qlib](https://github.com/microsoft/qlib) - AI量化投资平台
