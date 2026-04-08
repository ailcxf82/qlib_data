# 🚀 快速开始指南

## 5分钟完成Tushare数据到Qlib格式的转换

### 第一步：安装依赖

```bash
cd d:\quant_project\qlib_Data
pip install -r requirements.txt
```

### 第二步：设置Token（环境变量，推荐✅）

**Windows PowerShell:**
```powershell
$env:TUSHARE_API_KEY="你的Tushare_Token"
```

**Windows CMD:**
```cmd
set TUSHARE_API_KEY=你的Tushare_Token
```

**Linux/Mac:**
```bash
export TUSHARE_API_KEY=你的Tushare_Token
```

> 💡 **提示**: 获取Token地址: https://tushare.pro → 个人中心 → API Token

### 第三步：测试运行（单只股票）

```bash
python test_tushare_to_qlib.py --mode single
```

这将：
- ✅ 从环境变量自动读取Token（无需命令行参数）
- 📊 获取平安银行(000001.SZ) 2023年全年的数据
- 🔗 从4个Tushare接口获取数据（技术因子、资金流向、融资融券、财务指标）
- ⚡ **智能频率控制** - 自动管理API请求速率，避免触发限制
- 💾 转换为Qlib标准格式保存到 `D:/qlib_data/qlib_data_test/`
- 📈 显示实时API调用统计

### 第四步：验证数据

```bash
python test_tushare_to_qlib.py --mode verify
```

检查输出目录结构是否正确。

### 第五步：完整运行（全部股票）

#### 标准模式（安全稳定，适合5000积分）

```bash
python tushare_to_qlib.py --start_date 20200101 --end_date 20231231
```

#### 高速模式⚡（需要8000+积分，速度提升10倍以上！）

```bash
python tushare_to_qlib.py --start_date 20200101 --end_date 20231231 --high-speed
```

> ⚠️ **注意**: 全量下载可能需要较长时间（取决于股票数量和时间范围）
>
> - 标准模式: ~1000只股票约需2.7小时
> - 高速模式: ~1000只股票约需28分钟

---

## 📂 生成的文件说明

```
d:\quant_project\qlib_Data\
├── tushare_to_qlib.py          # 主程序：数据获取和转换（已集成智能频率控制）
├── test_tushare_to_qlib.py     # 测试脚本：验证功能（支持环境变量）
├── README.md                   # 完整文档（含频率限制详细说明）
├── ENV_SETUP_GUIDE.md          # 环境变量配置指南
├── QUICKSTART.md               # 本文件 - 快速开始
├── requirements.txt            # 依赖包列表
├── config/
│   └── secrets.yaml.example    # 配置文件模板
└── examples/
    └── use_qlib_data.py        # Qlib使用示例代码
```

---

## 💡 常用命令

| 目标 | 命令 | 说明 |
|------|------|------|
| **测试单只股票** | `python test_tushare_to_qlib.py --mode single` | 验证功能正常 |
| **测试10只股票** | `python test_tushare_to_qlib.py --mode multiple` | 小规模测试 |
| **验证生成的数据** | `python test_tushare_to_qlib.py --mode verify` | 检查数据格式 |
| **标准全量转换** | `python tushare_to_qlib.py --start_date 20230101 --end_date 20231231` | 安全稳定 |
| **高速全量转换** | `python tushare_to_qlib.py --start_date 20230101 --end_date 20231231 --high-speed` | 需8000+积分 |
| **小规模测试** | 添加参数 `--max_stocks 10` | 只处理10只股票 |
| **自定义输出目录** | 添加参数 `--output_dir YOUR_PATH` | 指定保存位置 |

---

## 🔄 智能API频率控制

本工具内置了**企业级的API频率控制系统**：

### ✨ 核心特性

1. **📊 分接口独立控制**: 每个Tushare接口有独立的速率限制
2. **🎯 自动限流检测**: 智能识别限流错误并动态调整
3. **🔄 指数退避重试**: 失败后1s→2s→4s自动重试
4. **📈 实时监控**: 每50只股票显示API使用统计
5. **🛡️ 安全余量**: 实际间隔比限制多10%，确保安全
6. **⚡ 高速模式**: 支持8000+积分用户的高速访问

### 📋 频率限制对比

| 接口 | 标准模式 (5000分) | 高速模式 (8000+分) | 提升 |
|------|------------------|------------------|------|
| stk_factor_pro | 30次/分钟 | **500次/分钟** | **16.7x** ⚡ |
| moneyflow_dc | 30次/分钟 | 100次/分钟 | 3.3x |
| margin_detail | 30次/分钟 | 60次/分钟 | 2x |
| fina_indicator | 20次/分钟 | 40次/分钟 | 2x |

详见 [README.md](./README.md) 的"API频率限制"章节。

---

## 🔗 下一步

数据生成后，可以：

1. **查看示例代码** - 运行Qlib使用示例：
   ```bash
   cd examples
   python use_qlib_data.py --example basic      # 基础用法
   python use_qlib_data.py --example technical  # 技术指标
   python use_qlib_data.py --example workflow   # 完整工作流
   ```

2. **在Qlib中使用** - 参考README.md中的"Qlib数据格式说明"章节

3. **构建策略** - 使用Qlib的表达式引擎构建Alpha因子

4. **了解详情** - 查看 [ENV_SETUP_GUIDE.md](./ENV_SETUP_GUIDE.md) 了解更多环境变量配置方法

---

## ❓ 遇到问题？

### 问题1: 提示"未提供Tushare API Token"

**解决方案**:
```powershell
# 确保已设置环境变量
$env:TUSHARE_API_KEY="your_token"

# 验证设置
echo $env:TUSHARE_API_KEY

# 重新运行
python test_tushare_to_qlib.py --mode single
```

详见 [ENV_SETUP_GUIDE.md](./ENV_SETUP_GUIDE.md)

### 问题2: 出现"请求过快"或"频率限制"错误

**这是正常的！** 工具会自动处理：
- ✅ 自动等待合适的时间间隔
- ✅ 动态降低请求速率
- ✅ 自动重试（最多3次）
- ✅ 继续处理后续股票

如果频繁出现，建议切换到标准模式（不使用 `--high-speed`）。

### 问题3: 想查看详细的API调用日志

程序会在以下时间点输出统计信息：
- 每50只股票打印一次中间统计
- 任务完成后打印最终报告

示例输出：
```
📊 API调用统计（最近1分钟）:
   stk_factor_pro         :  28次 /  30.0次/分钟 (利用率:  93.3%)
   moneyflow_dc           :  29次 /  30.0次/分钟 (利用率:  96.7%)
   margin_detail           :  27次 /  30.0次/分钟 (利用率:  90.0%)
   fina_indicator          :  18次 /  20.0次/分钟 (利用率:  90.0%)
   stock_basic             :   1次 /  60.0次/分钟 (利用率:   1.7%)
```

---

## ⚡ 性能优化建议

- **首次测试**: 始终先用 `--max_stocks 10` 或 `--mode single` 测试
- **缓存机制**: 重复运行会自动读取缓存，速度更快
- **增量更新**: 调整start_date只获取新数据，避免全量重新下载
- **选择合适模式**:
  - 5000积分 → 标准模式（稳定可靠）
  - 8000+积分 → 高速模式（极速体验）
- **避开高峰期**: Tushare服务器在工作日9:00-15:00可能较慢

---

## 📚 更多文档

- 📘 [README.md](./README.md) - 完整文档（字段说明、API限制、性能优化等）
- 📗 [ENV_SETUP_GUIDE.md](./ENV_SETUP_GUIDE.md) - 环境变量配置详细指南
- 📙 [examples/use_qlib_data.py](./examples/use_qlib_data.py) - 6个实战案例

---

祝使用愉快！🎉

如有问题，请查看 [常见问题](./README.md#-常见问题) 章节。
