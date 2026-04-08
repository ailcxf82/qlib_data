# 🎯 环境变量配置指南

## 快速设置 TUSHARE_API_KEY

### Windows 系统

#### PowerShell（推荐）

```powershell
# 1. 设置当前会话的环境变量
$env:TUSHARE_API_KEY="your_tushare_token_here"

# 2. 运行程序
python tushare_to_qlib.py --start_date 20230101 --end_date 20231231 --max_stocks 10

# 或者运行测试
python test_tushare_to_qlib.py --mode single
```

**永久设置（推荐）:**

```powershell
# 添加到用户环境变量（永久生效）
[System.Environment]::SetEnvironmentVariable("TUSHARE_API_KEY", "your_token_here", "User")

# 验证设置
[System.Environment]::GetEnvironmentVariable("TUSHARE_API_KEY", "User")

# 重启终端后生效，之后无需每次设置
python tushare_to_qlib.py --start_date 20230101 --end_date 20231231
```

#### CMD 命令提示符

```cmd
# 1. 设置当前会话的环境变量
set TUSHARE_API_KEY=your_tushare_token_here

# 2. 运行程序
python tushare_to_qlib.py --start_date 20230101 --end_date 20231231 --max_stocks 10
```

**永久设置：**

```cmd
# 使用setx命令（永久生效）
setx TUSHARE_API_KEY "your_tushare_token_here"

# 重启CMD后生效
```

### Linux / macOS 系统

```bash
# 1. 设置当前会话的环境变量
export TUSHARE_API_KEY="your_tushare_token_here"

# 2. 运行程序
python tushare_to_qlib.py --start_date 20230101 --end_date 20231231 --max_stocks 10
```

**永久设置（添加到 shell 配置文件）:**

```bash
# Bash (Linux/macOS)
echo 'export TUSHARE_API_KEY="your_tushare_token_here"' >> ~/.bashrc
source ~/.bashrc

# Zsh (macOS默认)
echo 'export TUSHARE_API_KEY="your_tushare_token_here"' >> ~/.zshrc
source ~/.zshrc

# 验证
echo $TUSHARE_API_KEY
```

---

## ✅ 验证环境变量是否设置成功

### Windows PowerShell

```powershell
# 检查是否已设置
if ($env:TUSHARE_API_KEY) {
    Write-Host "✅ TUSHARE_API_KEY 已设置"
    Write-Host "值长度: $($env:TUSHARE_API_KEY.Length) 字符"
} else {
    Write-Host "❌ TUSHARE_API_KEY 未设置"
}
```

### Linux/macOS

```bash
# 检查是否已设置
if [ -n "$TUSHARE_API_KEY" ]; then
    echo "✅ TUSHARE_API_KEY 已设置"
    echo "值长度: ${#TUSHARE_API_KEY} 字符"
else
    echo "❌ TUSHARE_API_KEY 未设置"
fi
```

---

## 🔧 常见问题

### Q1: 设置了环境变量但程序还是提示未找到Token？

**可能原因和解决方案：**

1. **设置后未重启终端**
   - 解决方案：关闭并重新打开终端窗口

2. **PowerShell vs CMD 混用**
   - 在PowerShell设置的变量在CMD中不可见，反之亦然
   - 解决方案：使用 `[System.Environment]::SetEnvironmentVariable()` 进行永久设置

3. **IDE/编辑器需要重启**
   - VS Code、PyCharm等IDE可能需要重启才能识别新的环境变量
   - 解决方案：重启IDE或编辑器

### Q2: Token中包含特殊字符怎么办？

如果Token包含特殊字符（如 `&`, `%`, `!` 等），确保正确引号：

```powershell
# PowerShell - 使用单引号
$env:TUSHARE_API_KEY='your_token_with_special_chars_&%#!'

# 或双引号（某些字符需要转义）
$env:TUSHARE_API_KEY="your_token_here"
```

```bash
# Linux/Mac - 使用单引号（推荐）
export TUSHARE_API_KEY='your_token_with_special_chars_&%#!'
```

### Q3: 如何临时覆盖环境变量的Token？

命令行参数优先级最高：

```bash
# 环境变量设置了Token A
export TUSHARE_API_KEY="token_A"

# 但可以使用 --token 参数使用 Token B
python tushare_to_qlib.py --token token_B --start_date 20230101 --end_date 20231231
```

### Q4: 如何在多个项目间管理不同的Token？

建议使用不同方式：

- **开发/测试**: 使用命令行参数 `--token`
- **生产环境**: 使用环境变量（永久设置）
- **CI/CD**: 在流水线配置中设置环境变量

---

## 🛠️ 高级用法

### 在 Python 脚本中读取环境变量

```python
import os

# 读取Token
token = os.environ.get('TUSHARE_API_KEY')

if not token:
    raise ValueError("请设置环境变量 TUSHARE_API_KEY")

print(f"Token 已读取，长度: {len(token)}")
```

### 在 .env 文件中管理（开发环境）

创建 `.env` 文件：

```env
# .env 文件
TUSHARE_API_KEY=your_token_here
```

安装 `python-dotenv` 并加载：

```bash
pip install python-dotenv
```

```python
from dotenv import load_dotenv
import os

load_dotenv()  # 加载 .env 文件

token = os.environ.get('TUSHARE_API_KEY')
```

⚠️ **注意**: 确保 `.env` 文件已添加到 `.gitignore`，避免泄露Token！

---

## 📚 相关资源

- [Tushare 官网](https://tushare.pro) - 注册获取Token
- [Windows 环境变量文档](https://docs.microsoft.com/en-us/windows/win32/procthread/environment-variables)
- [Linux 环境变量指南](https://linuxize.com/post/how-to-set-and-list-environment-variables-in-linux/)

---

## 💡 最佳实践

✅ **推荐做法**：
1. 使用永久环境变量设置（避免每次手动输入）
2. 将Token添加到 `.gitignore` 相关文件
3. 定期更换Token（安全考虑）
4. 监控API调用频率（避免超额）

❌ **避免做法**：
1. 不要将Token硬编码在代码中
2. 不要将Token提交到版本控制系统
3. 不要在公共场合分享Token
4. 不要使用过短或不安全的Token
