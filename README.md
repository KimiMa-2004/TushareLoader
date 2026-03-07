# TushareDataloader

从 [Tushare Pro](https://tushare.pro/) 拉取数据并落盘为 Parquet 的 Python 工具。支持“常量表”（如股票列表）和“按交易日”的日频数据（如日线），对日频数据支持增量：只拉取本地缺失的交易日并追加写入。

---

## 项目结构

```
TushareDataloader/
├── main.py              # 入口示例：配置 logger，调用各类 Loader
├── loader/
│   ├── __init__.py      # 导出 loader.stock 等子模块
│   ├── base.py          # 基类 Loader、TushareConstantLoader、TushareDailyLoader
│   └── stock.py         # 股票相关：StockBasic、StockDaily
├── logger/
│   ├── __init__.py      # 导出 get_logger、delete_logger_file
│   └── logger.py        # 日志配置（控制台 / 文件、LOG_LEVEL、LOGGER_DIR）
├── requirements.txt
├── .env.example         # 环境变量模板（复制为 .env 并填写）
└── README.md
```

- **loader/base.py**  
  - `Loader`：抽象基类，负责 Tushare 连接、数据目录、重试与延迟。  
  - `TushareConstantLoader`：无时间维度，一次拉全量（如 `stock_basic`），存为 `{DATAROOT}/{db_name}/{table_name}/{table_name}.parquet`。  
  - `TushareDailyLoader`：按交易日拉取，仅拉取缺失日期并保存为 `{DATAROOT}/{db_name}/{table_name}/{YYYYMMDD}.parquet`，支持 `start`/`end` 与交易所交易日历。

- **loader/stock.py**  
  - `StockBasic`：继承 `TushareConstantLoader`，拉取股票基础信息。  
  - `StockDaily`：继承 `TushareDailyLoader`，拉取日线，参数 `start`/`end` 格式可为 `YYYY-MM-DD` 或 `YYYYMMDD`。

- **logger/**  
  提供 `get_logger(name=..., filename=..., level=..., log_to_console=...)`，可选写文件到 `LOGGER_DIR`（默认 `./logs`）。

---

## 环境要求

- Python 3.10+
- 在 [Tushare Pro](https://tushare.pro/register) 注册并获取 token

---

## 安装与配置

1. 克隆仓库并安装依赖：

   ```bash
   pip install -r requirements.txt
   ```

2. 复制环境变量模板并填写（不要将 `.env` 提交到 Git）：

   ```bash
   copy .env.example .env   # Windows
   # cp .env.example .env  # Linux/macOS
   ```

3. 在 `.env` 中必填：

   | 变量 | 说明 |
   |------|------|
   | `TUSHARE_TOKEN` | Tushare Pro 的 token |
   | `DATAROOT` | 数据根目录，所有 Parquet 输出在 `{DATAROOT}/{db_name}/{table_name}/` 下 |

   可选：

   | 变量 | 默认值 | 说明 |
   |------|--------|------|
   | `LOG_LEVEL` | `INFO` | 日志级别 |
   | `LOGGER_DIR` | `./logs` | 日志文件目录 |

---

## 使用方法

在项目根目录下运行（保证能 import 到 `logger` 和 `loader`）：

```bash
python main.py
```

**示例代码**（见 `main.py`）：

```python
from logger import get_logger
import loader

logger = get_logger(name='stock_loader', filename='stock', log_to_console=False)
# 拉取股票基础信息（常量表，全量）
_ = loader.stock.StockBasic(logger)
# 拉取日线，仅补全 2026-02-01 起缺失的交易日
_ = loader.stock.StockDaily(logger, start='2026-02-01')
```

- 常量表：每次运行会覆盖同路径下的 `{table_name}.parquet`。  
- 日频表：仅在存在缺失交易日时请求 API，并将新日期写入 `{YYYYMMDD}.parquet`。

---

## 扩展更多数据源

- 无时间维度：继承 `TushareConstantLoader`，实现 `_run_func(self) -> pd.DataFrame`，在子类 `__init__` 里调用 `super().__init__(logger, db_name='...', table_name='...')`。  
- 按日拉取：继承 `TushareDailyLoader`，实现 `_run_func(self, date: str) -> pd.DataFrame`（`date` 为 `YYYYMMDD`），并传入 `start`/`end`。

参考 `loader/stock.py` 中的 `StockBasic` 与 `StockDaily`。
