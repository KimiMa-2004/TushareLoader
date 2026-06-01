# TushareDataloader

从 [Tushare Pro](https://tushare.pro/) 拉取数据并落盘为 Parquet 的 Python 工具。支持"常量表"（如股票列表）和"按交易日"的日频数据（如日线），对日频数据支持增量：只拉取本地缺失的交易日并追加写入。

同时提供 **Reader** 模块读取已落盘的 Parquet 数据（支持 pandas / DuckDB / Polars），以及 **Schema Overview** 脚本生成全库 Schema 报告。

---

## 项目结构

```
TushareDataloader/
├── main.py                  # 入口示例：配置 logger，调用各类 Loader
├── schema_overview.py       # Schema 概览脚本：扫描 DATAROOT，生成 Markdown 报告
├── loader/
│   ├── __init__.py          # 导出 loader.stock 等子模块
│   ├── base.py              # 基类 Loader、TushareConstantLoader、TushareDailyLoader
│   └── stock.py             # 股票相关：StockBasic、StockDaily
├── reader/
│   ├── __init__.py          # 导出 read_table、get_table_info
│   └── reader.py            # 读取 Parquet，支持 pandas / DuckDB / Polars
├── logger/
│   ├── __init__.py          # 导出 get_logger、delete_logger_file
│   └── logger.py            # 日志配置（控制台 / 文件、LOG_LEVEL、LOGGER_DIR）
├── requirements.txt
├── .env.example             # 环境变量模板（复制为 .env 并填写）
└── README.md
```

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

## Loader — 数据拉取

### 模块说明

- **loader/base.py**  
  - `Loader`：抽象基类，负责 Tushare 连接、数据目录、重试与延迟。  
  - `TushareConstantLoader`：无时间维度，一次拉全量（如 `stock_basic`），存为 `{DATAROOT}/{db_name}/{table_name}/{table_name}.parquet`。  
  - `TushareDailyLoader`：按交易日拉取，仅拉取缺失日期并保存为 `{DATAROOT}/{db_name}/{table_name}/{YYYYMMDD}.parquet`，支持 `start`/`end` 与交易所交易日历。

- **loader/stock.py**  
  - `StockBasic`：继承 `TushareConstantLoader`，拉取股票基础信息。  
  - `StockDaily`：继承 `TushareDailyLoader`，拉取日线，参数 `start`/`end` 格式可为 `YYYY-MM-DD` 或 `YYYYMMDD`。

- **logger/**  
  提供 `get_logger(name=..., filename=..., level=..., log_to_console=...)`，可选写文件到 `LOGGER_DIR`（默认 `./logs`）。

### 使用方法

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

### 扩展更多数据源

- 无时间维度：继承 `TushareConstantLoader`，实现 `_run_func(self) -> pd.DataFrame`，在子类 `__init__` 里调用 `super().__init__(logger, db_name='...', table_name='...')`。  
- 按日拉取：继承 `TushareDailyLoader`，实现 `_run_func(self, date: str) -> pd.DataFrame`（`date` 为 `YYYYMMDD`），并传入 `start`/`end`。

参考 `loader/stock.py` 中的 `StockBasic` 与 `StockDaily`。

---

## Reader — 数据读取

从 `{DATAROOT}/{db_name}/{table_name}` 加载 Parquet 数据，并以指定格式返回：**pandas DataFrame**、**DuckDB Relation**、**Polars DataFrame** 或 **Polars LazyFrame**。

### 数据布局

与 `loader/base.py` 保持一致，Reader 支持三种存储布局：

| 布局类型 | 路径模式 | 示例 |
|----------|----------|------|
| **常量表**（嵌套） | `{DATAROOT}/{db_name}/{table_name}/{table_name}.parquet` | `D:/data/tushare/stock_basic/stock_basic.parquet` |
| **日频表**（嵌套） | `{DATAROOT}/{db_name}/{table_name}/{YYYYMMDD}.parquet` | `D:/data/tushare/stock_daily/20260201.parquet` |
| **扁平单文件** | `{DATAROOT}/{db_name}/{table_name}.parquet` | `D:/data/tushare/stock_basic.parquet` |

`_resolve_path()` 会自动检测使用哪种布局，无需手动指定。

### API

#### `read_table()`

核心函数，加载数据并返回指定格式。

```python
from reader import read_table

df = read_table("tushare", "stock_daily", output_format="pandas")
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `db_name` | `str` | — | 数据库名，如 `"tushare"` |
| `table_name` | `str` | — | 表名，如 `"stock_daily"`、`"stock_basic"` |
| `output_format` | `"pandas" \| "duckdb" \| "polars" \| "polars_lazy"` | `"pandas"` | 返回格式，见下方说明 |
| `dates` | `list[str] \| None` | `None` | 显式日期列表（`YYYYMMDD` 或 `YYYY-MM-DD`），仅对日频表生效 |
| `start` | `str \| None` | `None` | 起始日期（闭区间），仅对日频表生效 |
| `end` | `str \| None` | `None` | 结束日期（闭区间），仅对日频表生效 |
| `columns` | `list[str] \| None` | `None` | 要读取的列名列表；`None` 表示全部列；空列表会抛出 `ValueError` |

**返回格式**

| 格式 | 返回类型 | 适用场景 |
|------|----------|----------|
| `"pandas"` | `pd.DataFrame` | 兼容传统 pandas 工作流 |
| `"duckdb"` | `duckdb.DuckDBPyRelation` | 需要进一步 SQL 链式查询、复杂聚合 |
| `"polars"` | `pl.DataFrame` | 高性能内存计算 |
| `"polars_lazy"` | `pl.LazyFrame` | 惰性查询，延迟执行，支持查询优化；适合超大表仅读取部分列或需要进一步链式操作 |

> **性能提示**：`"polars_lazy"` 绕过 DuckDB，直接使用 Polars 扫描 Parquet，支持列裁剪和谓词下推。`"pandas"`、`"duckdb"`、`"polars"` 内部通过 DuckDB 读取，对小表开销较低。

#### `get_table_info()`

获取表的 Schema 概览信息。

```python
from reader import get_table_info

info = get_table_info("tushare", "stock_daily", output_format="polars")
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `db_name` | `str` | — | 数据库名 |
| `table_name` | `str` | — | 表名 |
| `output_format` | `"polars" \| "pandas" \| "dict"` | `"polars"` | 返回格式 |

**返回列**

| 列名 | 说明 |
|------|------|
| `column_name` | 字段名 |
| `dtype` | 数据类型 |
| `non_null_count` | 非空值数量 |
| `null_pct` | 空值百分比 |

结果按 `null_pct` 降序排列，方便快速发现缺失严重的字段。

### 使用示例

#### 基本读取

```python
from reader import read_table, get_table_info

# 读取常量表
stock_basic = read_table("tushare", "stock_basic", output_format="pandas")

# 读取日频表，指定日期范围
daily = read_table("tushare", "stock_daily", start="2026-02-01", end="2026-02-28")

# 按显式日期列表读取
daily = read_table("tushare", "stock_daily", dates=["20260201", "20260202"])

# 只读取部分列（列裁剪下推到 Parquet）
daily = read_table("tushare", "stock_daily", columns=["trade_date", "close", "volume"])

# 使用 DuckDB 进行 SQL 链式查询
rel = read_table("tushare", "stock_daily", output_format="duckdb")
avg_close = rel.filter("close > 0").aggregate("AVG(close) AS avg_close")

# 使用 Polars LazyFrame 惰性查询
lf = read_table("tushare", "stock_daily", output_format="polars_lazy")
result = lf.filter(pl.col("close") > 100).select("trade_date", "close").collect()
```

#### Schema 分析

```python
# 查看表的字段信息
info = get_table_info("tushare", "stock_daily", output_format="polars")
print(info)

# 输出字典格式
info = get_table_info("tushare", "stock_basic", output_format="dict")
print(info)
```

### 模块导出

```python
# reader/__init__.py
from reader.reader import read_table, get_table_info
```

顶层直接可用 `read_table` 和 `get_table_info`，无需深入到子模块。

---

## Schema Overview — Schema 报告生成

扫描 `DATAROOT` 下所有数据库和表，生成一份 Markdown 格式的 Schema 概览报告，包含每个表的字段名、数据类型、非空数量及空值百分比。

### 快速开始

```bash
# 默认输出 schema_overview.md
python schema_overview.py

# 指定输出路径
python schema_overview.py -o report.md
```

运行前确保 `.env` 中已配置 `DATAROOT`，如果 `DATAROOT` 目录不存在，脚本会报错退出。

### 命令行参数

| 参数 | 说明 |
|------|------|
| `-o`, `--output` | 输出文件路径，默认 `schema_overview.md` |

### 工作原理

**1. 表发现**

遍历 `DATAROOT` 下的所有子目录（每个子目录视为一个数据库），自动识别两种表布局：

扁平表（数据库目录下的单文件）：
```
{db_dir}/
  └── table_name.parquet
```
→ 表名 = 文件名去掉 `.parquet`

嵌套表（数据库目录下的子文件夹）：
```
{db_dir}/
  └── table_name/
        ├── table_name.parquet        ← 常量表
        └── 20260201.parquet          ← 日频表
```
→ 以子文件夹名作为表名，自动区分单文件 / 多文件

**2. 表画像**

对每个发现的表，使用 Polars 读取并统计：

| 统计项 | 说明 |
|--------|------|
| `database` | 所在数据库名 |
| `table_name` | 表名 |
| `column_name` | 字段名 |
| `dtype` | Polars 数据类型 |
| `non_null_count` | 非空值数量 |
| `total_rows` | 总行数 |
| `null_pct` | 空值百分比（保留两位小数） |

结果按 `null_pct` 降序排列，缺失最严重的列排在最前面。

**3. 报告生成**

生成的 Markdown 报告结构如下：

```
# Schema Overview
**Generated**: 2026-06-01 22:00:00
**DATAROOT**: `D:/data`
**Databases**: 1
**Tables**: 3
**Total Columns**: 45

## Database: `tushare` (3 tables)

### Table: `stock_basic`
**Rows**: 5,230 | **Columns**: 6

| Column | Type | Non-Null Count | Null % |
| :--- | :--- | ---: | ---: |
| exchange | String | 5,230 | 0.00 |
| ... | ... | ... | ... |

### Table: `stock_daily`
...
```

### 错误处理

- 如果 `DATAROOT` 目录不存在 → 打印 `ERROR` 并退出
- 如果某个表读取失败 → 打印 `[WARN]` 并跳过，不影响其他表的扫描
- 如果某个表数据为空（0 行）→ 跳过该表
- 如果目录下没有任何 `.parquet` 文件 → 跳过该目录