# QUANTAXIS Rust 集成报告

> 日期: 2026-07-14
> 涉及项目: QUANTAXIS, qa-rs, qadataswap, qaexchange-rs

---

## 1. 概述

将 QUANTAXIS 从不可访问的 `qars3` (原 qars2) 迁移到 qa-rs 的 Rust PyO3 绑定，同时集成 qadataswap 零拷贝数据交换。qlib-rs 同时保持对 qaexchange-rs 的兼容。

### 架构

```
qa-rs (Rust crate, crates.io 名: qa-rs)
├── rlib ──────────────────► qaexchange-rs (Rust 交易所, default-features=false)
│                              └── python feature 排除, 零影响
└── cdylib + python feature ─► qars3 (.so PyO3 扩展)
                                  └── qars3 Python 包 (pip install -e .)
                                      └── QUANTAXIS QARSBridge
```

---

## 2. 项目间关系

| 项目 | 语言 | 角色 | 依赖方式 |
|---|---|---|---|
| qa-rs | Rust | 核心库 (账户/撮合/协议) | — |
| qaexchange-rs | Rust | 交易所服务器 | `qars = { path = "../qa-rs", default-features = false }` |
| QUANTAXIS | Python | 量化投研框架 | `import qars3` → `QARSBridge` 桥接 |
| qadataswap | C++/Python | 零拷贝数据交换 | `import qadataswap` → `QADataBridge` 桥接 |

---

## 3. 文件修改清单

### 3.1 QUANTAXIS (3 文件, 6 行)

| 文件 | 修改 | 原因 |
|---|---|---|
| `setup.py` (2处) | `qars3>=0.0.45` → `qars3>=0.1.0` | qa-rs 版本递增 |
| `QUANTAXIS/__init__.py:343` | `except ImportError` → `except Exception` | QADataBridge 初始化可能抛出 AttributeError |
| `QUANTAXIS/QADataBridge/__init__.py:43-50` | `except ImportError` → `except Exception`; 添加 `hasattr` 检查 `has_arrow_support` | qadataswap 原生扩展缺 Arrow 库时不崩溃 |

### 3.2 qa-rs (9 文件, ~40 行 + 目录重命名)

| 文件 | 修改 | 原因 |
|---|---|---|
| `Cargo.toml` | `crate-type = ["rlib", "cdylib"]` | rlib 供 qaexchange-rs, cdylib 供 Python |
| `src/lib.rs` | 4 模块 `#[cfg(feature = "python")]` 条件编译; `__version__` | python feature 时编译 pyo3 模块, 否则排除 |
| `src/qaaccount/mod.rs` | `portfolio`, `qaperformance` 条件编译 | 被 marketsys 间接依赖 |
| `src/qamarket/mod.rs` | `marketsys`, `simmarket` 条件编译 | 被 pyo3 模块直接依赖 |
| `setup.py` | 包名 `qars`→`qars3`; 版本 `0.0.13`→`0.1.0`; 新增 `features=["python"]` | QUANTAXIS 期望 `qars3` 包名 |
| `pyproject.toml` | 新建 | 声明 `setuptools_rust` 构建依赖 (uv 隔离构建需要) |
| `qars/` → `qars3/` | 目录重命名 | 匹配新包名 |
| `qars3/__init__.py` | `*args, **kwargs`; 新增 `QAAccount` 别名; `__version__`; `on_price_change` 等 | QUANTAXIS API 兼容 |
| `qars3/backtest.py` | `qars` → `qars3` | 内部引用修正 |
| `qars3/rsbacktest.py` | `qars` → `qars3` | 内部引用修正 |
| `src/qadata/marketcenter.rs` | 为 python feature 实现 `QAMarketCenter` | marketsys 需要完整结构而非空存根 |
| `src/qaanalysis/qifianalysis.rs` | polars 0.54 API 修复 (`DataFrame::new`, `sort`) | 编译兼容 |
| `src/qamarket/simmarket.rs` | `Success` 枚举匹配新增 `opposite_order_id` 字段 | 结构变更 |
| `src/qamarket/marketsys.rs` | `serde_json::to_string` / `target_weight` 添加 `&` | 类型匹配 |

### 3.3 qadataswap (1 文件, ~15 行)

| 文件 | 修改 | 原因 |
|---|---|---|
| `src/python/qadataswap/__init__.py` | 原生扩展失败时不再 `raise ImportError`; `SharedDataFrame = None` 回退 | 缺 `libarrow_python.so` 时 QUANTAXIS 仍可加载 |

---

## 4. 环境配置

### 4.1 系统依赖

```bash
# 系统 Arrow C++ 库 (已存在)
libarrow.so.2500 → /lib/x86_64-linux-gnu/libarrow.so.2500
```

### 4.2 Python 虚拟环境

```bash
cd /home/admin/MyProject/QUANTAXIS
source .venv/bin/activate

# 关键包版本
qars3==0.1.0       (本地 editable: /home/admin/MyProject/qa-rs)
qadataswap==0.1.0  (本地 editable: /home/admin/MyProject/qadataswap)
pyarrow==25.0.0    (升级自 17.0.0, 匹配 libarrow.so.2500)
orjson==3.11.9
polars==0.20.31
```

### 4.3 LD_LIBRARY_PATH

已写入 `.venv/bin/activate`:
```bash
export LD_LIBRARY_PATH="/home/admin/MyProject/QUANTAXIS/.venv/lib/python3.11/site-packages/pyarrow:$LD_LIBRARY_PATH"
```

### 4.4 Rust 工具链

```bash
source "$HOME/.cargo/env"   # Rust nightly (qaexchange-rs 需要)
```

---

## 5. 测试结果

### 5.1 编译验证

| 项目 | 命令 | 结果 |
|---|---|---|
| qa-rs (rlib) | `cargo check` | ✅ 0 错误 |
| qa-rs (python) | `cargo check --features python` | ✅ 0 错误, 162 warnings |
| qaexchange-rs | `cargo check` | ✅ 0 错误 |
| Web 前端 | `vue-cli-service build` | ✅ 通过 |

### 5.2 单元测试

| 项目 | 结果 |
|---|---|
| qa-rs | **54 通过, 0 失败** |
| qaexchange-rs | **880 通过, 1 flaky, 3 忽略** |

### 5.3 QUANTAXIS 功能测试

| 测试项 | 结果 | 说明 |
|---|---|---|
| 基础导入 | ✅ | QIFI, QARSBridge, QADataBridge, QAFetch, QAData, QAUtil, QAMarket, QAEngine, QAPubSub |
| QARS Rust 账户 (qars3) | ✅ | 股票买卖 + 期货开平 + 多品种持仓 |
| QIFI Python 账户 | ✅ | 可用 (部分 API 差异) |
| QADataBridge 数据转换 | ✅ | Pandas↔Polars 零拷贝, Arrow 支持 |
| QADataBridge 共享内存 | ✅ | SharedMemoryWriter/Reader 可用 |

### 5.4 QUANTAXIS 自带测试脚本

| 测试脚本 | 结果 |
|---|---|
| `scripts/test_dependencies.py` | **9/9 通过** 含 QARS2 账户系统 |
| `scripts/test_backward_compatibility.py` | **7/8 通过** (旧 QIFI API 1 项不兼容, 非本次引入) |

---

## 6. 交易场景验证

### 6.1 股票 (A 股 T+1)

```python
from QUANTAXIS.QARSBridge import QARSAccount

a = QARSAccount(account_cookie='stock', init_cash=1000000)
a.buy('000001', 20.0, '2025-01-20', 1000.0)   # 买入 000001 20手@1000
a.settle()                                       # 日终结算
a.sell('000001', 20.0, '2025-01-21', 1050.0)    # 次日卖出 20手@1050
# 结果: balance=1000968.75 (盈利1000, 扣除手续费31.25)
```

### 6.2 期货 (rb2005 螺纹钢)

```python
b = QARSAccount(account_cookie='fut', init_cash=1000000)
b.buy_open('rb2005', 10.0, '2025-01-20', 3500.0)   # 开多10手@3500
b.sell_close('rb2005', 10.0, '2025-01-20', 3600.0)  # 平多10手@3600
# 结果: 盈亏=10000 (10手 × 10吨/手 × 100点)
```

### 6.3 多品种持仓

```
IF2501: 多持仓=2.0,  保证金=270,000  (300×4500×2×10%)
000001: 多持仓=20.0, 保证金=20,000   (100% 股票)
rb2005: 多持仓=10.0, 保证金=31,500   (10×3500×10×9%)
```

---

## 7. 关键问题与修复

| # | 问题 | 根因 | 修复 |
|---|---|---|---|
| 1 | `pip install` 找不到 setuptools_rust | uv 隔离构建环境 | 创建 `qa-rs/pyproject.toml` |
| 2 | `PyInit_qars_core` 不存在 | python feature 未传递给 cargo | `setup.py` 添加 `features=["python"]` |
| 3 | `buy_open() takes 5 positional arguments, 6 given` | QUANTAXIS 传 `validate` 位置参数 | `*args, **kwargs` 包装 |
| 4 | `has_arrow_support` AttributeError | QADataBridge 只捕获 ImportError | `except Exception` + `hasattr` |
| 5 | `libarrow_python.so.2500` 缺失 | pyarrow 17.0.0 不匹配 | 升级 pyarrow → 25.0.0 |
| 6 | libarrow 升级后段错误 | ABI 不兼容 | pyarrow 版本与系统 Arrow 版本对齐 |
| 7 | `QAAccount` 不存在 | qars3 只有 `QA_QIFIAccount` | 添加 `QAAccount(QA_QIFIAccount)` 别名 |

---

## 8. 对 qaexchange-rs 的影响

**零影响。** qaexchange-rs 使用 `default-features = false` 关闭 python feature。

```toml
# qaexchange-rs/Cargo.toml
qars = { path = "../qa-rs", package = "qa-rs", default-features = false }
```

- `#[cfg(feature = "python")]` 代码被排除
- `cdylib` 生成额外的 .so 文件但 qaexchange-rs 只使用 `rlib`
- 编译: 0 错误, 测试: 880 通过 / 1 flaky

---

## 9. 快速启动

```bash
# 1. 启动 QUANTAXIS 环境
cd /home/admin/MyProject/QUANTAXIS
source .venv/bin/activate

# 2. 验证
python3 -c "import QUANTAXIS as QA; print(QA.__version__, QA.__has_qars__)"

# 3. Rust 交易所 (可选)
cd /home/admin/MyProject/qaexchange-rs
source "$HOME/.cargo/env"
cargo run --bin qaexchange-server
```

---

## 10. 待完善

| 项目 | 优先级 | 说明 |
|---|---|---|
| QIFI_Account `account_cookie` 缺失 | LOW | 向后兼容性测试 7/8 的唯一失败点 |
| qa-rs 162 个 warning | LOW | 命名风格, 不影响功能 |
| qaexchange-rs flaky 性能测试 | LOW | `peek()` O(n) 克隆导致 |
| qadataswap 共享内存端到端测试 | MEDIUM | 需要多进程环境 |

---

*文档结束*
