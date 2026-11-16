# 仪器检验值标准化与临床试验数据离线导出

FastAPI + PostgreSQL。按仪器维护标准化规则 **标准值 = round(a × 原测值 + b, 4)**
（四舍五入、远离零，原测值永不修改），换规则走后台重算并在**单个事务内原子切换**；
同时保留基于同一 MVCC 快照、双（三）连接并行的 ZIP 离线导出。

## 启动

```bash
docker compose up --build
```

浏览器打开 <http://localhost:8000> 。首次启动时 `db/init.sql` 自动灌入合成数据
（3 个试验 T-100/T-200/T-300，各 2 万患者，每人 5 条检验，共 30 万条；
3 台仪器 INST-A/B/C，初始规则 a=1、b=0，并预置 30 万条标准值）。

## 数据模型

| 表 | 作用 |
|---|---|
| `patients` | 患者号、试验号 |
| `instruments` | 仪器号；`active_a/active_b/rule_version` 为当前**生效**规则 |
| `labs` | 检验号、患者号、**原测值**（标准化流程永不修改）、仪器号（可空、可改） |
| `lab_standard` | 持久化标准值，附 `rule_version/a/b`，记录每行由哪版规则生成 |
| `norm_jobs` | 重算轮次（running/succeeded/failed），含进度 `done_labs/total_labs/pass_no` |
| `norm_job_lab` | 重算暂存表，切换事务才把结果发布到 `lab_standard` |

## 标准化与一致性设计（normalizer.py + 触发器）

1. **初始规则 a=1、b=0**：建库时批量生成标准值；之后所有增改删由行触发器
   `labs_standard_maintain()` 在**同一事务**内按当前生效规则维护 `lab_standard`，
   因此任何独立 SQL 连接写入都满足「原值、规则、标准值同属一个已提交状态」。
2. **换规则 = 开一轮后台重算**，每仪器同一时刻只允许一轮
   （部分唯一索引 `norm_jobs_one_running` + 应用锁双保险）。重算把新结果分批
   （每批 2000 行、独立短事务）写入暂存表，**成功前查询始终走旧规则与旧标准值**。
3. **重算不挡写、不挡查询**：分批短事务不持长锁；扫描期间的新增/改值通过多轮
   扫描（最多 3 轮）+ 切换前对账补齐。重算期间独立连接可自由增改删检验、变更仪器号。
4. **最终切换只有一个事务**（`_publish`）：
   - 先取与触发器同一把 `pg_advisory_xact_lock(910017, hashtext(instrument_id))`，
     因此切换开始前已提交的写全部可见，**不会漏掉新增检验、不会漏掉恢复的已删检验、
     不会覆盖新测值**；仅该事务运行的短暂时间内该仪器的写被挡下，无关仪器不受影响；
   - 事务内完成「暂存对账 → 强校验行数一一对应 → 发布 `lab_standard` →
     翻转 `instruments` 生效规则与版本号」，外部不可能看到半成品；
   - 提交即释放锁，被挡的写随后按**新规则**由触发器维护。
5. **失败与重启**：失败只把轮次置 `failed` 并清理暂存，旧规则与现有标准值完全不受
   影响、继续供数；失败轮可通过 `POST /api/jobs/{id}/retry` 重试。应用启动时
   `recover()` 把残留 `running` 轮次置为 failed 并清暂存。
6. 慢重算可通过换规则时的 `batch_delay`（每批休眠秒数，休眠在事务外、不占锁）演示，
   验证慢重算不阻塞查询与无关仪器操作。

### 状态机

```
running ─┬─► succeeded   （暂存对账通过，规则与全部标准值在一个事务内切换）
         └─► failed      （任一 SQL 失败/连接断开；旧规则与旧结果保留，可重试）
```

## 网页功能（GET /）

1. **仪器规则与重算进度**：每台仪器的生效规则/版本、检验数与标准值数；填写新 a、b
   （可带慢重算延迟）提交换规则；进度条与轮次状态每秒刷新；失败轮显示「重试」按钮。
2. **检验维护**：新增检验、改原测值、变更/清除仪器号、删除检验（重算期间同样可用）。
3. **按试验查看**：分页查看原值、标准值、规则版本与 a/b（接口用单条 REPEATABLE READ
   只读事务，保证同一已提交快照）。
4. **快照导出**：ZIP 含 `patients.csv`、`labs.csv`（含仪器号、标准值、规则版本、a/b）、
   `rules.csv`（快照时刻各仪器生效规则）。
5. 源端写入模拟（跨表事务更正，含随机仪器号变更）、数据库连接观察、事件日志。

## API 摘要

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/instruments` | 仪器、生效规则、检验/标准值数、最近轮次 |
| POST | `/api/instruments/{id}/rule` | 换规则并开始重算 `{a,b,batch_delay}` |
| GET | `/api/instruments/{id}/jobs` · `/api/jobs/{id}` | 轮次与进度 |
| POST | `/api/jobs/{id}/retry` | 失败轮重试 |
| POST/ PATCH/ DELETE | `/api/labs`、`/api/labs/{id}` | 检验增改删（触发器即时维护标准值） |
| GET | `/api/trials/{id}/labs` | 按试验查看原值/标准值/规则（同快照） |
| GET | `/api/trials` | 试验列表 |
| POST | `/api/batches` | 快照导出批次 `{trial_id, slow_seconds, kill_reader_after}` |
| GET | `/api/batches/current`、`/api/batches/{id}`、`/.../cancel`、`/.../download` | 批次管理 |
| POST | `/api/mutator/start` · `/stop`，GET `/api/mutator` | 源端写入模拟 |
| GET | `/api/connections` | normalizer/export/mutator 后端连接 |

### 快照导出原理（exporter.py，保留原有能力并扩展）

1. keeper 连接 `BEGIN ISOLATION LEVEL REPEATABLE READ` 后
   `pg_export_snapshot()` 导出快照并保持事务打开；
2. 三条独立 reader 连接各自 `SET TRANSACTION SNAPSHOT` 到同一快照，并行读
   patients / labs（含标准值）/ instruments 规则，基于 MVCC 不锁源表；
3. 三份 CSV 完整写出后才打包 ZIP，状态 `ready` 才开放下载；取消即时受理，
   后台 `pg_cancel_backend` + 客户端 terminate 兜底（`_kill_reader` 双保险）。

## 本地开发（不用 Docker）

```bash
pip install -r app/requirements.txt
# 准备 PostgreSQL 16 并执行 db/init.sql 后：
cd app
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/clinical \
  uvicorn main:app --port 8000
```

## 自测

端到端自测覆盖：换规则全流程与四舍五入、成功前旧规则供数、每仪器单轮 409、
慢重算期间独立连接增改删/变更仪器号后切换不漏不覆盖、检验 API、按试验查看、
三 CSV 快照导出、真实故障触发的失败轮与重试、重启 recover、advisory 锁挡写语义。

- 正常环境下 `docker compose up --build` 后即可在网页逐项操作；
- 本次交付在本地 PostgreSQL 16 + uvicorn 上运行，**50 项断言全部通过**
  （1 项依赖进程信号的故障注入在受限沙箱内无法触发，记为 SKIP，非产品缺陷）。

## 目录

```
app/main.py        FastAPI 路由（标准化、检验维护、试验视图、导出、模拟）
app/normalizer.py  重算轮次：分批暂存、对账、单事务原子切换、失败/重启恢复
app/exporter.py    快照导出：同一快照三连接并行、取消、状态机
app/mutator.py     源端跨表事务更正模拟（含随机仪器号变更）
app/static/        浏览器页面
db/init.sql        建表、触发器、合成数据与初始标准值
docker-compose.yml / app/Dockerfile
```
