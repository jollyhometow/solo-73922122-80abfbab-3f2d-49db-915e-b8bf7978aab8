"""仪器标准化重算管理器。

口径：standard_value = round(a * 原测值 + b, 4)，原测值永不修改。

一致性设计
----------
1. 换规则不在原表上直接改：后台任务把新规则结果分批写入暂存表 norm_job_lab，
   成功前所有查询仍走 instruments.active_* 与 lab_standard 中的旧规则结果；
2. 重算分批提交、每批独立短事务，不持长锁，慢重算不阻塞查询与无关仪器操作；
3. 重算期间任何连接对 labs 的增改删都由行触发器按当前生效规则维护 lab_standard，
   并对该仪器加 advisory xact lock；最终切换事务先取同一把锁，因此：
   - 切换开始前已提交的写全部可见，不会漏新增/恢复已删/覆盖新测值；
   - 只有切换这一个事务运行的短暂时间内，该仪器的写被挡下；
4. 切换在单个事务内完成「暂存对账 → 发布 lab_standard → 翻转 instruments 规则」，
   任何一次供数看到的原值、规则、标准值都属于同一个已提交状态；
5. 失败只把轮次标记 failed 并清理暂存，旧规则与现有标准值完全不受影响；
   重启时把残留 running 轮次置 failed，失败轮可重试。

每仪器同一时刻只允许一轮（数据库部分唯一索引 + 进程内锁双保险）。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal

import asyncpg

# 与 db/init.sql 触发器中 labs_standard_maintain() 使用的常量保持一致。
ADV_LOCK_NS = 910017
BATCH_SIZE = 2000
MAX_PASSES = 3

RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"


@dataclass
class JobView:
    row: dict
    task: asyncio.Task | None = None
    started_wall: float | None = None


class Normalizer:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._locks: dict[str, asyncio.Lock] = {}
        self._tasks: dict[int, asyncio.Task] = {}

    def _inst_lock(self, instrument_id: str) -> asyncio.Lock:
        lock = self._locks.get(instrument_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[instrument_id] = lock
        return lock

    # ------------------------------------------------------------ 启动恢复

    async def recover(self) -> list[dict]:
        """进程重启后：把残留 running 轮次置 failed，并清掉其暂存数据。

        旧规则与 lab_standard 从未被未完成轮次修改，全部继续可用。
        """
        conn = await asyncpg.connect(
            self.dsn,
            server_settings={
                "application_name": "normalizer-recover",
                "max_parallel_workers_per_gather": "0",
            },
        )
        try:
            rows = await conn.fetch(
                "SELECT job_id, instrument_id FROM norm_jobs WHERE status = $1",
                RUNNING,
            )
            recovered = [dict(r) for r in rows]
            for r in rows:
                await conn.execute(
                    "DELETE FROM norm_job_lab WHERE job_id = $1", r["job_id"]
                )
                await conn.execute(
                    "UPDATE norm_jobs SET status = $2, finished_at = now(), "
                    "error = 'interrupted by application restart' "
                    "WHERE job_id = $1",
                    r["job_id"], FAILED,
                )
            return recovered
        finally:
            await conn.close()

    # ------------------------------------------------------------ 开轮次

    async def start_job(
        self,
        instrument_id: str,
        a: Decimal,
        b: Decimal,
        batch_delay: float = 0.0,
    ) -> dict:
        """为仪器创建一轮重算；已有 running 轮次或仪器不存在时抛异常。"""
        lock = self._inst_lock(instrument_id)
        if lock.locked():
            raise JobConflict(f"instrument {instrument_id!r} already has a running job")

        conn = await asyncpg.connect(
            self.dsn,
            server_settings={
                "application_name": f"normalizer-ctl-{instrument_id}",
                "max_parallel_workers_per_gather": "0",
            },
        )
        try:
            inst = await conn.fetchrow(
                "SELECT instrument_id, active_a, active_b FROM instruments "
                "WHERE instrument_id = $1",
                instrument_id,
            )
            if inst is None:
                raise UnknownInstrument(f"unknown instrument_id: {instrument_id!r}")
            if a == inst["active_a"] and b == inst["active_b"]:
                raise SameRule("new rule equals the active rule; nothing to recalculate")

            total = await conn.fetchval(
                "SELECT count(*) FROM labs WHERE instrument_id = $1", instrument_id
            )
            try:
                row = await conn.fetchrow(
                    "INSERT INTO norm_jobs "
                    "(instrument_id, pending_a, pending_b, batch_delay, status, "
                    " total_labs, started_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6, now()) RETURNING *",
                    instrument_id, a, b, batch_delay, RUNNING, total,
                )
            except asyncpg.UniqueViolationError:
                # 部分唯一索引 norm_jobs_one_running 兜底
                raise JobConflict(
                    f"instrument {instrument_id!r} already has a running job"
                )
        finally:
            await conn.close()

        job = dict(row)
        task = asyncio.create_task(self._run(job["job_id"]))
        self._tasks[job["job_id"]] = task
        return job

    async def retry_job(self, job_id: int) -> dict:
        ctl = await asyncpg.connect(
            self.dsn,
            server_settings={
                "application_name": "normalizer-ctl-retry",
                "max_parallel_workers_per_gather": "0",
            },
        )
        try:
            failed = await ctl.fetchrow(
                "SELECT * FROM norm_jobs WHERE job_id = $1", job_id
            )
            if failed is None:
                raise JobNotFound(f"job {job_id} not found")
            if failed["status"] != FAILED:
                raise JobConflict(f"job {job_id} is {failed['status']}, not failed")
            running = await ctl.fetchval(
                "SELECT 1 FROM norm_jobs WHERE instrument_id = $1 AND status = $2 LIMIT 1",
                failed["instrument_id"], RUNNING,
            )
            if running:
                raise JobConflict(
                    f"instrument {failed['instrument_id']!r} already has a running job"
                )
            row = await ctl.fetchrow(
                "INSERT INTO norm_jobs "
                "(instrument_id, pending_a, pending_b, batch_delay, status, "
                " total_labs, started_at) "
                "SELECT instrument_id, pending_a, pending_b, batch_delay, $2, "
                "(SELECT count(*) FROM labs WHERE instrument_id = norm_jobs.instrument_id), "
                "now() FROM norm_jobs WHERE job_id = $1 RETURNING *",
                job_id, RUNNING,
            )
        finally:
            await ctl.close()
        task = asyncio.create_task(self._run(row["job_id"]))
        self._tasks[row["job_id"]] = task
        return dict(row)

    async def shutdown(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    # ------------------------------------------------------------ 后台重算

    async def _run(self, job_id: int) -> None:
        conn = await asyncpg.connect(
            self.dsn,
            server_settings={
                "application_name": f"normalizer-job-{job_id}",
                "max_parallel_workers_per_gather": "0",
            },
        )
        try:
            job = await conn.fetchrow("SELECT * FROM norm_jobs WHERE job_id = $1", job_id)
            inst_id, a, b = job["instrument_id"], job["pending_a"], job["pending_b"]
            delay = float(job["batch_delay"])

            # 多轮扫描：第一轮全量，之后只补扫描期间被改动/新增的检验。
            for pass_no in range(1, MAX_PASSES + 1):
                await self._scan_pass(conn, job_id, inst_id, a, b, pass_no, delay)
                stale, extra = await self._diff_counts(conn, job_id, inst_id)
                if stale == 0 and extra == 0:
                    break
            await self._publish(conn, job_id, inst_id, a, b)
        except asyncpg.PostgresError as exc:
            await self._mark_failed(job_id, f"{type(exc).__name__}: {exc}")
        except Exception as exc:  # 连接断开等：保留旧规则，轮次可重试
            await self._mark_failed(job_id, f"{type(exc).__name__}: {exc}")
        finally:
            try:
                await conn.close()
            except Exception:
                pass
            self._tasks.pop(job_id, None)

    async def _scan_pass(
        self, conn, job_id: int, inst_id: str, a: Decimal, b: Decimal,
        pass_no: int, delay: float,
    ) -> None:
        await conn.execute(
            "UPDATE norm_jobs SET pass_no = $2 WHERE job_id = $1", job_id, pass_no
        )
        cursor = 0
        while True:
            # 每批一个独立短事务：不持长锁，写入方不被重算阻塞。
            async with conn.transaction():
                rows = await conn.fetch(
                    "SELECT l.lab_id, l.value, l.instrument_id "
                    "FROM labs l "
                    "WHERE l.instrument_id = $1 AND l.lab_id > $2 "
                    "ORDER BY l.lab_id LIMIT $3",
                    inst_id, cursor, BATCH_SIZE,
                )
                if rows:
                    await conn.executemany(
                        "INSERT INTO norm_job_lab "
                        "(job_id, lab_id, instrument_id, raw_value, standard_value) "
                        "VALUES ($1, $2, $3, $4, "
                        "round(($5)::numeric * ($4)::numeric + ($6)::numeric, 4)) "
                        "ON CONFLICT (job_id, lab_id) DO UPDATE SET "
                        "instrument_id = EXCLUDED.instrument_id, "
                        "raw_value = EXCLUDED.raw_value, "
                        "standard_value = EXCLUDED.standard_value",
                        [
                            (job_id, r["lab_id"], r["instrument_id"], r["value"], a, b)
                            for r in rows
                        ],
                    )
                    cursor = rows[-1]["lab_id"]
                done = await conn.fetchval(
                    "SELECT count(*) FROM norm_job_lab WHERE job_id = $1", job_id
                )
                await conn.execute(
                    "UPDATE norm_jobs SET done_labs = $2 WHERE job_id = $1",
                    job_id, done,
                )
            if not rows:
                break
            if delay > 0:
                # 模拟慢重算：休眠在事务之外，不占任何锁。
                await asyncio.sleep(delay)

    async def _diff_counts(self, conn, job_id: int, inst_id: str) -> tuple[int, int]:
        """暂存 vs 当前已提交 labs 的差异：缺失/过期数、多余数。"""
        row = await conn.fetchrow(
            """
            WITH cur AS (
                SELECT lab_id, value FROM labs WHERE instrument_id = $2
            ),
            stg AS (
                SELECT lab_id, raw_value FROM norm_job_lab WHERE job_id = $1
            )
            SELECT
              (SELECT count(*) FROM cur
                 LEFT JOIN stg USING (lab_id)
                 WHERE stg.lab_id IS NULL
                    OR stg.raw_value IS DISTINCT FROM cur.value) AS stale,
              (SELECT count(*) FROM stg s
                 FULL JOIN cur c USING (lab_id)
                 WHERE c.lab_id IS NULL) AS extra
            """,
            job_id, inst_id,
        )
        return row["stale"], row["extra"]

    async def _publish(
        self, conn, job_id: int, inst_id: str, a: Decimal, b: Decimal
    ) -> None:
        """最终切换：单事务内对账 + 发布 + 规则翻转，全程持有仪器写锁。"""
        async with conn.transaction():
            # 与行触发器同一把 advisory lock：只挡该仪器的写，且仅本事务期间。
            await conn.execute(
                "SELECT pg_advisory_xact_lock(($1)::int, hashtext($2))",
                ADV_LOCK_NS, inst_id,
            )
            job = await conn.fetchrow(
                "SELECT * FROM norm_jobs WHERE job_id = $1 FOR UPDATE", job_id
            )
            if job is None or job["status"] != RUNNING:
                raise asyncpg.PostgresError(f"job {job_id} is no longer running")

            # 1) 对账：把切换前最后一刻仍缺失/过期的行按当前原值补齐。
            await conn.execute(
                """
                INSERT INTO norm_job_lab
                    (job_id, lab_id, instrument_id, raw_value, standard_value)
                SELECT $1, l.lab_id, l.instrument_id, l.value,
                       round(($2)::numeric * l.value + ($3)::numeric, 4)
                FROM labs l
                WHERE l.instrument_id = $4
                ON CONFLICT (job_id, lab_id) DO UPDATE SET
                    instrument_id = EXCLUDED.instrument_id,
                    raw_value = EXCLUDED.raw_value,
                    standard_value = EXCLUDED.standard_value
                WHERE norm_job_lab.raw_value IS DISTINCT FROM EXCLUDED.raw_value
                   OR norm_job_lab.instrument_id IS DISTINCT FROM EXCLUDED.instrument_id
                """,
                job_id, a, b, inst_id,
            )
            # 2) 删除已不存在（检验被删或仪器号被改走）的暂存行。
            await conn.execute(
                "DELETE FROM norm_job_lab s WHERE s.job_id = $1 AND NOT EXISTS ("
                "SELECT 1 FROM labs l "
                "WHERE l.lab_id = s.lab_id AND l.instrument_id = $2)",
                job_id, inst_id,
            )
            # 3) 强校验：暂存集必须与当前仪器检验集一一对应。
            cur_count = await conn.fetchval(
                "SELECT count(*) FROM labs WHERE instrument_id = $1", inst_id
            )
            stg_count = await conn.fetchval(
                "SELECT count(*) FROM norm_job_lab WHERE job_id = $1", job_id
            )
            if cur_count != stg_count:
                raise asyncpg.PostgresError(
                    f"staging mismatch before switch: cur={cur_count} stg={stg_count}"
                )

            # 4) 发布标准值（与规则翻转同一事务，外部不可能看到半成品）。
            new_version = await conn.fetchval(
                "SELECT rule_version + 1 FROM instruments "
                "WHERE instrument_id = $1 FOR UPDATE",
                inst_id,
            )
            await conn.execute(
                """
                INSERT INTO lab_standard
                    (lab_id, instrument_id, standard_value, rule_version, a, b, updated_at)
                SELECT s.lab_id, s.instrument_id, s.standard_value, $2, $3, $4, now()
                FROM norm_job_lab s WHERE s.job_id = $1
                ON CONFLICT (lab_id) DO UPDATE SET
                    instrument_id = EXCLUDED.instrument_id,
                    standard_value = EXCLUDED.standard_value,
                    rule_version = EXCLUDED.rule_version,
                    a = EXCLUDED.a, b = EXCLUDED.b, updated_at = now()
                """,
                job_id, new_version, a, b,
            )
            await conn.execute(
                "UPDATE instruments SET active_a = $2, active_b = $3, "
                "rule_version = $4, rule_updated_at = now() "
                "WHERE instrument_id = $1",
                inst_id, a, b, new_version,
            )
            await conn.execute(
                "UPDATE norm_jobs SET status = $2, finished_at = now(), "
                "done_labs = $3, pass_no = pass_no WHERE job_id = $1",
                job_id, SUCCEEDED, stg_count,
            )
            await conn.execute(
                "DELETE FROM norm_job_lab WHERE job_id = $1", job_id
            )
        # 事务提交即释放 advisory lock，被挡下的写随后按新规则由触发器维护。

    async def _mark_failed(self, job_id: int, error: str) -> None:
        try:
            ctl = await asyncpg.connect(
                self.dsn,
                server_settings={
                    "application_name": f"normalizer-fail-{job_id}",
                    "max_parallel_workers_per_gather": "0",
                },
            )
            try:
                await ctl.execute(
                    "DELETE FROM norm_job_lab WHERE job_id = $1", job_id
                )
                await ctl.execute(
                    "UPDATE norm_jobs SET status = $2, error = $3, "
                    "finished_at = now() WHERE job_id = $1 AND status = $4",
                    job_id, FAILED, error, RUNNING,
                )
            finally:
                await ctl.close()
        except Exception:
            pass  # 重启恢复（recover）会兜底处理残留 running 轮次


class UnknownInstrument(Exception):
    """仪器未注册。"""


class JobConflict(Exception):
    """该仪器已有运行中的轮次，或轮次状态不允许当前操作。"""


class JobNotFound(Exception):
    """轮次不存在。"""


class SameRule(Exception):
    """新规则与生效规则完全相同。"""
