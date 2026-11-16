"""FastAPI 入口：仪器检验值标准化 + 临床试验数据离线导出。

浏览器入口：GET / ；API 见 /docs。
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path

import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from exporter import READY, BatchManager
from mutator import Mutator
from normalizer import (
    JobConflict,
    JobNotFound,
    Normalizer,
    SameRule,
    UnknownInstrument,
)

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/clinical"
)
EXPORT_DIR = os.environ.get("EXPORT_DIR", "./exports")

manager = BatchManager(DATABASE_URL, EXPORT_DIR)
mutator = Mutator(DATABASE_URL)
normalizer = Normalizer(DATABASE_URL)
pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    manager.prepare_dir()
    recovered = await normalizer.recover()
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=8,
        server_settings={"max_parallel_workers_per_gather": "0"},
    )
    yield
    await mutator.stop()
    await normalizer.shutdown()
    await manager.shutdown()
    await pool.close()


app = FastAPI(title="仪器检验值标准化与离线导出", lifespan=lifespan)


def dec4(x: Decimal) -> float:
    return float(x.quantize(Decimal("0.0001")))


def instrument_public(row) -> dict:
    return {
        "instrument_id": row["instrument_id"],
        "active_a": float(row["active_a"]),
        "active_b": float(row["active_b"]),
        "rule_version": row["rule_version"],
        "rule_updated_at": row["rule_updated_at"].isoformat(),
    }


def job_public(row) -> dict:
    return {
        "job_id": row["job_id"],
        "instrument_id": row["instrument_id"],
        "pending_a": float(row["pending_a"]),
        "pending_b": float(row["pending_b"]),
        "batch_delay": float(row["batch_delay"]),
        "status": row["status"],
        "pass_no": row["pass_no"],
        "total_labs": row["total_labs"],
        "done_labs": row["done_labs"],
        "error": row["error"],
        "created_at": row["created_at"].isoformat(),
        "started_at": row["started_at"].isoformat() if row["started_at"] else None,
        "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
    }


# ============================================================ 标准化 API

class RuleRequest(BaseModel):
    a: Decimal = Field(..., description="新规则系数 a（十进制）")
    b: Decimal = Field(0, description="新规则截距 b（十进制）")
    batch_delay: float = Field(
        0, ge=0, le=5, description="演示用：每批休眠秒数，模拟慢重算（不占锁）"
    )


@app.get("/api/instruments")
async def list_instruments():
    """仪器列表：生效规则、当前检验数、最近一轮重算进度。"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT i.*,
                   (SELECT count(*) FROM labs l
                     WHERE l.instrument_id = i.instrument_id) AS lab_count,
                   (SELECT count(*) FROM lab_standard s
                     WHERE s.instrument_id = i.instrument_id) AS standard_count,
                   j.job_id AS last_job_id
            FROM instruments i
            LEFT JOIN LATERAL (
                SELECT job_id FROM norm_jobs
                WHERE instrument_id = i.instrument_id
                ORDER BY job_id DESC LIMIT 1
            ) j ON true
            ORDER BY i.instrument_id
            """
        )
    out = []
    for r in rows:
        item = instrument_public(r)
        item["lab_count"] = r["lab_count"]
        item["standard_count"] = r["standard_count"]
        item["last_job_id"] = r["last_job_id"]
        out.append(item)
    return out


@app.post("/api/instruments/{instrument_id}/rule", status_code=201)
async def change_rule(instrument_id: str, req: RuleRequest):
    """换规则：创建该仪器的一轮重算（每仪器同时只受理一轮）。"""
    try:
        job = await normalizer.start_job(
            instrument_id, req.a, req.b, req.batch_delay
        )
    except UnknownInstrument as exc:
        raise HTTPException(404, str(exc))
    except SameRule as exc:
        raise HTTPException(400, str(exc))
    except JobConflict as exc:
        raise HTTPException(409, str(exc))
    return job_public(job)


@app.get("/api/instruments/{instrument_id}/jobs")
async def list_jobs(instrument_id: str, limit: int = Query(10, ge=1, le=50)):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM norm_jobs WHERE instrument_id = $1 "
            "ORDER BY job_id DESC LIMIT $2",
            instrument_id, limit,
        )
    return [job_public(r) for r in rows]


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM norm_jobs WHERE job_id = $1", job_id)
    if row is None:
        raise HTTPException(404, "job not found")
    return job_public(row)


@app.post("/api/jobs/{job_id}/retry", status_code=201)
async def retry_job(job_id: int):
    """失败轮重试：沿用原待生效规则，创建新轮次。"""
    try:
        job = await normalizer.retry_job(job_id)
    except JobNotFound as exc:
        raise HTTPException(404, str(exc))
    except JobConflict as exc:
        raise HTTPException(409, str(exc))
    return job_public(job)


# ============================================================ 检验维护 API
# 重算期间这些写入走独立连接，由数据库触发器按生效规则维护标准值。

class LabCreate(BaseModel):
    patient_id: int
    value: Decimal = Field(..., ge=Decimal("-99999999"), le=Decimal("99999999"))
    instrument_id: str | None = None


class LabUpdate(BaseModel):
    value: Decimal | None = Field(
        None, ge=Decimal("-99999999"), le=Decimal("99999999")
    )
    instrument_id: str | None = None
    clear_instrument: bool = False


@app.post("/api/labs", status_code=201)
async def create_lab(req: LabCreate):
    lab_id = None
    async with pool.acquire() as conn:
        try:
            lab_id = await conn.fetchval("SELECT nextval('lab_id_seq')")
            row = await conn.fetchrow(
                "INSERT INTO labs (lab_id, patient_id, value, instrument_id) "
                "VALUES ($1, $2, $3, $4) "
                "RETURNING lab_id, patient_id, value, instrument_id",
                lab_id, req.patient_id, req.value, req.instrument_id,
            )
        except asyncpg.ForeignKeyViolationError:
            raise HTTPException(400, f"patient_id {req.patient_id} does not exist")
        except asyncpg.NotNullViolationError:
            raise HTTPException(400, "invalid lab payload")
        std = await conn.fetchrow(
            "SELECT standard_value, rule_version, a, b FROM lab_standard "
            "WHERE lab_id = $1", lab_id
        )
    out = dict(row)
    out["standard"] = None if std is None else {
        "standard_value": dec4(std["standard_value"]),
        "rule_version": std["rule_version"],
        "a": float(std["a"]),
        "b": float(std["b"]),
    }
    return out


@app.patch("/api/labs/{lab_id}")
async def update_lab(lab_id: int, req: LabUpdate):
    sets, args = [], []
    if req.value is not None:
        args.append(req.value)
        sets.append(f"value = ${len(args)}")
    if req.clear_instrument:
        sets.append("instrument_id = NULL")
    elif req.instrument_id is not None:
        args.append(req.instrument_id)
        sets.append(f"instrument_id = ${len(args)}")
    if not sets:
        raise HTTPException(400, "nothing to update")
    args.append(lab_id)
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                f"UPDATE labs SET {', '.join(sets)} "
                f"WHERE lab_id = ${len(args)} "
                "RETURNING lab_id, patient_id, value, instrument_id",
                *args,
            )
        except asyncpg.ForeignKeyViolationError:
            raise HTTPException(400, "foreign key violation")
        if row is None:
            raise HTTPException(404, "lab not found")
        std = await conn.fetchrow(
            "SELECT standard_value, rule_version, a, b FROM lab_standard "
            "WHERE lab_id = $1", lab_id
        )
    out = dict(row)
    out["standard"] = None if std is None else {
        "standard_value": dec4(std["standard_value"]),
        "rule_version": std["rule_version"],
        "a": float(std["a"]),
        "b": float(std["b"]),
    }
    return out


@app.delete("/api/labs/{lab_id}", status_code=204)
async def delete_lab(lab_id: int):
    async with pool.acquire() as conn:
        tag = await conn.execute("DELETE FROM labs WHERE lab_id = $1", lab_id)
    # asyncpg 返回形如 "DELETE 1" 的命令标签
    if int(tag.split()[-1]) == 0:
        raise HTTPException(404, "lab not found")


# ============================================================ 按试验查看

@app.get("/api/trials/{trial_id}/labs")
async def trial_labs(
    trial_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """按试验查看原值/标准值/规则。

    单条 REPEATABLE READ 只读事务：返回的原值、规则、标准值取自同一快照、
    同一已提交状态。
    """
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            total = await conn.fetchval(
                "SELECT count(*) FROM patients WHERE trial_id = $1", trial_id
            )
            if total == 0:
                raise HTTPException(404, f"unknown trial_id: {trial_id!r}")
            rows = await conn.fetch(
                """
                SELECT l.lab_id, l.patient_id, l.value AS raw_value,
                       l.instrument_id,
                       ls.standard_value, ls.rule_version, ls.a, ls.b
                FROM labs l
                JOIN patients p ON p.patient_id = l.patient_id
                LEFT JOIN lab_standard ls ON ls.lab_id = l.lab_id
                WHERE p.trial_id = $1
                ORDER BY l.lab_id
                LIMIT $2 OFFSET $3
                """,
                trial_id, limit, offset,
            )
    items = []
    for r in rows:
        items.append({
            "lab_id": r["lab_id"],
            "patient_id": r["patient_id"],
            "raw_value": float(r["raw_value"]),
            "instrument_id": r["instrument_id"],
            "standard_value": (
                dec4(r["standard_value"]) if r["standard_value"] is not None else None
            ),
            "rule_version": r["rule_version"],
            "a": float(r["a"]) if r["a"] is not None else None,
            "b": float(r["b"]) if r["b"] is not None else None,
        })
    return {"trial_id": trial_id, "limit": limit, "offset": offset, "items": items}


# ============================================================ 批次导出

class StartRequest(BaseModel):
    trial_id: str
    slow_seconds: float = Field(0, ge=0, le=120, description="模拟慢查询秒数")
    kill_reader_after: float | None = Field(
        None, ge=0.5, le=120, description="N 秒后断开 labs 读连接（故障注入）"
    )


@app.get("/api/trials")
async def trials():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT trial_id, count(*) AS patients FROM patients "
            "GROUP BY trial_id ORDER BY trial_id"
        )
    return [{"trial_id": r["trial_id"], "patients": r["patients"]} for r in rows]


@app.post("/api/batches", status_code=201)
async def start_batch(req: StartRequest):
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM patients WHERE trial_id = $1 LIMIT 1", req.trial_id
        )
    if not exists:
        raise HTTPException(404, f"unknown trial_id: {req.trial_id!r}")
    # 若仍有运行中的批次，start() 内部会先取消它再开新批次
    batch = await manager.start(req.trial_id, req.slow_seconds, req.kill_reader_after)
    return batch.public()


@app.get("/api/batches")
async def list_batches():
    items = sorted(manager.batches.values(), key=lambda b: b.created_at, reverse=True)
    return [b.public() for b in items[:20]]


@app.get("/api/batches/current")
async def current_batch():
    batch = manager.current()
    return batch.public() if batch is not None else {"status": "idle"}


@app.get("/api/batches/{batch_id}")
async def get_batch(batch_id: str):
    batch = manager.batches.get(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    return batch.public()


@app.post("/api/batches/{batch_id}/cancel")
async def cancel_batch(batch_id: str):
    """取消须及时受理：立即返回 cancelled，不等待慢查询结束。"""
    batch = await manager.cancel(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    return batch.public()


@app.get("/api/batches/{batch_id}/download")
async def download_batch(batch_id: str):
    batch = manager.batches.get(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    if batch.status != READY or batch.zip_path is None or not batch.zip_path.exists():
        raise HTTPException(409, f"batch not ready for download (status={batch.status})")
    return FileResponse(
        batch.zip_path, filename=batch.zip_path.name, media_type="application/zip"
    )


@app.post("/api/mutator/start")
async def mutator_start():
    await mutator.start()
    return mutator.status()


@app.post("/api/mutator/stop")
async def mutator_stop():
    await mutator.stop()
    return mutator.status()


@app.get("/api/mutator")
async def mutator_status():
    return mutator.status()


@app.get("/api/connections")
async def connections():
    """标准化/导出/更正相关的数据库后端连接（观察连接释放与无锁状态）。"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT pid, application_name, state, wait_event_type,
                   (now() - xact_start)::text AS xact_age
            FROM pg_stat_activity
            WHERE application_name LIKE 'export-%'
               OR application_name LIKE 'normalizer-%'
               OR application_name = 'mutator'
            ORDER BY pid
            """
        )
    return [dict(r) for r in rows]


static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
