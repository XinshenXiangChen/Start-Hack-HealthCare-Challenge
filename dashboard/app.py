#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import sys
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles


REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = REPO_ROOT / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import standardize as std  # type: ignore  # noqa: E402


SUPPORTED_EXT = {".csv", ".xlsx", ".pdf", ".sql"}
SCHEMA_SQL = REPO_ROOT / "database" / "sqlserver" / "CreateImportTables.sql"

INPUT_DIR = Path(os.getenv("PIPELINE_INPUT_DIR", str(REPO_ROOT / "runtime" / "incoming")))
OUTPUT_DIR = Path(
    os.getenv("PIPELINE_OUTPUT_DIR", str(REPO_ROOT / "runtime" / "standardized"))
)
ARCHIVE_DIR = Path(
    os.getenv("PIPELINE_ARCHIVE_DIR", str(REPO_ROOT / "runtime" / "processed"))
)
MODEL = os.getenv("PIPELINE_MODEL", "llama3.2:latest")
LLM_TIMEOUT = int(os.getenv("PIPELINE_LLM_TIMEOUT", "45"))
NO_LLM = os.getenv("PIPELINE_NO_LLM", "0").strip().lower() in {"1", "true", "yes"}
SCAN_SECONDS = int(os.getenv("PIPELINE_SCAN_SECONDS", "5"))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_name(name: str) -> str:
    return Path(name).name.replace(" ", "_")


def signature(path: Path) -> str:
    st = path.stat()
    return f"{path.resolve()}::{st.st_size}::{st.st_mtime_ns}"


@dataclass
class Job:
    id: str
    status: str
    source: str
    input_file: str
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    output_file: str | None = None
    table: str | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    mapped_columns: int | None = None
    semantic_extraction_used: bool = False
    semantic_rows_enriched: int = 0
    profile: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class State:
    def __init__(self) -> None:
        self.schema = std.parse_schema(SCHEMA_SQL)
        self.jobs: list[Job] = []
        self.jobs_by_id: dict[str, Job] = {}
        self.seen_signatures: set[str] = set()
        self.running_signatures: set[str] = set()
        self.lock = asyncio.Lock()
        self.scanner_task: asyncio.Task[Any] | None = None

    async def add_job(self, job: Job) -> None:
        async with self.lock:
            self.jobs.append(job)
            self.jobs_by_id[job.id] = job

    async def update_job(self, job: Job) -> None:
        async with self.lock:
            self.jobs_by_id[job.id] = job

    async def list_jobs(self, limit: int = 200) -> list[dict[str, Any]]:
        async with self.lock:
            data = [asdict(j) for j in reversed(self.jobs[-limit:])]
        return data

    async def summary(self) -> dict[str, Any]:
        async with self.lock:
            total = len(self.jobs)
            ok = sum(1 for j in self.jobs if j.status == "ok")
            err = sum(1 for j in self.jobs if j.status == "error")
            running = sum(1 for j in self.jobs if j.status == "running")
        return {
            "total_jobs": total,
            "ok_jobs": ok,
            "error_jobs": err,
            "running_jobs": running,
            "input_dir": str(INPUT_DIR),
            "output_dir": str(OUTPUT_DIR),
            "archive_dir": str(ARCHIVE_DIR),
            "model": MODEL,
            "llm_enabled": not NO_LLM,
            "scan_seconds": SCAN_SECONDS,
        }


state = State()


def ensure_dirs() -> None:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


def detect_table_with_fallback(
    path: Path,
    headers: list[str],
    records: list[dict[str, Any]],
    profile: dict[str, Any],
) -> str | None:
    table = std.detect_table(path, headers, profile)
    if not table:
        table = std.detect_table_from_records(records)
    if not table and not NO_LLM:
        allowed_tables = [t for t in state.schema.keys() if t in std.TABLE_HINTS]
        table = std.run_llama_table_detection(
            model=MODEL,
            path=path,
            headers=headers,
            records=records,
            allowed_tables=allowed_tables or list(state.schema.keys()),
            timeout_s=LLM_TIMEOUT,
        )
    return table


async def process_file(path: Path, source: str) -> None:
    job = Job(
        id=uuid.uuid4().hex[:12],
        status="running",
        source=source,
        input_file=str(path),
        created_at=utc_now(),
        started_at=utc_now(),
    )
    await state.add_job(job)
    try:
        headers_probe, records_probe, profile_probe = std.load_records(path, "tbImportAcData")
        table = detect_table_with_fallback(path, headers_probe, records_probe, profile_probe)
        if not table or table not in state.schema:
            raise ValueError("table_not_detected_or_not_in_schema")

        headers, records, profile = std.load_records(path, table)
        target_cols = [c for c in state.schema[table] if c != "coId"]
        mapping = std.build_mapping(
            table=table,
            headers=headers,
            target_cols=target_cols,
            use_llm=not NO_LLM,
            model=MODEL,
            timeout_s=LLM_TIMEOUT,
            prompt_template=(
                (REPO_ROOT / "pipeline" / "prompts" / "column_mapper_prompt.txt")
                .read_text(encoding="utf-8")
            ),
        )
        rows = std.standardize_records(records, target_cols, mapping)

        semantic_rows_enriched = 0
        semantic_extraction_used = False
        if not NO_LLM and std.should_use_semantic_extraction(headers, records, mapping):
            rows, semantic_rows_enriched = std.enrich_rows_with_semantic_extraction(
                table=table,
                headers=headers,
                records=records,
                target_cols=target_cols,
                rows=rows,
                model=MODEL,
                timeout_s=LLM_TIMEOUT,
            )
            semantic_extraction_used = True

        ext = path.suffix.lower().lstrip(".")
        output_name = f"{path.stem}__{ext}__{table}__{job.id}.csv"
        output_path = OUTPUT_DIR / output_name
        std.write_csv(output_path, rows, target_cols)

        archived_name = f"{path.stem}__{job.id}{path.suffix.lower()}"
        archived_path = ARCHIVE_DIR / archived_name
        try:
            shutil.move(str(path), str(archived_path))
        except Exception:
            pass

        job.status = "ok"
        job.output_file = str(output_path)
        job.table = table
        job.rows_in = len(records)
        job.rows_out = len(rows)
        job.mapped_columns = len(mapping)
        job.semantic_extraction_used = semantic_extraction_used
        job.semantic_rows_enriched = semantic_rows_enriched
        job.profile = profile
        job.finished_at = utc_now()
        await state.update_job(job)
    except Exception as exc:
        job.status = "error"
        job.error = str(exc)
        job.finished_at = utc_now()
        await state.update_job(job)


def schedule_processing(path: Path, source: str) -> bool:
    try:
        sig = signature(path)
    except FileNotFoundError:
        return False
    if sig in state.seen_signatures or sig in state.running_signatures:
        return False
    state.running_signatures.add(sig)

    async def _run(p: Path, s: str, sig_value: str) -> None:
        try:
            await process_file(p, s)
        finally:
            state.running_signatures.discard(sig_value)
            state.seen_signatures.add(sig_value)

    asyncio.create_task(_run(path, source, sig))
    return True


async def scanner_loop() -> None:
    while True:
        try:
            for path in sorted(INPUT_DIR.rglob("*")):
                if not path.is_file() or path.suffix.lower() not in SUPPORTED_EXT:
                    continue
                schedule_processing(path, "watcher")
        except Exception:
            pass
        await asyncio.sleep(max(1, SCAN_SECONDS))


app = FastAPI(title="Healthcare Standardization Dashboard", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(REPO_ROOT / "dashboard" / "static")), name="static")


@app.on_event("startup")
async def on_startup() -> None:
    ensure_dirs()
    if state.scanner_task is None:
        state.scanner_task = asyncio.create_task(scanner_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if state.scanner_task:
        state.scanner_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await state.scanner_task


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html = (REPO_ROOT / "dashboard" / "static" / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


@app.get("/api/summary")
async def api_summary() -> dict[str, Any]:
    return await state.summary()


@app.get("/api/jobs")
async def api_jobs(limit: int = 200) -> dict[str, Any]:
    jobs = await state.list_jobs(limit=max(1, min(limit, 1000)))
    return {"jobs": jobs}


@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)) -> dict[str, Any]:
    ext = Path(file.filename or "").suffix.lower()
    if ext not in SUPPORTED_EXT:
        raise HTTPException(status_code=400, detail=f"unsupported extension: {ext}")
    ensure_dirs()
    dst = INPUT_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{safe_name(file.filename or 'upload')}"
    with dst.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    scheduled = schedule_processing(dst, "upload")
    return {"status": "accepted", "saved_to": str(dst), "scheduled_immediately": scheduled}


@app.get("/api/download/{job_id}")
async def api_download(job_id: str) -> FileResponse:
    job = state.jobs_by_id.get(job_id)
    if not job or not job.output_file:
        raise HTTPException(status_code=404, detail="job/output not found")
    path = Path(job.output_file)
    if not path.exists():
        raise HTTPException(status_code=404, detail="output file missing")
    return FileResponse(path=str(path), filename=path.name, media_type="text/csv")
