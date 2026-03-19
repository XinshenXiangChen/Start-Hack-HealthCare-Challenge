#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import csv
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
WORKER_CONCURRENCY = int(os.getenv("PIPELINE_WORKER_CONCURRENCY", "1"))
IID_DICTIONARY_ENV = (os.getenv("PIPELINE_IID_DICTIONARY") or "").strip()
IID_SEARCH_ROOT = Path(os.getenv("PIPELINE_IID_SEARCH_ROOT", str(REPO_ROOT.parent)))


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
    archived_input_file: str | None = None
    error: str | None = None


@dataclass
class QueueItem:
    path: Path
    source: str
    signature: str
    job_id: str


class State:
    def __init__(self) -> None:
        self.schema = std.parse_schema(SCHEMA_SQL)
        self.jobs: list[Job] = []
        self.jobs_by_id: dict[str, Job] = {}
        self.seen_signatures: set[str] = set()
        self.queued_signatures: set[str] = set()
        self.running_signatures: set[str] = set()
        self.queue: asyncio.Queue[QueueItem] = asyncio.Queue()
        self.lock = asyncio.Lock()
        self.scanner_task: asyncio.Task[Any] | None = None
        self.worker_tasks: list[asyncio.Task[Any]] = []

    async def add_job(self, job: Job) -> None:
        async with self.lock:
            self.jobs.append(job)
            self.jobs_by_id[job.id] = job

    async def get_job(self, job_id: str) -> Job | None:
        async with self.lock:
            return self.jobs_by_id.get(job_id)

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
            queued = sum(1 for j in self.jobs if j.status == "queued")
        return {
            "total_jobs": total,
            "ok_jobs": ok,
            "error_jobs": err,
            "running_jobs": running,
            "queued_jobs": queued,
            "queue_size": self.queue.qsize(),
            "worker_concurrency": max(1, WORKER_CONCURRENCY),
            "input_dir": str(INPUT_DIR),
            "output_dir": str(OUTPUT_DIR),
            "archive_dir": str(ARCHIVE_DIR),
            "model": MODEL,
            "llm_enabled": not NO_LLM,
            "scan_seconds": SCAN_SECONDS,
            "iid_dictionary": (
                std.get_iid_dictionary_source() if hasattr(std, "get_iid_dictionary_source") else None
            ),
        }

    async def reset_state(self) -> dict[str, int]:
        async with self.lock:
            running = sum(1 for j in self.jobs if j.status == "running")
            if running > 0:
                return {"running_jobs": running, "cleared_jobs": 0, "cleared_queue_items": 0}
            cleared_jobs = len(self.jobs)
            self.jobs.clear()
            self.jobs_by_id.clear()
            self.seen_signatures.clear()
            self.queued_signatures.clear()
            self.running_signatures.clear()

        cleared_q = 0
        while True:
            try:
                _ = self.queue.get_nowait()
                self.queue.task_done()
                cleared_q += 1
            except asyncio.QueueEmpty:
                break
        return {"running_jobs": 0, "cleared_jobs": cleared_jobs, "cleared_queue_items": cleared_q}


state = State()


def ensure_dirs() -> None:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


def purge_runtime_files() -> int:
    deleted = 0
    for base in (INPUT_DIR, OUTPUT_DIR, ARCHIVE_DIR):
        if not base.exists():
            continue
        for p in base.rglob("*"):
            if p.is_file():
                p.unlink(missing_ok=True)
                deleted += 1
    return deleted


def truncate_cell(value: Any, max_len: int = 220) -> str:
    text = "" if value is None else str(value)
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def preview_from_records(
    headers: list[str],
    records: list[dict[str, Any]],
    max_rows: int,
    max_cols: int,
) -> dict[str, Any]:
    shown_headers = headers[:max_cols]
    out_rows: list[list[str]] = []
    for rec in records[:max_rows]:
        out_rows.append([truncate_cell(rec.get(col, "")) for col in shown_headers])
    return {
        "headers": shown_headers,
        "rows": out_rows,
        "total_rows": len(records),
        "total_cols": len(headers),
        "shown_rows": len(out_rows),
        "shown_cols": len(shown_headers),
        "truncated_rows": len(records) > max_rows,
        "truncated_cols": len(headers) > max_cols,
    }


def load_output_csv_preview(path: Path, max_rows: int, max_cols: int) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    enc, delim = std.sniff_text_file(path)
    with path.open(encoding=enc, newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        headers = list(reader.fieldnames or [])
        records: list[dict[str, Any]] = []
        for row in reader:
            records.append(row)
    return preview_from_records(headers, records, max_rows, max_cols)


def resolve_job_input_path(job: Job) -> Path | None:
    candidates: list[Path] = []
    if job.archived_input_file:
        candidates.append(Path(job.archived_input_file))
    candidates.append(Path(job.input_file))
    for p in candidates:
        if p.exists():
            return p
    return None


def detect_table_with_fallback(
    path: Path,
    headers: list[str],
    records: list[dict[str, Any]],
    profile: dict[str, Any],
) -> str | None:
    table = std.detect_table(path, headers, profile)
    if not table and hasattr(std, "detect_table_from_records"):
        table = std.detect_table_from_records(records)
    if not table and not NO_LLM and hasattr(std, "run_llama_table_detection"):
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


async def process_file(path: Path, source: str, job: Job) -> None:
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
        if (
            not NO_LLM
            and hasattr(std, "should_use_semantic_extraction")
            and hasattr(std, "enrich_rows_with_semantic_extraction")
            and std.should_use_semantic_extraction(headers, records, mapping)
        ):
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
            job.archived_input_file = str(archived_path)
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


async def enqueue_processing(path: Path, source: str) -> tuple[bool, str | None]:
    try:
        sig = signature(path)
    except FileNotFoundError:
        return False, None
    if (
        sig in state.seen_signatures
        or sig in state.running_signatures
        or sig in state.queued_signatures
    ):
        return False, None

    job = Job(
        id=uuid.uuid4().hex[:12],
        status="queued",
        source=source,
        input_file=str(path),
        created_at=utc_now(),
    )
    await state.add_job(job)
    state.queued_signatures.add(sig)
    await state.queue.put(QueueItem(path=path, source=source, signature=sig, job_id=job.id))
    return True, job.id


async def worker_loop(worker_id: int) -> None:
    _ = worker_id
    while True:
        item = await state.queue.get()
        try:
            state.queued_signatures.discard(item.signature)
            state.running_signatures.add(item.signature)
            job = await state.get_job(item.job_id)
            if job is None:
                continue
            job.status = "running"
            job.started_at = utc_now()
            await state.update_job(job)
            await process_file(item.path, item.source, job)
        finally:
            state.running_signatures.discard(item.signature)
            state.seen_signatures.add(item.signature)
            state.queue.task_done()


async def scanner_loop() -> None:
    while True:
        try:
            for path in sorted(INPUT_DIR.rglob("*")):
                if not path.is_file() or path.suffix.lower() not in SUPPORTED_EXT:
                    continue
                await enqueue_processing(path, "watcher")
        except Exception:
            pass
        await asyncio.sleep(max(1, SCAN_SECONDS))


app = FastAPI(title="Healthcare Standardization Dashboard", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(REPO_ROOT / "dashboard" / "static")), name="static")


@app.on_event("startup")
async def on_startup() -> None:
    ensure_dirs()
    if hasattr(std, "configure_iid_dictionary"):
        iid_path = Path(IID_DICTIONARY_ENV) if IID_DICTIONARY_ENV else None
        std.configure_iid_dictionary(
            path=iid_path,
            search_roots=[INPUT_DIR, INPUT_DIR.parent, REPO_ROOT, IID_SEARCH_ROOT],
        )
    if state.scanner_task is None:
        state.scanner_task = asyncio.create_task(scanner_loop())
    if not state.worker_tasks:
        for i in range(max(1, WORKER_CONCURRENCY)):
            state.worker_tasks.append(asyncio.create_task(worker_loop(i)))


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if state.scanner_task:
        state.scanner_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await state.scanner_task
    for task in state.worker_tasks:
        task.cancel()
    for task in state.worker_tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await task
    state.worker_tasks.clear()


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
    scheduled, job_id = await enqueue_processing(dst, "upload")
    return {
        "status": "accepted",
        "saved_to": str(dst),
        "queued": scheduled,
        "job_id": job_id,
    }


@app.get("/api/download/{job_id}")
async def api_download(job_id: str) -> FileResponse:
    job = state.jobs_by_id.get(job_id)
    if not job or not job.output_file:
        raise HTTPException(status_code=404, detail="job/output not found")
    path = Path(job.output_file)
    if not path.exists():
        raise HTTPException(status_code=404, detail="output file missing")
    return FileResponse(path=str(path), filename=path.name, media_type="text/csv")


@app.get("/api/download-input/{job_id}")
async def api_download_input(job_id: str) -> FileResponse:
    job = state.jobs_by_id.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    path = resolve_job_input_path(job)
    if path is None:
        raise HTTPException(status_code=404, detail="input file missing")
    return FileResponse(path=str(path), filename=path.name)


@app.get("/api/compare/{job_id}")
async def api_compare(job_id: str, rows: int = 12, cols: int = 12) -> dict[str, Any]:
    rows = max(1, min(rows, 100))
    cols = max(1, min(cols, 80))
    job = state.jobs_by_id.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    before_error: str | None = None
    after_error: str | None = None

    input_path = resolve_job_input_path(job)
    if input_path is None:
        before_error = "input file missing"
    else:
        try:
            table = job.table or "tbImportAcData"
            h, recs, profile = std.load_records(input_path, table)
            before = preview_from_records(h, recs, rows, cols)
            before["profile"] = profile
            before["path"] = str(input_path)
        except Exception as exc:
            before_error = str(exc)

    if job.output_file:
        try:
            out_path = Path(job.output_file)
            after = load_output_csv_preview(out_path, rows, cols)
            after["path"] = str(out_path)
        except Exception as exc:
            after_error = str(exc)
    else:
        after_error = "output not available yet"

    return {
        "job_id": job.id,
        "status": job.status,
        "table": job.table,
        "source": job.source,
        "before": before,
        "after": after,
        "before_error": before_error,
        "after_error": after_error,
        "download_input_url": f"/api/download-input/{job.id}",
        "download_output_url": f"/api/download/{job.id}" if job.output_file else None,
    }


@app.post("/api/reset")
async def api_reset(purge_files: bool = False) -> dict[str, Any]:
    result = await state.reset_state()
    if result["running_jobs"] > 0:
        raise HTTPException(
            status_code=409,
            detail=f"cannot reset while {result['running_jobs']} job(s) are running",
        )
    deleted_files = purge_runtime_files() if purge_files else 0
    return {
        "status": "ok",
        "cleared_jobs": result["cleared_jobs"],
        "cleared_queue_items": result["cleared_queue_items"],
        "purge_files": purge_files,
        "deleted_files": deleted_files,
    }
