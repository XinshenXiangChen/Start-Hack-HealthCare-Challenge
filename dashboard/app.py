#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import copy
import csv
import contextlib
import json
import os
import re
import shutil
import subprocess
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
LINKED_DIR = Path(
    os.getenv("PIPELINE_LINKED_DIR", str(REPO_ROOT / "runtime" / "linked"))
)
LINK_STATE_PATH = LINKED_DIR / "entity_index.json"
MODEL = os.getenv("PIPELINE_MODEL", "llama3.2:latest")
LLM_TIMEOUT = int(os.getenv("PIPELINE_LLM_TIMEOUT", "45"))
NO_LLM = os.getenv("PIPELINE_NO_LLM", "0").strip().lower() in {"1", "true", "yes"}
SCAN_SECONDS = int(os.getenv("PIPELINE_SCAN_SECONDS", "5"))
WORKER_CONCURRENCY = int(os.getenv("PIPELINE_WORKER_CONCURRENCY", "1"))
IID_DICTIONARY_ENV = (os.getenv("PIPELINE_IID_DICTIONARY") or "").strip()
IID_SEARCH_ROOT = Path(os.getenv("PIPELINE_IID_SEARCH_ROOT", str(REPO_ROOT.parent)))

GLOBAL_ENTITY_KEYS = ["coPatient_id", "coCaseId", "coCaseIdAlpha", "coEncounter_id"]
TABLE_ENTITY_KEY_CANDIDATES: dict[str, list[str]] = {
    "tbImportAcData": ["coCaseId", "coCaseIdAlpha"],
    "tbImportLabsData": ["coCaseId", "coPatient_id"],
    "tbImportIcd10Data": ["coCaseId"],
    "tbImportDeviceMotionData": ["coPatient_id", "coCaseId"],
    "tbImportDevice1HzMotionData": ["coPatient_id", "coCaseId", "coDevice_id"],
    "tbImportMedicationInpatientData": ["coPatient_id", "coCaseId", "coEncounter_id"],
    "tbImportNursingDailyReportsData": ["coPatient_id", "coCaseId"],
}
ENTITY_SAMPLE_LIMIT = max(5, int(os.getenv("PIPELINE_ENTITY_SAMPLE_LIMIT", "40")))
ENTITY_PREVIEW_COLS = max(3, int(os.getenv("PIPELINE_ENTITY_PREVIEW_COLS", "8")))
AI_LINKING_ENABLED = os.getenv("PIPELINE_AI_LINKING", "1").strip().lower() not in {
    "0",
    "false",
    "no",
}
AI_LINK_TIMEOUT = max(4, int(os.getenv("PIPELINE_AI_LINK_TIMEOUT", "15")))
AI_LINK_CANDIDATES = max(3, int(os.getenv("PIPELINE_AI_LINK_CANDIDATES", "8")))
AI_LINK_MAX_CALLS_PER_JOB = max(0, int(os.getenv("PIPELINE_AI_LINK_MAX_CALLS_PER_JOB", "40")))
TOKEN_RE = re.compile(r"[a-z0-9]{3,}")
JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def parse_bool_env(name: str, default: bool = False) -> bool:
    text = (os.getenv(name) or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


DEBUG_ROUTING = parse_bool_env("PIPELINE_DEBUG_ROUTING", False)


def log_routing_debug(payload: dict[str, Any]) -> None:
    if not DEBUG_ROUTING:
        return
    print(
        "[routing-debug] " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        flush=True,
    )


def normalize_entity_value(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    if text.lower() in {"na", "n/a", "nan", "none", "null", "unknown", "-"}:
        return ""
    return text


def token_value(value: str) -> str:
    return " ".join(value.strip().lower().split())


def unique_preserving_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for val in values:
        if val in seen:
            continue
        seen.add(val)
        out.append(val)
    return out


def extract_entity_keys(table: str, row: dict[str, Any]) -> dict[str, str]:
    keys: dict[str, str] = {}
    candidates = unique_preserving_order(
        TABLE_ENTITY_KEY_CANDIDATES.get(table, []) + GLOBAL_ENTITY_KEYS
    )
    for col in candidates:
        if col not in row:
            continue
        value = normalize_entity_value(row.get(col))
        if value:
            keys[col] = value
    return keys


def build_entity_tokens(entity_keys: dict[str, str]) -> list[str]:
    return [f"{k}:{token_value(v)}" for k, v in entity_keys.items() if token_value(v)]


def tokenize_text(value: Any) -> set[str]:
    text = normalize_entity_value(value).lower()
    if not text:
        return set()
    return set(TOKEN_RE.findall(text))


def semantic_tokens_from_preview(preview: dict[str, str]) -> list[str]:
    hints = (
        "patient",
        "case",
        "encounter",
        "device",
        "ward",
        "room",
        "birth",
        "dob",
        "sex",
        "gender",
        "name",
    )
    tokens: list[str] = []
    for key, value in preview.items():
        kn = key.lower()
        if not any(h in kn for h in hints):
            continue
        vv = token_value(value)
        if not vv:
            continue
        tokens.append(f"sem:{kn}:{vv}")
    return unique_preserving_order(tokens)[:4]


def run_llm_entity_link(
    model: str,
    table: str,
    row_preview: dict[str, str],
    candidates: list[dict[str, Any]],
    timeout_s: int,
) -> tuple[str | None, float]:
    if not row_preview or not candidates:
        return None, 0.0
    prompt = (
        "You are matching disconnected healthcare records across files.\n"
        "Task: choose the best existing entity_id for the incoming row. If no safe match, return null.\n"
        "Use identifiers and context (case/patient/encounter/device/ward/timestamps/text).\n"
        "Output JSON only: {\"entity_id\": \"...\" | null, \"confidence\": 0..1}.\n\n"
        f"table={table}\n"
        f"incoming_row={json.dumps(row_preview, ensure_ascii=False)}\n"
        f"candidates={json.dumps(candidates, ensure_ascii=False)}\n"
    )
    try:
        proc = subprocess.run(
            ["ollama", "run", model, prompt],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None, 0.0
    out = (proc.stdout or "").strip()
    match = JSON_BLOCK_RE.search(out)
    if not match:
        return None, 0.0
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None, 0.0
    entity_id = parsed.get("entity_id")
    conf_raw = parsed.get("confidence", 0.0)
    try:
        confidence = float(conf_raw)
    except Exception:
        confidence = 0.0
    if entity_id is None:
        return None, max(0.0, min(1.0, confidence))
    entity_id_txt = str(entity_id).strip()
    if not entity_id_txt:
        return None, max(0.0, min(1.0, confidence))
    return entity_id_txt, max(0.0, min(1.0, confidence))


def build_row_preview(
    table: str,
    row: dict[str, Any],
    entity_keys: dict[str, str],
    max_items: int = ENTITY_PREVIEW_COLS,
) -> dict[str, str]:
    preview: dict[str, str] = {}
    priority = unique_preserving_order(TABLE_ENTITY_KEY_CANDIDATES.get(table, []) + GLOBAL_ENTITY_KEYS)
    for key in priority:
        value = normalize_entity_value(entity_keys.get(key))
        if value and key not in preview:
            preview[key] = value
            if len(preview) >= max_items:
                return preview
    for col, raw_value in row.items():
        if col in preview:
            continue
        value = normalize_entity_value(raw_value)
        if not value:
            continue
        preview[col] = truncate_cell(value, 120)
        if len(preview) >= max_items:
            break
    return preview


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
    linked_rows: int = 0
    linked_entities_touched: int = 0
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
        self.link_lock = asyncio.Lock()
        self.scanner_task: asyncio.Task[Any] | None = None
        self.worker_tasks: list[asyncio.Task[Any]] = []
        self.linked_rows_by_table: dict[str, dict[str, Any]] = {}
        self.entity_index: dict[str, dict[str, Any]] = {}
        self.entity_token_to_id: dict[str, str] = {}
        self.entity_counter: int = 0
        self.ai_link_cache: dict[str, str] = {}
        self.ai_link_attempts: int = 0
        self.ai_link_matches: int = 0

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
        async with self.link_lock:
            linked_entities = len(self.entity_index)
            linked_tables = len(self.linked_rows_by_table)
            linked_rows = sum(
                int((stats or {}).get("row_count", 0))
                for stats in self.linked_rows_by_table.values()
            )
            ai_attempts = self.ai_link_attempts
            ai_matches = self.ai_link_matches
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
            "linked_entities": linked_entities,
            "linked_tables": linked_tables,
            "linked_rows": linked_rows,
            "linked_dir": str(LINKED_DIR),
            "ai_linking_enabled": AI_LINKING_ENABLED and not NO_LLM,
            "ai_link_attempts": ai_attempts,
            "ai_link_matches": ai_matches,
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
        async with self.link_lock:
            self.linked_rows_by_table.clear()
            self.entity_index.clear()
            self.entity_token_to_id.clear()
            self.entity_counter = 0
            self.ai_link_cache.clear()
            self.ai_link_attempts = 0
            self.ai_link_matches = 0
            self._persist_link_state_locked()

        cleared_q = 0
        while True:
            try:
                _ = self.queue.get_nowait()
                self.queue.task_done()
                cleared_q += 1
            except asyncio.QueueEmpty:
                break
        return {"running_jobs": 0, "cleared_jobs": cleared_jobs, "cleared_queue_items": cleared_q}

    def _next_entity_id_locked(self) -> str:
        self.entity_counter += 1
        return f"ent_{self.entity_counter:08d}"

    def _ensure_entity_locked(self, entity_id: str, now: str) -> dict[str, Any]:
        entity = self.entity_index.get(entity_id)
        if entity is not None:
            return entity
        entity = {
            "entity_id": entity_id,
            "tokens": [],
            "keys": {},
            "table_counts": {},
            "row_count": 0,
            "jobs": [],
            "samples": [],
            "first_seen": now,
            "last_seen": now,
        }
        self.entity_index[entity_id] = entity
        return entity

    @staticmethod
    def _append_unique(values: list[str], value: str, max_len: int | None = None) -> None:
        if not value or value in values:
            return
        values.append(value)
        if max_len is not None and len(values) > max_len:
            del values[0 : len(values) - max_len]

    def _merge_entities_locked(self, keep_id: str, drop_id: str) -> None:
        if keep_id == drop_id:
            return
        keep = self.entity_index.get(keep_id)
        drop = self.entity_index.pop(drop_id, None)
        if keep is None or drop is None:
            return

        keep_tokens = keep.setdefault("tokens", [])
        for tok in drop.get("tokens", []):
            if tok not in keep_tokens:
                keep_tokens.append(tok)
            self.entity_token_to_id[tok] = keep_id

        keep_keys = keep.setdefault("keys", {})
        for key, values in (drop.get("keys") or {}).items():
            keep_vals = keep_keys.setdefault(key, [])
            for val in values or []:
                self._append_unique(keep_vals, str(val), max_len=20)

        keep_table_counts = keep.setdefault("table_counts", {})
        for table, count in (drop.get("table_counts") or {}).items():
            keep_table_counts[table] = int(keep_table_counts.get(table, 0)) + int(count or 0)

        keep["row_count"] = int(keep.get("row_count", 0)) + int(drop.get("row_count", 0) or 0)
        keep_jobs = keep.setdefault("jobs", [])
        for job_id in drop.get("jobs", []):
            self._append_unique(keep_jobs, str(job_id), max_len=400)

        merged_samples = keep.setdefault("samples", [])
        for sample in drop.get("samples", []):
            sample_cp = dict(sample)
            sample_cp["entity_id"] = keep_id
            if len(merged_samples) < ENTITY_SAMPLE_LIMIT:
                merged_samples.append(sample_cp)
            else:
                break

        keep_first = str(keep.get("first_seen") or "")
        drop_first = str(drop.get("first_seen") or "")
        keep_last = str(keep.get("last_seen") or "")
        drop_last = str(drop.get("last_seen") or "")
        if keep_first and drop_first:
            keep["first_seen"] = min(keep_first, drop_first)
        elif drop_first:
            keep["first_seen"] = drop_first
        if keep_last and drop_last:
            keep["last_seen"] = max(keep_last, drop_last)
        elif drop_last:
            keep["last_seen"] = drop_last

    def _resolve_entity_id_locked(self, entity_keys: dict[str, str], now: str) -> str:
        tokens = build_entity_tokens(entity_keys)
        existing_ids: list[str] = []
        for tok in tokens:
            entity_id = self.entity_token_to_id.get(tok)
            if entity_id and entity_id in self.entity_index and entity_id not in existing_ids:
                existing_ids.append(entity_id)

        if existing_ids:
            existing_ids.sort(
                key=lambda eid: (
                    int((self.entity_index.get(eid) or {}).get("row_count", 0)),
                    eid,
                ),
                reverse=True,
            )
            keep_id = existing_ids[0]
            for other_id in existing_ids[1:]:
                self._merge_entities_locked(keep_id, other_id)
            entity_id = keep_id
        else:
            entity_id = self._next_entity_id_locked()
            self._ensure_entity_locked(entity_id, now)

        entity = self._ensure_entity_locked(entity_id, now)
        entity_tokens = entity.setdefault("tokens", [])
        for tok in tokens:
            self.entity_token_to_id[tok] = entity_id
            if tok not in entity_tokens:
                entity_tokens.append(tok)
        return entity_id

    def _attach_tokens_locked(self, entity_id: str, tokens: list[str]) -> None:
        if not tokens:
            return
        entity = self.entity_index.get(entity_id)
        if entity is None:
            return
        entity_tokens = entity.setdefault("tokens", [])
        for tok in tokens:
            if not tok:
                continue
            self.entity_token_to_id[tok] = entity_id
            if tok not in entity_tokens:
                entity_tokens.append(tok)

    def _entity_text_tokens_locked(self, entity: dict[str, Any]) -> set[str]:
        toks: set[str] = set()
        for key, vals in (entity.get("keys") or {}).items():
            toks.update(tokenize_text(key))
            for val in vals or []:
                toks.update(tokenize_text(val))
        for sample in (entity.get("samples") or [])[:4]:
            toks.update(tokenize_text(sample.get("table")))
            for key, value in (sample.get("preview") or {}).items():
                toks.update(tokenize_text(key))
                toks.update(tokenize_text(value))
        return toks

    def _candidate_entities_for_llm_locked(
        self,
        table: str,
        row_preview: dict[str, str],
    ) -> list[dict[str, Any]]:
        if not self.entity_index:
            return []
        row_tokens: set[str] = set()
        row_tokens.update(tokenize_text(table))
        for key, value in row_preview.items():
            row_tokens.update(tokenize_text(key))
            row_tokens.update(tokenize_text(value))

        scored: list[tuple[int, int, str, dict[str, Any]]] = []
        for entity_id, ent in self.entity_index.items():
            ent_tokens = self._entity_text_tokens_locked(ent)
            overlap = len(row_tokens.intersection(ent_tokens))
            row_count = int(ent.get("row_count", 0) or 0)
            scored.append((overlap, row_count, entity_id, ent))

        scored.sort(reverse=True)
        out: list[dict[str, Any]] = []
        for overlap, _row_count, entity_id, ent in scored:
            if len(out) >= AI_LINK_CANDIDATES:
                break
            if overlap <= 0 and len(out) >= max(2, AI_LINK_CANDIDATES // 2):
                break
            out.append(
                {
                    "entity_id": entity_id,
                    "overlap_score": overlap,
                    "keys": ent.get("keys", {}),
                    "table_counts": ent.get("table_counts", {}),
                    "last_seen": ent.get("last_seen"),
                    "sample_preview": (
                        (ent.get("samples") or [{}])[0].get("preview", {})
                        if ent.get("samples")
                        else {}
                    ),
                }
            )
        return out

    def _resolve_entity_id_with_llm_locked(
        self,
        table: str,
        row_preview: dict[str, str],
    ) -> tuple[str | None, float]:
        if not AI_LINKING_ENABLED or NO_LLM:
            return None, 0.0
        candidates = self._candidate_entities_for_llm_locked(table, row_preview)
        if not candidates:
            return None, 0.0
        cache_key = json.dumps(
            {
                "table": table,
                "row_preview": row_preview,
                "candidate_ids": [c["entity_id"] for c in candidates],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        if cache_key in self.ai_link_cache:
            cached = self.ai_link_cache[cache_key]
            return (cached, 1.0) if cached else (None, 0.0)

        self.ai_link_attempts += 1
        entity_id, confidence = run_llm_entity_link(
            model=MODEL,
            table=table,
            row_preview=row_preview,
            candidates=candidates,
            timeout_s=AI_LINK_TIMEOUT,
        )
        if entity_id and entity_id in self.entity_index and confidence >= 0.45:
            self.ai_link_cache[cache_key] = entity_id
            self.ai_link_matches += 1
            return entity_id, confidence
        self.ai_link_cache[cache_key] = ""
        return None, confidence

    def _append_linked_rows_csv_locked(self, table: str, new_rows: list[dict[str, Any]]) -> Path:
        path = LINKED_DIR / f"{table}__linked.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        new_headers = unique_preserving_order(
            [key for row in new_rows for key in row.keys()]
        )

        if not path.exists() or path.stat().st_size == 0:
            fieldnames = new_headers
            with path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in new_rows:
                    writer.writerow({h: row.get(h, "") for h in fieldnames})
            return path

        existing_rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            existing_rows.extend(reader)
            existing_headers = list(reader.fieldnames or [])

        if existing_headers == new_headers:
            with path.open("a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=new_headers)
                for row in new_rows:
                    writer.writerow({h: row.get(h, "") for h in new_headers})
            return path

        merged_headers = unique_preserving_order(existing_headers + new_headers)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=merged_headers)
            writer.writeheader()
            for row in existing_rows:
                writer.writerow({h: row.get(h, "") for h in merged_headers})
            for row in new_rows:
                writer.writerow({h: row.get(h, "") for h in merged_headers})
        tmp_path.replace(path)
        return path

    def _persist_link_state_locked(self) -> None:
        LINKED_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "entity_counter": self.entity_counter,
            "entity_index": self.entity_index,
            "entity_token_to_id": self.entity_token_to_id,
            "linked_rows_by_table": self.linked_rows_by_table,
            "ai_link_attempts": self.ai_link_attempts,
            "ai_link_matches": self.ai_link_matches,
        }
        tmp_path = LINK_STATE_PATH.with_suffix(LINK_STATE_PATH.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        tmp_path.replace(LINK_STATE_PATH)

    async def load_link_state(self) -> None:
        async with self.link_lock:
            if not LINK_STATE_PATH.exists():
                return
            try:
                payload = json.loads(LINK_STATE_PATH.read_text(encoding="utf-8"))
            except Exception:
                return
            if not isinstance(payload, dict):
                return

            entity_index = payload.get("entity_index")
            token_to_id = payload.get("entity_token_to_id")
            table_stats = payload.get("linked_rows_by_table")
            counter = payload.get("entity_counter")
            ai_attempts = payload.get("ai_link_attempts")
            ai_matches = payload.get("ai_link_matches")
            if isinstance(entity_index, dict):
                self.entity_index = entity_index
            if isinstance(token_to_id, dict):
                self.entity_token_to_id = {
                    str(k): str(v) for k, v in token_to_id.items() if k and v
                }
            if isinstance(table_stats, dict):
                self.linked_rows_by_table = table_stats
            try:
                self.entity_counter = int(counter)
            except Exception:
                self.entity_counter = 0
            if self.entity_counter <= 0:
                self.entity_counter = len(self.entity_index)
            try:
                self.ai_link_attempts = int(ai_attempts or 0)
            except Exception:
                self.ai_link_attempts = 0
            try:
                self.ai_link_matches = int(ai_matches or 0)
            except Exception:
                self.ai_link_matches = 0

    async def link_rows(
        self,
        table: str,
        rows: list[dict[str, Any]],
        job: Job,
    ) -> tuple[int, int]:
        if not rows:
            return 0, 0
        now = utc_now()
        linked_rows = 0
        touched_entities: set[str] = set()
        rows_for_linked_file: list[dict[str, Any]] = []
        ai_calls_used = 0

        async with self.link_lock:
            for idx, row in enumerate(rows, start=1):
                entity_keys = extract_entity_keys(table, row)
                row_preview = build_row_preview(table, row, entity_keys)
                semantic_tokens = semantic_tokens_from_preview(row_preview)
                entity_id: str | None = None
                match_method = "id_keys"
                ai_confidence = 0.0

                if entity_keys:
                    entity_id = self._resolve_entity_id_locked(entity_keys, now)
                else:
                    match_method = "new_entity"
                    if (
                        AI_LINKING_ENABLED
                        and not NO_LLM
                        and ai_calls_used < AI_LINK_MAX_CALLS_PER_JOB
                    ):
                        ai_calls_used += 1
                        ai_entity_id, ai_confidence = self._resolve_entity_id_with_llm_locked(
                            table=table,
                            row_preview=row_preview,
                        )
                        if ai_entity_id and ai_entity_id in self.entity_index:
                            entity_id = ai_entity_id
                            match_method = "ai_match"

                    if entity_id is None and semantic_tokens:
                        for tok in semantic_tokens:
                            existing = self.entity_token_to_id.get(tok)
                            if existing and existing in self.entity_index:
                                entity_id = existing
                                match_method = "semantic_token"
                                break

                    if entity_id is None:
                        entity_id = self._next_entity_id_locked()
                        self._ensure_entity_locked(entity_id, now)

                if semantic_tokens:
                    self._attach_tokens_locked(entity_id, semantic_tokens)

                entity = self._ensure_entity_locked(entity_id, now)
                entity["last_seen"] = now

                keys_obj = entity.setdefault("keys", {})
                for key, value in entity_keys.items():
                    vals = keys_obj.setdefault(key, [])
                    self._append_unique(vals, value, max_len=20)

                table_counts = entity.setdefault("table_counts", {})
                table_counts[table] = int(table_counts.get(table, 0)) + 1
                entity["row_count"] = int(entity.get("row_count", 0)) + 1

                jobs = entity.setdefault("jobs", [])
                self._append_unique(jobs, job.id, max_len=400)

                samples = entity.setdefault("samples", [])
                if len(samples) < ENTITY_SAMPLE_LIMIT:
                    samples.append(
                        {
                            "entity_id": entity_id,
                            "table": table,
                            "job_id": job.id,
                            "row_index": idx,
                            "input_file": job.input_file,
                            "output_file": job.output_file or "",
                            "captured_at": now,
                            "source": job.source,
                            "match_method": match_method,
                            "llm_confidence": ai_confidence,
                            "entity_keys": entity_keys,
                            "preview": row_preview,
                        }
                    )

                linked_row = dict(row)
                linked_row["_eid"] = entity_id
                linked_row["_entity_id"] = entity_id
                linked_row["_table"] = table
                linked_row["_job_id"] = job.id
                linked_row["_row_index"] = idx
                linked_row["_source"] = job.source
                linked_row["_source_file"] = job.input_file
                linked_row["_source_job_id"] = job.id
                linked_row["_source_table"] = table
                linked_row["_source_row_index"] = idx
                linked_row["_match_method"] = match_method
                linked_row["_llm_confidence"] = f"{ai_confidence:.3f}" if ai_confidence else ""
                linked_row["_captured_at"] = now
                rows_for_linked_file.append(linked_row)

                linked_rows += 1
                touched_entities.add(entity_id)

            if rows_for_linked_file:
                path = self._append_linked_rows_csv_locked(table, rows_for_linked_file)
                stats = self.linked_rows_by_table.setdefault(
                    table,
                    {"table": table, "row_count": 0, "job_count": 0, "jobs": [], "path": str(path)},
                )
                stats["row_count"] = int(stats.get("row_count", 0)) + linked_rows
                stats["path"] = str(path)
                stats["updated_at"] = now
                jobs = stats.setdefault("jobs", [])
                self._append_unique(jobs, job.id, max_len=500)
                stats["job_count"] = len(jobs)

            self._persist_link_state_locked()

        return linked_rows, len(touched_entities)

    async def list_entities(self, limit: int = 200, query: str = "") -> list[dict[str, Any]]:
        q = query.strip().lower()
        async with self.link_lock:
            rows = list(self.entity_index.values())
            if q:
                filtered: list[dict[str, Any]] = []
                for ent in rows:
                    if q in str(ent.get("entity_id", "")).lower():
                        filtered.append(ent)
                        continue
                    found = False
                    for key, values in (ent.get("keys") or {}).items():
                        if q in key.lower():
                            found = True
                            break
                        for val in values or []:
                            if q in str(val).lower():
                                found = True
                                break
                        if found:
                            break
                    if found:
                        filtered.append(ent)
                rows = filtered

            rows.sort(
                key=lambda ent: (
                    int(ent.get("row_count", 0) or 0),
                    str(ent.get("last_seen") or ""),
                ),
                reverse=True,
            )
            out: list[dict[str, Any]] = []
            for ent in rows[: max(1, min(limit, 1000))]:
                table_counts = ent.get("table_counts") or {}
                out.append(
                    {
                        "entity_id": ent.get("entity_id"),
                        "row_count": int(ent.get("row_count", 0) or 0),
                        "table_counts": table_counts,
                        "tables": sorted(table_counts.keys()),
                        "jobs": ent.get("jobs", []),
                        "keys": ent.get("keys", {}),
                        "last_seen": ent.get("last_seen"),
                        "sample_count": len(ent.get("samples", []) or []),
                    }
                )
            return out

    async def get_entity(self, entity_id: str) -> dict[str, Any] | None:
        async with self.link_lock:
            entity = self.entity_index.get(entity_id)
            if entity is None:
                return None
            return copy.deepcopy(entity)


state = State()


def ensure_dirs() -> None:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    LINKED_DIR.mkdir(parents=True, exist_ok=True)


def purge_runtime_files() -> int:
    deleted = 0
    for base in (INPUT_DIR, OUTPUT_DIR, ARCHIVE_DIR, LINKED_DIR):
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


def count_nonempty_output_columns(
    rows: list[dict[str, Any]], target_cols: list[str]
) -> int:
    if not rows:
        return 0
    count = 0
    for col in target_cols:
        for row in rows:
            if str(row.get(col, "") or "").strip():
                count += 1
                break
    return count


def preview_from_records(
    headers: list[str],
    records: list[dict[str, Any]],
    max_rows: int,
    max_cols: int,
    prefer_nonempty_cols: bool = False,
) -> dict[str, Any]:
    shown_headers = headers[:max_cols]
    if prefer_nonempty_cols and headers:
        counts: list[tuple[int, int, str]] = []
        for idx, col in enumerate(headers):
            nonempty = 0
            for rec in records:
                if str(rec.get(col, "") or "").strip():
                    nonempty += 1
            counts.append((nonempty, idx, col))
        counts.sort(key=lambda t: (-t[0], t[1]))
        chosen = [col for nonempty, _idx, col in counts if nonempty > 0][:max_cols]
        if chosen:
            shown_headers = chosen
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
    return preview_from_records(
        headers=headers,
        records=records,
        max_rows=max_rows,
        max_cols=max_cols,
        prefer_nonempty_cols=True,
    )


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
) -> tuple[str | None, dict[str, Any]]:
    routing_debug: dict[str, Any] = {}
    table = std.detect_table(path, headers, profile, debug=routing_debug)
    if not table and hasattr(std, "detect_table_from_records"):
        fb_table = std.detect_table_from_records(records)
        routing_debug["fallback_records_table"] = fb_table
        if fb_table:
            table = fb_table
            routing_debug["matched_table"] = fb_table
            routing_debug["matched_by"] = "fallback_records"
            routing_debug["matched_value"] = "detect_table_from_records"
    if not table and not NO_LLM and hasattr(std, "run_llama_table_detection"):
        allowed_tables = [t for t in state.schema.keys() if t in std.TABLE_HINTS]
        fb_table = std.run_llama_table_detection(
            model=MODEL,
            path=path,
            headers=headers,
            records=records,
            allowed_tables=allowed_tables or list(state.schema.keys()),
            timeout_s=LLM_TIMEOUT,
        )
        routing_debug["fallback_llm_allowed_tables"] = allowed_tables or list(state.schema.keys())
        routing_debug["fallback_llm_table"] = fb_table
        if fb_table:
            table = fb_table
            routing_debug["matched_table"] = fb_table
            routing_debug["matched_by"] = "fallback_llm"
            routing_debug["matched_value"] = MODEL
    if not table:
        routing_debug.setdefault("matched_table", None)
        routing_debug.setdefault("matched_by", "none")
        routing_debug.setdefault("matched_value", None)
    return table, routing_debug


async def process_file(path: Path, source: str, job: Job) -> None:
    try:
        headers_probe, records_probe, profile_probe = std.load_records(path, "tbImportAcData")
        table, routing_debug = detect_table_with_fallback(
            path, headers_probe, records_probe, profile_probe
        )
        if not table or table not in state.schema:
            log_routing_debug(
                {
                    "job_id": job.id,
                    "source": source,
                    "status": "error",
                    "reason": "table_not_detected_or_not_in_schema",
                    "routing_debug": routing_debug,
                }
            )
            raise ValueError("table_not_detected_or_not_in_schema")

        headers, records, profile = std.load_records(path, table)
        target_cols = [c for c in state.schema[table] if c != "coId"]
        routing_debug["target_cols_count"] = len(target_cols)
        routing_debug["target_cols_sample"] = target_cols[:12]
        profile["routing_debug"] = routing_debug
        log_routing_debug(
            {
                "job_id": job.id,
                "source": source,
                "status": "ok",
                "table": table,
                "routing_debug": routing_debug,
            }
        )
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
        rows = std.standardize_records(
            records=records,
            target_cols=target_cols,
            mapping=mapping,
            table=table,
            headers=headers,
        )

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
        job.output_file = str(output_path)

        linked_rows = 0
        linked_entities_touched = 0
        try:
            linked_rows, linked_entities_touched = await state.link_rows(table, rows, job)
            profile["linked_rows"] = linked_rows
            profile["linked_entities_touched"] = linked_entities_touched
        except Exception as link_exc:
            profile["cross_file_link_error"] = str(link_exc)

        archived_name = f"{path.stem}__{job.id}{path.suffix.lower()}"
        archived_path = ARCHIVE_DIR / archived_name
        try:
            shutil.move(str(path), str(archived_path))
            job.archived_input_file = str(archived_path)
        except Exception:
            pass

        job.status = "ok"
        job.table = table
        job.rows_in = len(records)
        job.rows_out = len(rows)
        active_output_columns = count_nonempty_output_columns(rows, target_cols)
        job.mapped_columns = max(len(mapping), active_output_columns)
        job.semantic_extraction_used = semantic_extraction_used
        job.semantic_rows_enriched = semantic_rows_enriched
        job.linked_rows = linked_rows
        job.linked_entities_touched = linked_entities_touched
        profile["active_output_columns"] = active_output_columns
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
    await state.load_link_state()
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


@app.get("/api/entities")
async def api_entities(limit: int = 200, q: str = "") -> dict[str, Any]:
    q_txt = (q or "").strip()
    entities = await state.list_entities(limit=limit, query=q_txt)
    if not entities and not q_txt and LINK_STATE_PATH.exists():
        await state.load_link_state()
        entities = await state.list_entities(limit=limit, query=q_txt)
    async with state.link_lock:
        total_entities = len(state.entity_index)
    return {"entities": entities, "total_entities": total_entities}


@app.get("/api/entity/{entity_id}")
async def api_entity(entity_id: str) -> dict[str, Any]:
    entity = await state.get_entity(entity_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="entity not found")
    return entity


@app.get("/api/download-linked/{table}")
async def api_download_linked(table: str) -> FileResponse:
    async with state.link_lock:
        stats = copy.deepcopy(state.linked_rows_by_table.get(table))
    if not isinstance(stats, dict):
        raise HTTPException(status_code=404, detail="table not linked yet")
    path = Path(stats.get("path") or (LINKED_DIR / f"{table}__linked.csv"))
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="linked file not found")
    return FileResponse(path=str(path), filename=path.name, media_type="text/csv")


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


@app.post("/api/rerun/{job_id}")
async def api_rerun(job_id: str) -> dict[str, Any]:
    job = await state.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    if job.status in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="job is still in progress")

    src_path = resolve_job_input_path(job)
    if src_path is None:
        raise HTTPException(status_code=404, detail="input source file not found")
    if src_path.suffix.lower() not in SUPPORTED_EXT:
        raise HTTPException(status_code=400, detail="unsupported file type for rerun")

    ensure_dirs()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rerun_name = f"{stamp}_rerun_{job.id}_{safe_name(src_path.name)}"
    dst = INPUT_DIR / rerun_name
    shutil.copy2(src_path, dst)

    scheduled, new_job_id = await enqueue_processing(dst, f"rerun:{job_id}")
    return {
        "status": "accepted",
        "source_job_id": job_id,
        "saved_to": str(dst),
        "queued": scheduled,
        "job_id": new_job_id,
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
