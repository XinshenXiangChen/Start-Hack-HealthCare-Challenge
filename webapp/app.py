from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

from flask import Flask, redirect, render_template, request, url_for
from werkzeug.datastructures import FileStorage


REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_OUT_DIR = REPO_ROOT / "pipeline" / "out" / "web"
WEB_UPLOAD_DIR = REPO_ROOT / "pipeline" / "out" / "uploads"
WEB_STANDARDIZED_DIR = REPO_ROOT / "pipeline" / "out" / "standardized_web"
SCHEMA_SQL = REPO_ROOT / "database" / "sqlserver" / "CreateImportTables.sql"
REVIEW_ACTIONS_FILE = WEB_OUT_DIR / "review_actions.json"
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
DEFAULT_MODEL = "llama3.2:latest"
DEFAULT_LLM_TIMEOUT = 45

WEB_OUT_DIR.mkdir(parents=True, exist_ok=True)
WEB_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
WEB_STANDARDIZED_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True


def pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.0f}%"


def one_decimal(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


app.jinja_env.filters["pct"] = pct
app.jinja_env.filters["one_decimal"] = one_decimal


def _sanitize_dotenv() -> None:
    """
    Best-effort rewrite of a discovered `.env` file to UTF-8 to avoid
    python-dotenv UnicodeDecodeError on Windows UTF-16 files.
    """
    try:
        from dotenv import find_dotenv  # type: ignore
    except Exception:
        return

    try:
        dotenv_path_str = find_dotenv()
    except Exception:
        return
    if not dotenv_path_str:
        return

    dotenv_path = Path(dotenv_path_str)
    if not dotenv_path.exists():
        return

    try:
        raw = dotenv_path.read_bytes()
        if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
            text = raw.decode("utf-16")
        else:
            for enc in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
                try:
                    text = raw.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                text = raw.decode("utf-8", errors="replace")

        dotenv_path.write_text(text, encoding="utf-8")
    except Exception:
        return


_sanitize_dotenv()


def latest_run() -> Path | None:
    jsons = sorted(WEB_OUT_DIR.glob("run_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return jsons[0] if jsons else None


def load_run(run_path: Path) -> dict:
    return json.loads(run_path.read_text(encoding="utf-8"))


def stage_uploaded_files(run_id: str, uploads: list[FileStorage]) -> tuple[Path | None, dict[str, Any] | None]:
    valid_uploads = [upload for upload in uploads if upload and upload.filename]

    if not valid_uploads:
        return None, {
            "status": "error",
            "title": "No files uploaded",
            "user_message": "Upload one or more source files to start the intake review.",
            "log": "No uploaded files were provided.",
        }

    upload_root = WEB_UPLOAD_DIR / run_id
    upload_root.mkdir(parents=True, exist_ok=True)
    seen_names: set[str] = set()
    duplicate_files: list[str] = []
    for upload in valid_uploads:
        name = Path(upload.filename or "").name
        norm = name.lower()
        if norm in seen_names:
            duplicate_files.append(name)
            continue
        seen_names.add(norm)
        target = upload_root / name
        upload.save(target)

    if duplicate_files:
        return None, {
            "status": "error",
            "title": "Duplicate filenames uploaded",
            "user_message": "Each uploaded file needs a unique filename so it can be tracked separately.",
            "duplicate_files": duplicate_files,
            "log": "Duplicate uploaded filenames detected.",
        }

    return upload_root, None


def safe_ratio(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return num / den


def eval_schema_quality(
    rows: list[dict[str, Any]], table_schema: list[dict[str, Any]]
) -> dict[str, Any]:
    cols = [c for c in table_schema if c["name"] != "coId"]
    required = [c["name"] for c in cols if not c["nullable"]]
    type_checked = 0
    type_valid = 0
    type_invalid_examples: list[dict[str, Any]] = []
    req_total = len(rows) * len(required)
    req_present = 0
    required_missing_by_column: dict[str, dict[str, int]] = {
        c: {"present": 0, "missing": 0, "total": len(rows)} for c in required
    }

    pipeline_dir = REPO_ROOT / "pipeline"
    if str(pipeline_dir) not in sys.path:
        sys.path.insert(0, str(pipeline_dir))
    import eval as eval_module  # type: ignore

    for row_idx, row in enumerate(rows):
        for col in cols:
            name = col["name"]
            value = "" if row.get(name) is None else str(row.get(name)).strip()
            if value == "":
                continue
            type_checked += 1
            sql_type = str(col["type"])
            if eval_module.value_matches_type(sql_type, value):
                type_valid += 1
            elif len(type_invalid_examples) < 25:
                type_invalid_examples.append(
                    {
                        "row_index": row_idx,
                        "column": name,
                        "sql_type": sql_type,
                        "value": value,
                    }
                )
        for req_col in required:
            if str(row.get(req_col, "") or "").strip() != "":
                req_present += 1
                required_missing_by_column[req_col]["present"] += 1
            else:
                required_missing_by_column[req_col]["missing"] += 1

    return {
        "type_checked_values": type_checked,
        "type_valid_values": type_valid,
        "type_valid_rate": safe_ratio(type_valid, type_checked),
        "required_columns": required,
        "required_fill_rate": safe_ratio(req_present, req_total),
        "required_missing_values": req_total - req_present,
        "required_missing_by_column": required_missing_by_column,
        "type_invalid_examples": type_invalid_examples,
    }


def process_uploaded_files(
    upload_root: Path, run_id: str, model: str, llm_timeout: int, no_llm: bool
) -> dict[str, Any]:
    pipeline_dir = REPO_ROOT / "pipeline"
    if str(pipeline_dir) not in sys.path:
        sys.path.insert(0, str(pipeline_dir))

    import standardize as std  # type: ignore
    from schema_report import parse_schema as parse_schema_detailed  # type: ignore

    schema_simple = std.parse_schema(SCHEMA_SQL)
    schema_detailed = parse_schema_detailed(SCHEMA_SQL)
    standardized_dir = WEB_STANDARDIZED_DIR / run_id
    standardized_dir.mkdir(parents=True, exist_ok=True)

    files = sorted([p for p in upload_root.iterdir() if p.is_file()])
    case_results: list[dict[str, Any]] = []

    for idx, path in enumerate(files, start=1):
        case_id = f"upload_{idx:03d}"
        result: dict[str, Any] = {
            "id": case_id,
            "input_file": str(path),
            "expected_table": None,
        }
        try:
            headers_probe, _, profile_probe = std.load_records(path, "tbImportAcData")
            predicted_table = std.detect_table(path, headers_probe, profile_probe)
            result["predicted_table"] = predicted_table
            result["expected_table"] = predicted_table
            result["table_match"] = True if predicted_table else None

            if not predicted_table or predicted_table not in schema_simple:
                result["status"] = "error"
                result["error"] = "table_not_detected_or_not_in_schema"
                case_results.append(result)
                continue

            headers, records, profile = std.load_records(path, predicted_table)
            target_cols = [c for c in schema_simple[predicted_table] if c != "coId"]
            mapping = std.build_mapping(
                table=predicted_table,
                headers=headers,
                target_cols=target_cols,
                use_llm=not no_llm,
                model=model,
                timeout_s=llm_timeout,
                prompt_template=None,
            )
            predicted_rows = std.standardize_records(records, target_cols, mapping)

            out_file = standardized_dir / f"{path.stem}__{predicted_table}.csv"
            std.write_csv(out_file, predicted_rows, target_cols)

            result.update(
                {
                    "status": "ok",
                    "profile": profile,
                    "headers_in": len(headers),
                    "rows_in": len(records),
                    "rows_out": len(predicted_rows),
                    "mapped_columns": len(mapping),
                    "mapping_precision": None,
                    "mapping_recall": None,
                    "mapping_f1": None,
                    "mapping_exact_ratio": None,
                    "mapping_gold_pred_by_key": mapping,
                    "cell_accuracy": None,
                    "cell_mismatch_samples": [],
                    "missing_rows": None,
                    "extra_rows": None,
                    "standardized_output_file": str(out_file),
                }
            )
            result.update(eval_schema_quality(predicted_rows, schema_detailed[predicted_table]))
        except Exception as exc:
            result["status"] = "error"
            result["error"] = str(exc)
        case_results.append(result)

    executed = [r for r in case_results if r.get("status") == "ok"]
    failed = [r for r in case_results if r.get("status") != "ok"]

    def mean_of(key: str) -> float | None:
        vals = [r.get(key) for r in executed if r.get(key) is not None]
        return safe_ratio(sum(vals), len(vals))

    summary = {
        "cases_total": len(case_results),
        "cases_executed": len(executed),
        "cases_failed": len(failed),
        "table_accuracy": None,
        "mean_mapping_f1": None,
        "mean_cell_accuracy": None,
        "mean_type_valid_rate": mean_of("type_valid_rate"),
        "mean_required_fill_rate": mean_of("required_fill_rate"),
    }
    return {"summary": summary, "cases": case_results}


def load_review_actions() -> dict[str, dict[str, dict[str, str]]]:
    if not REVIEW_ACTIONS_FILE.exists():
        return {}
    try:
        return json.loads(REVIEW_ACTIONS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_review_actions(data: dict[str, dict[str, dict[str, str]]]) -> None:
    REVIEW_ACTIONS_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def score_tone(value: float | None) -> str:
    if value is None:
        return "muted"
    if value >= 0.9:
        return "good"
    if value >= 0.7:
        return "warn"
    return "bad"


def summarize_case(case: dict[str, Any], action_state: dict[str, str] | None) -> dict[str, Any]:
    normalized_case = {
        "status": case.get("status"),
        "error": case.get("error"),
        "input_file": case.get("input_file", ""),
        "expected_table": case.get("expected_table"),
        "predicted_table": case.get("predicted_table"),
        "table_match": case.get("table_match"),
        "required_fill_rate": case.get("required_fill_rate"),
        "required_missing_values": case.get("required_missing_values"),
        "required_missing_by_column": case.get("required_missing_by_column"),
        "type_valid_rate": case.get("type_valid_rate"),
        "type_invalid_examples": case.get("type_invalid_examples"),
        "mapping_f1": case.get("mapping_f1"),
        "mapping_precision": case.get("mapping_precision"),
        "mapping_recall": case.get("mapping_recall"),
        "mapping_exact_ratio": case.get("mapping_exact_ratio"),
        "mapping_gold_pred_by_key": case.get("mapping_gold_pred_by_key"),
        "cell_accuracy": case.get("cell_accuracy"),
        "cell_mismatch_samples": case.get("cell_mismatch_samples"),
        "missing_rows": case.get("missing_rows"),
        "extra_rows": case.get("extra_rows"),
        "rows_in": case.get("rows_in"),
        "rows_out": case.get("rows_out"),
        "headers_in": case.get("headers_in"),
        "mapped_columns": case.get("mapped_columns"),
        "profile": case.get("profile"),
    }

    required_fill_rate = normalized_case["required_fill_rate"]
    type_valid_rate = normalized_case["type_valid_rate"]
    mapping_f1 = normalized_case["mapping_f1"]
    cell_accuracy = normalized_case["cell_accuracy"]
    scores = [value for value in [required_fill_rate, type_valid_rate, mapping_f1, cell_accuracy] if isinstance(value, (int, float))]
    quality_score = sum(scores) / len(scores) if scores else None

    anomaly_count = 0
    if case.get("required_missing_values"):
        anomaly_count += 1
    if case.get("type_invalid_examples"):
        anomaly_count += 1
    if case.get("cell_mismatch_samples"):
        anomaly_count += 1
    if case.get("table_match") is False or case.get("status") != "ok":
        anomaly_count += 1

    if case.get("status") != "ok":
        health_label = "Blocked"
        workflow_status = "Needs replacement"
        recommended_action = "replace"
    elif anomaly_count == 0 and (quality_score or 0) >= 0.95:
        health_label = "Ready"
        workflow_status = "Ready for export"
        recommended_action = "accept"
    elif (required_fill_rate or 0) < 0.85:
        health_label = "Incomplete"
        workflow_status = "Missing required data"
        recommended_action = "manual"
    elif case.get("table_match") is False or (mapping_f1 is not None and mapping_f1 < 0.7):
        health_label = "Needs review"
        workflow_status = "Likely source or mapping issue"
        recommended_action = "replace"
    else:
        health_label = "Flagged"
        workflow_status = "Anomalies detected"
        recommended_action = "manual"

    return {
        **case,
        **normalized_case,
        "input_name": Path(str(normalized_case["input_file"])).name or str(normalized_case["input_file"]),
        "quality_score": quality_score,
        "quality_pct": one_decimal(quality_score),
        "quality_tone": score_tone(quality_score),
        "required_fill_pct": pct(required_fill_rate),
        "required_fill_tone": score_tone(required_fill_rate),
        "type_valid_pct": pct(type_valid_rate),
        "type_valid_tone": score_tone(type_valid_rate),
        "mapping_f1_pct": pct(mapping_f1),
        "mapping_f1_tone": score_tone(mapping_f1),
        "cell_accuracy_pct": pct(cell_accuracy),
        "cell_accuracy_tone": score_tone(cell_accuracy),
        "health_label": health_label,
        "health_tone": "good" if health_label == "Ready" else ("bad" if health_label == "Blocked" else "warn"),
        "workflow_status": workflow_status,
        "recommended_action": recommended_action,
        "anomaly_count": anomaly_count,
        "selected_action": (action_state or {}).get("action", ""),
        "action_note": (action_state or {}).get("note", ""),
        "action_updated_at": (action_state or {}).get("updated_at", ""),
    }


def enrich_run(run_data: dict[str, Any], run_id: str) -> dict[str, Any]:
    if run_data.get("status") == "error":
        return run_data

    review_actions = load_review_actions().get(run_id, {})
    cases = [summarize_case(case, review_actions.get(str(case.get("id", "")))) for case in run_data.get("cases", [])]

    ready_cases = sum(1 for case in cases if case["health_label"] == "Ready")
    blocked_cases = sum(1 for case in cases if case.get("status") != "ok")
    flagged_cases = sum(1 for case in cases if case["anomaly_count"] > 0)
    pending_actions = sum(1 for case in cases if case["health_label"] != "Ready" and not case["selected_action"])

    source_counts: dict[str, int] = {}
    for case in cases:
        ext = Path(str(case.get("input_file", ""))).suffix.lower() or "unknown"
        source_counts[ext] = source_counts.get(ext, 0) + 1

    run_data["cases"] = cases
    run_data["ops_summary"] = {
        "ready_cases": ready_cases,
        "blocked_cases": blocked_cases,
        "flagged_cases": flagged_cases,
        "pending_actions": pending_actions,
        "accepted_cases": sum(1 for case in cases if case["selected_action"] == "accept"),
        "manual_cases": sum(1 for case in cases if case["selected_action"] == "manual"),
        "replace_cases": sum(1 for case in cases if case["selected_action"] == "replace"),
        "discard_cases": sum(1 for case in cases if case["selected_action"] == "discard"),
        "source_counts": source_counts,
    }
    return run_data


@app.route("/", methods=["GET"])
def index():
    latest = latest_run()
    latest_id = latest.stem if latest else None
    return render_template(
        "index.html",
        latest_run_id=latest_id,
    )


@app.route("/run", methods=["POST"])
def run():
    model = DEFAULT_MODEL
    llm_timeout = DEFAULT_LLM_TIMEOUT
    no_llm = bool(request.form.get("no_llm"))
    uploads = request.files.getlist("files")

    run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{int(time.time() * 1000) % 100000}"
    out_json_rel = f"pipeline/out/web/{run_id}.json"
    out_json_path = REPO_ROOT / out_json_rel

    upload_root, upload_error = stage_uploaded_files(run_id, uploads)
    if upload_error is not None:
        out_json_path.parent.mkdir(parents=True, exist_ok=True)
        out_json_path.write_text(
            json.dumps({"run_id": run_id, **upload_error}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return redirect(url_for("run_details", run_id=run_id))

    try:
        run_payload = process_uploaded_files(upload_root, run_id, model, llm_timeout, no_llm)
        out_json_path.parent.mkdir(parents=True, exist_ok=True)
        out_json_path.write_text(
            json.dumps(run_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as exc:
        out_json_path.parent.mkdir(parents=True, exist_ok=True)
        out_json_path.write_text(
            json.dumps(
                {
                    "status": "error",
                    "run_id": run_id,
                    "title": "Processing failed",
                    "user_message": "The uploaded files could not be processed.",
                    "log": str(exc),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    return redirect(url_for("run_details", run_id=run_id))


@app.route("/runs/<run_id>", methods=["GET"])
def run_details(run_id: str):
    run_path = WEB_OUT_DIR / f"{run_id}.json"
    if not run_path.exists():
        return f"Run not found: {run_id}", 404
    data = enrich_run(load_run(run_path), run_id)
    return render_template("run.html", run=data, run_id=run_id)


@app.route("/runs/<run_id>/cases/<case_id>/action", methods=["POST"])
def update_case_action(run_id: str, case_id: str):
    actions = load_review_actions()
    run_actions = actions.setdefault(run_id, {})
    action = request.form.get("action", "").strip()
    note = request.form.get("note", "").strip()

    if action:
        run_actions[case_id] = {
            "action": action,
            "note": note,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
    elif case_id in run_actions:
        run_actions.pop(case_id, None)

    save_review_actions(actions)
    return redirect(url_for("run_details", run_id=run_id))


if __name__ == "__main__":
    # Accessible at http://127.0.0.1:5000
    app.run(host="127.0.0.1", port=5000, debug=True)
