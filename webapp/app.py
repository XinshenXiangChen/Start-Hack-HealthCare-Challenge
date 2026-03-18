from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
import sys

from flask import Flask, redirect, render_template, request, url_for


REPO_ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = REPO_ROOT / "pipeline" / "gold"
WEB_OUT_DIR = REPO_ROOT / "pipeline" / "out" / "web"
SCHEMA_SQL = REPO_ROOT / "database" / "sqlserver" / "CreateImportTables.sql"

WEB_OUT_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True


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


def list_manifests() -> list[Path]:
    # Example manifest + generated benchmark manifests.
    patterns = [
        GOLD_DIR / "example_manifest.json",
    ]
    patterns.extend(list(GOLD_DIR.rglob("manifest.json")))
    # De-dup while preserving order.
    seen: set[Path] = set()
    out: list[Path] = []
    for p in patterns:
        if not p.exists() or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def manifest_options() -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    for p in list_manifests():
        rel = p.relative_to(REPO_ROOT).as_posix()
        options.append({"label": str(p), "value": rel})
    options.sort(key=lambda x: x["value"])
    return options


def latest_run() -> Path | None:
    jsons = sorted(WEB_OUT_DIR.glob("run_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return jsons[0] if jsons else None


def load_run(run_path: Path) -> dict:
    return json.loads(run_path.read_text(encoding="utf-8"))


def run_eval(
    *,
    manifest_rel: str,
    out_json_rel: str,
    data_root_rel: str | None,
    model: str,
    llm_timeout: int,
    no_llm: bool,
) -> tuple[int, str]:
    out_json_path = REPO_ROOT / out_json_rel
    out_md_path = (REPO_ROOT / out_json_rel).with_suffix(".md")

    cmd = [
        sys.executable,
        str(REPO_ROOT / "pipeline.py"),
        "eval",
        "--manifest",
        manifest_rel,
        "--out-json",
        out_json_rel,
        "--out-md",
        out_md_path.relative_to(REPO_ROOT).as_posix(),
        "--model",
        model,
        "--llm-timeout",
        str(llm_timeout),
    ]
    if data_root_rel:
        cmd.extend(["--data-root", data_root_rel])
    if no_llm:
        cmd.append("--no-llm")

    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return proc.returncode, (proc.stdout + "\n" + proc.stderr).strip()


@app.route("/", methods=["GET"])
def index():
    latest = latest_run()
    latest_id = latest.stem if latest else None
    return render_template(
        "index.html",
        manifests=manifest_options(),
        latest_run_id=latest_id,
    )


@app.route("/run", methods=["POST"])
def run():
    manifest_rel = request.form.get("manifest", "").strip()
    if not manifest_rel:
        return "Missing manifest", 400

    data_root = request.form.get("data_root", "").strip() or None
    model = request.form.get("model", "llama3.2:latest").strip()
    llm_timeout = int(request.form.get("llm_timeout", "45").strip())
    no_llm = bool(request.form.get("no_llm"))

    run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{int(time.time() * 1000) % 100000}"
    out_json_rel = f"pipeline/out/web/{run_id}.json"

    rc, log_text = run_eval(
        manifest_rel=manifest_rel,
        out_json_rel=out_json_rel,
        data_root_rel=data_root,
        model=model,
        llm_timeout=llm_timeout,
        no_llm=no_llm,
    )

    # If evaluation crashed, write a small error payload for visibility.
    out_json_path = REPO_ROOT / out_json_rel
    if rc != 0:
        out_json_path.parent.mkdir(parents=True, exist_ok=True)
        out_json_path.write_text(
            json.dumps(
                {
                    "status": "error",
                    "run_id": run_id,
                    "return_code": rc,
                    "log": log_text,
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
    data = load_run(run_path)
    return render_template("run.html", run=data, run_id=run_id)


if __name__ == "__main__":
    # Accessible at http://127.0.0.1:5000
    app.run(host="127.0.0.1", port=5000, debug=True)

