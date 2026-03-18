from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Optional


def _sanitize_dotenv() -> None:
    """
    python-dotenv defaults to finding a `.env` in the current working directory
    and parent folders. If that file exists but is UTF-16, decoding can crash.
    We rewrite any discovered `.env` to UTF-8 (best-effort) before other code runs.
    """
    try:
        from dotenv import find_dotenv  # type: ignore
    except Exception:
        return

    dotenv_path_str: Optional[str] = None
    try:
        dotenv_path_str = find_dotenv()
    except Exception:
        dotenv_path_str = None

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
        # Never block pipeline execution due to dotenv issues.
        return


REPO_ROOT = Path(__file__).resolve().parent
PIPELINE_DIR = REPO_ROOT / "pipeline"
DB_SQL = REPO_ROOT / "database" / "sqlserver" / "CreateImportTables.sql"

_sanitize_dotenv()


def run_python(script: Path, args: list[str]) -> int:
    cmd = [sys.executable, str(script), *args]
    proc = subprocess.run(cmd, cwd=REPO_ROOT)
    return proc.returncode


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Healthcare data standardization pipeline (CSV/XLSX/PDF/SQL -> SQL target schema)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    schema_cmd = subparsers.add_parser("schema", help="Extract DB schema report from SQL.")
    schema_cmd.add_argument("--out-json", default="pipeline/out/db_schema.json")
    schema_cmd.add_argument("--out-md", default="pipeline/out/db_schema.md")

    standardize_cmd = subparsers.add_parser(
        "standardize", help="Standardize an input folder to target-table CSVs."
    )
    standardize_cmd.add_argument("--input-dir", required=True)
    standardize_cmd.add_argument("--output-dir", required=True)
    standardize_cmd.add_argument("--model", default="llama3.2:latest")
    standardize_cmd.add_argument("--llm-timeout", type=int, default=45)
    standardize_cmd.add_argument(
        "--prompt-template",
        default="pipeline/prompts/column_mapper_prompt.txt",
    )
    standardize_cmd.add_argument("--no-llm", action="store_true")

    eval_cmd = subparsers.add_parser("eval", help="Evaluate against a benchmark manifest.")
    eval_cmd.add_argument("--manifest", required=True)
    eval_cmd.add_argument("--out-json", required=True)
    eval_cmd.add_argument("--out-md")
    eval_cmd.add_argument("--data-root", default=None, help="Base folder for manifest input_file resolution")
    eval_cmd.add_argument("--model", default="llama3.2:latest")
    eval_cmd.add_argument("--llm-timeout", type=int, default=45)
    eval_cmd.add_argument(
        "--prompt-template",
        default="pipeline/prompts/column_mapper_prompt.txt",
    )
    eval_cmd.add_argument("--no-llm", action="store_true")

    args = parser.parse_args()

    if args.command == "schema":
        rc = run_python(
            PIPELINE_DIR / "schema_report.py",
            [
                "--sql",
                str(DB_SQL),
                "--out-json",
                str(REPO_ROOT / args.out_json),
                "--out-md",
                str(REPO_ROOT / args.out_md),
            ],
        )
        raise SystemExit(rc)

    if args.command == "standardize":
        cmd_args = [
            "--schema-sql",
            str(DB_SQL),
            "--input-dir",
            str(REPO_ROOT / args.input_dir),
            "--output-dir",
            str(REPO_ROOT / args.output_dir),
            "--model",
            args.model,
            "--llm-timeout",
            str(args.llm_timeout),
        ]
        if args.prompt_template:
            cmd_args.extend(["--prompt-template", str(REPO_ROOT / args.prompt_template)])
        if args.no_llm:
            cmd_args.append("--no-llm")
        rc = run_python(PIPELINE_DIR / "standardize.py", cmd_args)
        raise SystemExit(rc)

    if args.command == "eval":
        cmd_args = [
            "--schema-sql",
            str(DB_SQL),
            "--repo-root",
            str(REPO_ROOT),
            "--manifest",
            str(REPO_ROOT / args.manifest),
            "--out-json",
            str(REPO_ROOT / args.out_json),
            "--model",
            args.model,
            "--llm-timeout",
            str(args.llm_timeout),
        ]
        if args.data_root:
            cmd_args.extend(["--data-root", str(REPO_ROOT / args.data_root)])
        if args.out_md:
            cmd_args.extend(["--out-md", str(REPO_ROOT / args.out_md)])
        if args.prompt_template:
            cmd_args.extend(["--prompt-template", str(REPO_ROOT / args.prompt_template)])
        if args.no_llm:
            cmd_args.append("--no-llm")
        rc = run_python(PIPELINE_DIR / "eval.py", cmd_args)
        raise SystemExit(rc)


if __name__ == "__main__":
    main()
