from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
PIPELINE_DIR = REPO_ROOT / "pipeline"
DB_SQL = REPO_ROOT / "database" / "sqlserver" / "CreateImportTables.sql"


def run_python(script: Path, args: list[str]) -> int:
    cmd = [sys.executable, str(script), *args]
    proc = subprocess.run(cmd, cwd=REPO_ROOT)
    return proc.returncode


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Healthcare data standardization pipeline (CSV/XLSX/PDF -> SQL target schema)."
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
