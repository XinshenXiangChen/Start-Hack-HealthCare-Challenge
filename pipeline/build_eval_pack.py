#!/usr/bin/env python3
"""Build an evaluation pack (manifest + expected artifacts) from repo datasets."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import standardize as std


def to_rel(path: Path, repo_root: Path) -> str:
    return str(path.resolve().relative_to(repo_root.resolve()))


def pick_round_robin(groups: dict[str, list[Path]], size: int) -> list[tuple[str, Path]]:
    ordered_tables = sorted(groups.keys())
    idx = {t: 0 for t in ordered_tables}
    selected: list[tuple[str, Path]] = []
    while len(selected) < size:
        made_progress = False
        for table in ordered_tables:
            arr = groups[table]
            i = idx[table]
            if i >= len(arr):
                continue
            selected.append((table, arr[i]))
            idx[table] += 1
            made_progress = True
            if len(selected) >= size:
                break
        if not made_progress:
            break
    return selected


def write_expected_csv(path: Path, rows: list[dict[str, Any]], cols: list[str], max_rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for row in rows[:max_rows]:
            writer.writerow({c: row.get(c, "") for c in cols})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-sql", required=True, type=Path)
    parser.add_argument("--repo-root", default=Path.cwd(), type=Path)
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--size", default=50, type=int)
    parser.add_argument("--max-expected-rows", default=5, type=int)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--input-dirs",
        nargs="+",
        default=[
            "Endtestdaten_mit_Fehlern_ einheitliche ID",
            "Endtestdaten_ohne_Fehler_ einheitliche ID",
        ],
    )
    parser.add_argument("--model", default="llama3.2:latest")
    parser.add_argument("--llm-timeout", default=45, type=int)
    parser.add_argument("--prompt-template", type=Path)
    parser.add_argument("--no-llm", action="store_true")
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    output_dir = args.output_dir.resolve()
    data_root = args.data_root.resolve() if args.data_root else repo_root
    expected_dir = output_dir / "expected_outputs"
    mapping_dir = output_dir / "expected_mappings"
    prompt_template = args.prompt_template.read_text(encoding="utf-8") if args.prompt_template else None

    schema = std.parse_schema(args.schema_sql.resolve())
    groups: dict[str, list[Path]] = defaultdict(list)

    for d in args.input_dirs:
        base = (data_root / d).resolve()
        for p in sorted(base.rglob("*")):
            if p.suffix.lower() not in {".csv", ".xlsx", ".pdf"}:
                continue
            headers_probe, _, _ = std.load_records(p, "tbImportAcData")
            table = std.detect_table(p, headers_probe)
            if table and table in schema:
                groups[table].append(p)

    chosen = pick_round_robin(groups, args.size)
    if len(chosen) < args.size:
        raise RuntimeError(
            f"requested {args.size} cases but only {len(chosen)} detectable files available"
        )

    cases: list[dict[str, Any]] = []
    for i, (table, file_path) in enumerate(chosen, start=1):
        headers, records, _ = std.load_records(file_path, table)
        target_cols = [c for c in schema[table] if c != "coId"]
        mapping = std.build_mapping(
            table=table,
            headers=headers,
            target_cols=target_cols,
            use_llm=not args.no_llm,
            model=args.model,
            timeout_s=args.llm_timeout,
            prompt_template=prompt_template,
        )
        standardized = std.standardize_records(records, target_cols, mapping)

        case_id = f"case_{i:03d}"
        mapping_path = mapping_dir / f"{case_id}.mapping.json"
        output_path = expected_dir / f"{case_id}.expected.csv"
        mapping_path.parent.mkdir(parents=True, exist_ok=True)
        mapping_payload = {k: v for k, v in mapping.items()}
        mapping_path.write_text(
            json.dumps(mapping_payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        write_expected_csv(output_path, standardized, target_cols, args.max_expected_rows)

        cases.append(
            {
                "id": case_id,
                "input_file": str(file_path.resolve().relative_to(data_root)),
                "expected_table": table,
                "expected_mapping_file": to_rel(mapping_path, repo_root),
                "expected_output_file": to_rel(output_path, repo_root),
            }
        )

    manifest = {
        "meta": {
            "kind": "silver_eval_pack",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "repo_root": str(repo_root),
            "data_root": str(data_root),
            "size": len(cases),
            "max_expected_rows": args.max_expected_rows,
            "llm_used_for_expectations": not args.no_llm,
            "note": "Auto-generated expectations from current pipeline version; use as benchmark bootstrap, not final human-labeled gold.",
        },
        "cases": cases,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    counts: dict[str, int] = defaultdict(int)
    for c in cases:
        counts[c["expected_table"]] += 1
    print(f"wrote manifest: {manifest_path}")
    print(f"cases: {len(cases)}")
    for table in sorted(counts):
        print(f"{table}: {counts[table]}")


if __name__ == "__main__":
    main()
