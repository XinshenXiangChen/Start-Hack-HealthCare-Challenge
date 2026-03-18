#!/usr/bin/env python3
"""Evaluate standardization quality against a gold manifest."""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import standardize as std
from schema_report import parse_schema as parse_schema_detailed


INT_RE = re.compile(r"^[+-]?\d+$")
NUMERIC_RE = re.compile(r"^[+-]?(\d+(\.\d+)?|\.\d+)$")
DT_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%d.%m.%Y",
    "%d.%m.%Y %H:%M",
    "%d.%m.%Y %H:%M:%S",
    "%d-%b-%Y",
    "%d-%b-%Y %H:%M:%S",
    "%Y%m%d",
    "%H:%M:%S",
]


def to_norm(value: Any) -> str:
    return "" if value is None else str(value).strip()


def safe_ratio(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return num / den


def resolve_path(path_text: str, manifest_dir: Path, repo_root: Path) -> Path:
    p = Path(path_text)
    if p.is_absolute():
        return p
    from_manifest = (manifest_dir / p).resolve()
    if from_manifest.exists():
        return from_manifest
    return (repo_root / p).resolve()


def read_expected_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    enc, delim = std.sniff_text_file(path)
    with path.open(encoding=enc, newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        headers = list(reader.fieldnames or [])
        rows = [{h: to_norm(row.get(h, "")) for h in headers} for row in reader]
    return headers, rows


def load_expected_mapping(
    case: dict[str, Any], manifest_dir: Path, repo_root: Path
) -> dict[str, str | None] | None:
    if "expected_mapping" in case and isinstance(case["expected_mapping"], dict):
        return {str(k): (None if v in (None, "") else str(v)) for k, v in case["expected_mapping"].items()}
    mapping_file = case.get("expected_mapping_file")
    if not mapping_file:
        return None
    p = resolve_path(str(mapping_file), manifest_dir, repo_root)
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected mapping file must contain JSON object: {p}")
    return {str(k): (None if v in (None, "") else str(v)) for k, v in data.items()}


def eval_mapping_metrics(
    expected: dict[str, str | None], predicted: dict[str, str]
) -> dict[str, Any]:
    sources = list(expected.keys())
    if not sources:
        return {
            "mapping_precision": None,
            "mapping_recall": None,
            "mapping_f1": None,
            "mapping_exact_ratio": None,
            "mapping_tp": 0,
            "mapping_fp": 0,
            "mapping_fn": 0,
        }

    tp = 0
    fp = 0
    fn = 0
    exact = 0

    for src in sources:
        gold = expected.get(src)
        pred = predicted.get(src)
        gold_non = gold not in (None, "")
        pred_non = pred not in (None, "")

        if (gold or None) == (pred or None):
            exact += 1

        if gold_non and pred_non:
            if gold == pred:
                tp += 1
            else:
                fp += 1
                fn += 1
        elif gold_non and not pred_non:
            fn += 1
        elif not gold_non and pred_non:
            fp += 1

    precision = safe_ratio(tp, tp + fp)
    recall = safe_ratio(tp, tp + fn)
    f1 = None if precision is None or recall is None or (precision + recall) == 0 else (2 * precision * recall) / (precision + recall)
    return {
        "mapping_precision": precision,
        "mapping_recall": recall,
        "mapping_f1": f1,
        "mapping_exact_ratio": safe_ratio(exact, len(sources)),
        "mapping_tp": tp,
        "mapping_fp": fp,
        "mapping_fn": fn,
    }


def parse_datetime_like(value: str) -> bool:
    text = value.strip()
    if not text:
        return True
    try:
        datetime.fromisoformat(text.replace("T", " "))
        return True
    except ValueError:
        pass
    for fmt in DT_FORMATS:
        try:
            datetime.strptime(text, fmt)
            return True
        except ValueError:
            continue
    return False


def value_matches_type(sql_type: str, value: str) -> bool:
    t = sql_type.lower()
    if value == "":
        return True
    if t.startswith("smallint") or t.startswith("int") or t.startswith("bigint"):
        return INT_RE.match(value) is not None
    if t.startswith("numeric") or t.startswith("decimal") or t.startswith("float"):
        return NUMERIC_RE.match(value.replace(",", ".")) is not None
    if t.startswith("datetime") or t.startswith("date") or t.startswith("time"):
        return parse_datetime_like(value)
    return True


def eval_schema_quality(
    rows: list[dict[str, Any]], table_schema: list[dict[str, Any]]
) -> dict[str, Any]:
    cols = [c for c in table_schema if c["name"] != "coId"]
    required = [c["name"] for c in cols if not c["nullable"]]
    type_checked = 0
    type_valid = 0
    req_total = len(rows) * len(required)
    req_present = 0

    for row in rows:
        for col in cols:
            name = col["name"]
            value = to_norm(row.get(name, ""))
            if value == "":
                continue
            type_checked += 1
            if value_matches_type(str(col["type"]), value):
                type_valid += 1
        for req_col in required:
            if to_norm(row.get(req_col, "")) != "":
                req_present += 1

    return {
        "type_checked_values": type_checked,
        "type_valid_values": type_valid,
        "type_valid_rate": safe_ratio(type_valid, type_checked),
        "required_columns": required,
        "required_fill_rate": safe_ratio(req_present, req_total),
        "required_missing_values": req_total - req_present,
    }


def eval_output_metrics(
    predicted_rows: list[dict[str, Any]], expected_headers: list[str], expected_rows: list[dict[str, str]]
) -> dict[str, Any]:
    cols = expected_headers
    exp_len = len(expected_rows)
    pred_len = len(predicted_rows)
    total_cells = exp_len * len(cols)
    correct_cells = 0
    exact_rows = 0

    for i in range(exp_len):
        pred = predicted_rows[i] if i < pred_len else None
        exp = expected_rows[i]
        row_ok = pred is not None
        for col in cols:
            p = to_norm(pred.get(col, "")) if pred is not None else ""
            e = to_norm(exp.get(col, "")) if exp is not None else ""
            if p == e:
                correct_cells += 1
            else:
                row_ok = False
        if row_ok:
            exact_rows += 1

    return {
        "expected_rows": exp_len,
        "predicted_rows": pred_len,
        "extra_rows": max(pred_len - exp_len, 0),
        "missing_rows": max(exp_len - pred_len, 0),
        "row_count_match": exp_len == pred_len,
        "row_exact_rate": safe_ratio(exact_rows, exp_len),
        "cell_accuracy": safe_ratio(correct_cells, total_cells),
    }


def format_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def render_markdown(summary: dict[str, Any], cases: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("# Evaluation Report")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Cases: {summary['cases_total']}")
    lines.append(f"- Executed: {summary['cases_executed']}")
    lines.append(f"- Failed: {summary['cases_failed']}")
    lines.append(f"- Table routing accuracy: {format_pct(summary['table_accuracy'])}")
    lines.append(f"- Mean mapping F1: {format_pct(summary['mean_mapping_f1'])}")
    lines.append(f"- Mean cell accuracy: {format_pct(summary['mean_cell_accuracy'])}")
    lines.append(f"- Mean schema type-valid rate: {format_pct(summary['mean_type_valid_rate'])}")
    lines.append(f"- Mean required-fill rate: {format_pct(summary['mean_required_fill_rate'])}")
    lines.append("")
    lines.append("## Per Case")
    lines.append("| Case | File | Table OK | Mapping F1 | Cell Acc | Type Valid | Required Fill |")
    lines.append("|---|---|---:|---:|---:|---:|---:|")
    for case in cases:
        lines.append(
            "| {id} | {file} | {table_ok} | {f1} | {cell} | {typev} | {req} |".format(
                id=case.get("id", ""),
                file=Path(case["input_file"]).name,
                table_ok="yes" if case.get("table_match") else "no",
                f1=format_pct(case.get("mapping_f1")),
                cell=format_pct(case.get("cell_accuracy")),
                typev=format_pct(case.get("type_valid_rate")),
                req=format_pct(case.get("required_fill_rate")),
            )
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-sql", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--repo-root", default=Path.cwd(), type=Path)
    parser.add_argument("--model", default="llama3.2:latest")
    parser.add_argument("--llm-timeout", default=45, type=int)
    parser.add_argument("--prompt-template", type=Path)
    parser.add_argument("--no-llm", action="store_true")
    parser.add_argument("--out-json", required=True, type=Path)
    parser.add_argument("--out-md", type=Path)
    parser.add_argument("--save-standardized-dir", type=Path)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    manifest_path = args.manifest.resolve()
    manifest_dir = manifest_path.parent
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    if "cases" not in manifest or not isinstance(manifest["cases"], list):
        raise ValueError("manifest must contain a 'cases' array")

    schema_simple = std.parse_schema(args.schema_sql.resolve())
    schema_detailed = parse_schema_detailed(args.schema_sql.resolve())
    prompt_template = args.prompt_template.read_text(encoding="utf-8") if args.prompt_template else None

    case_results: list[dict[str, Any]] = []
    for idx, case in enumerate(manifest["cases"], start=1):
        case_id = str(case.get("id") or f"case_{idx}")
        input_file = resolve_path(str(case["input_file"]), manifest_dir, repo_root)
        expected_table = case.get("expected_table")
        result: dict[str, Any] = {
            "id": case_id,
            "input_file": str(input_file),
            "expected_table": expected_table,
        }
        try:
            headers_probe, _, _ = std.load_records(input_file, "tbImportAcData")
            predicted_table = std.detect_table(input_file, headers_probe)
            result["predicted_table"] = predicted_table
            result["table_match"] = (
                expected_table is not None and predicted_table == expected_table
            )

            table_for_run = predicted_table or expected_table
            if not table_for_run or table_for_run not in schema_simple:
                result["status"] = "error"
                result["error"] = "table_not_detected_or_not_in_schema"
                case_results.append(result)
                continue

            headers, records, profile = std.load_records(input_file, table_for_run)
            target_cols = [c for c in schema_simple[table_for_run] if c != "coId"]
            mapping = std.build_mapping(
                table=table_for_run,
                headers=headers,
                target_cols=target_cols,
                use_llm=not args.no_llm,
                model=args.model,
                timeout_s=args.llm_timeout,
                prompt_template=prompt_template,
            )
            predicted_rows = std.standardize_records(records, target_cols, mapping)

            result.update(
                {
                    "status": "ok",
                    "profile": profile,
                    "headers_in": len(headers),
                    "rows_in": len(records),
                    "rows_out": len(predicted_rows),
                    "mapped_columns": len(mapping),
                }
            )

            expected_mapping = load_expected_mapping(case, manifest_dir, repo_root)
            if expected_mapping is not None:
                mm = eval_mapping_metrics(expected_mapping, mapping)
                result.update(mm)

            expected_output_file = case.get("expected_output_file")
            if expected_output_file:
                output_path = resolve_path(str(expected_output_file), manifest_dir, repo_root)
                exp_headers, exp_rows = read_expected_csv(output_path)
                om = eval_output_metrics(predicted_rows, exp_headers, exp_rows)
                result.update(om)

            if args.save_standardized_dir:
                save_dir = args.save_standardized_dir.resolve()
                save_dir.mkdir(parents=True, exist_ok=True)
                ext = input_file.suffix.lower().lstrip(".")
                out_file = save_dir / f"{input_file.stem}__{ext}__{table_for_run}.csv"
                std.write_csv(out_file, predicted_rows, target_cols)
                result["standardized_output_file"] = str(out_file)

            sq = eval_schema_quality(predicted_rows, schema_detailed[table_for_run])
            result.update(sq)
        except Exception as exc:
            result["status"] = "error"
            result["error"] = str(exc)
        case_results.append(result)

    executed = [r for r in case_results if r.get("status") == "ok"]
    failed = [r for r in case_results if r.get("status") != "ok"]
    table_scored = [r for r in case_results if r.get("expected_table") is not None]

    def mean_of(key: str) -> float | None:
        vals = [r.get(key) for r in executed if r.get(key) is not None]
        return safe_ratio(sum(vals), len(vals))

    summary = {
        "cases_total": len(case_results),
        "cases_executed": len(executed),
        "cases_failed": len(failed),
        "table_accuracy": safe_ratio(
            sum(1 for r in table_scored if r.get("table_match") is True),
            len(table_scored),
        ),
        "mean_mapping_f1": mean_of("mapping_f1"),
        "mean_cell_accuracy": mean_of("cell_accuracy"),
        "mean_type_valid_rate": mean_of("type_valid_rate"),
        "mean_required_fill_rate": mean_of("required_fill_rate"),
    }

    output = {"summary": summary, "cases": case_results}
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    if args.out_md:
        args.out_md.parent.mkdir(parents=True, exist_ok=True)
        args.out_md.write_text(render_markdown(summary, case_results), encoding="utf-8")


if __name__ == "__main__":
    main()
