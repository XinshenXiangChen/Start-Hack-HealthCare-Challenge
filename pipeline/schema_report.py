#!/usr/bin/env python3
"""Extract target DB schema from CreateImportTables.sql."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


CREATE_RE = re.compile(r"^\s*create\s+table\s+([A-Za-z0-9_]+)\s*$", re.IGNORECASE)


def parse_schema(sql_path: Path) -> dict[str, list[dict[str, str | bool]]]:
    schema: dict[str, list[dict[str, str | bool]]] = {}
    current_table: str | None = None

    for raw_line in sql_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("/*"):
            continue

        create_match = CREATE_RE.match(line)
        if create_match:
            current_table = create_match.group(1)
            schema[current_table] = []
            continue

        if current_table is None:
            continue

        if line.startswith(")"):
            current_table = None
            continue

        if line.lower().startswith("constraint"):
            continue

        clean = line.rstrip(",")
        parts = clean.split()
        if len(parts) < 2:
            continue

        col_name = parts[0]
        col_type = parts[1]
        nullable = "not null" not in clean.lower()
        schema[current_table].append(
            {"name": col_name, "type": col_type, "nullable": nullable}
        )

    return schema


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", required=True, type=Path)
    parser.add_argument("--out-json", required=True, type=Path)
    parser.add_argument("--out-md", required=True, type=Path)
    args = parser.parse_args()

    schema = parse_schema(args.sql)
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(schema, indent=2), encoding="utf-8")

    lines: list[str] = ["# DB Target Schema", ""]
    for table, cols in schema.items():
        req_count = sum(1 for c in cols if not c["nullable"])
        lines.append(f"## {table}")
        lines.append(f"- columns: {len(cols)}")
        lines.append(f"- required (NOT NULL): {req_count}")
        lines.append("")
        lines.append("| Column | Type | Nullable |")
        lines.append("|---|---|---|")
        for col in cols:
            lines.append(
                f"| `{col['name']}` | `{col['type']}` | "
                f"{'YES' if col['nullable'] else 'NO'} |"
            )
        lines.append("")

    args.out_md.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
