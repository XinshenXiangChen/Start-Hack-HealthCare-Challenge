#!/usr/bin/env python3
"""
Print contents of the local SQLite DB created by `dashboard/db_sqlite.py`.

By default:
  - prints all tables
  - shows first N rows per table
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "runtime" / "standardized.sqlite"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to standardized.sqlite")
    parser.add_argument("--table", default=None, help="Only print this table")
    parser.add_argument("--limit", type=int, default=20, help="Rows per table to print")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"SQLite DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    tables = [r["name"] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
    # Filter out sqlite internal tables if any.
    tables = [t for t in tables if not t.startswith("sqlite_")]
    if args.table:
        if args.table not in tables:
            raise SystemExit(f"Table '{args.table}' not found. Available: {tables}")
        tables = [args.table]

    if not tables:
        print("No tables found in DB.")
        return

    for t in tables:
        print("\n" + "=" * 80)
        print(f"TABLE: {t}")
        print("=" * 80)
        rows = cur.execute(f"SELECT * FROM [{t}] LIMIT ?", (args.limit,)).fetchall()
        if not rows:
            print("(no rows)")
            continue

        # Print header
        cols = rows[0].keys()
        print("\t".join(str(c) for c in cols))
        for r in rows:
            print("\t".join("" if r[c] is None else str(r[c]) for c in cols))

    conn.close()


if __name__ == "__main__":
    main()

