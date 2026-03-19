from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine
from sqlalchemy.engine import Engine

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "runtime" / "standardized.sqlite"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

DB_URL = f"sqlite:///{DB_PATH}"

engine: Engine = create_engine(DB_URL, echo=False, future=True)
metadata = MetaData()


def get_or_create_table(table_name: str, columns: list[str]) -> Table:
    """
    Ensure a SQLite table exists with at least the columns from the standardized CSV.

    - Adds an auto-incrementing integer primary key `id`
    - Stores all CSV columns as TEXT
    """

    if table_name in metadata.tables:
        return metadata.tables[table_name]

    cols: list[Any] = [Column("id", Integer, primary_key=True, autoincrement=True)]
    cols += [Column(c, String, nullable=True) for c in columns]

    table = Table(table_name, metadata, *cols)
    metadata.create_all(engine)
    return table


def load_standardized_csv_into_sqlite(csv_path: Path, table_name: str) -> int:
    """
    Load all rows from a standardized CSV into a SQLite table using SQLAlchemy Core.

    Returns the number of inserted rows.
    """

    import csv

    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        table = get_or_create_table(table_name, headers)

        rows: list[dict[str, Any]] = []
        for row in reader:
            rows.append({k: (v if v != "" else None) for k, v in row.items()})

    with engine.begin() as conn:
        if rows:
            conn.execute(table.insert(), rows)
    return len(rows)

