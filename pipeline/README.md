# Pipeline Module

This folder contains the core standardization and evaluation utilities.

Live dashboard app is under `dashboard/` at repo root and can be launched with:

```bash
python pipeline.py dashboard
```

Cross-file linking in dashboard mode:

- During ingestion, standardized rows are linked across files by shared entity IDs (`coPatient_id`, `coCaseId`, `coCaseIdAlpha`, `coEncounter_id`).
- When explicit IDs are missing, optional LLM fallback can match rows to existing entities to reduce data isolation.
- Linked index/state is persisted at `runtime/linked/entity_index.json`.
- Per-table linked exports are written to `runtime/linked/<table>__linked.csv`.
- Linked CSV rows include continuity/provenance columns like `_eid`, `_source_file`, `_source_job_id`, `_source_table`, `_source_row_index`, `_match_method`.
- API endpoints:
  - `GET /api/entities`
  - `GET /api/entity/{entity_id}`
  - `GET /api/download-linked/{table}`

## Components

- `standardize.py`: normalize mixed CSV/XLSX/PDF/SQL data into SQL target-table CSVs
- `schema_report.py`: parse SQL schema and export markdown/json reports
- `eval.py`: evaluate table routing, mapping quality, output quality, and schema validity
- `build_eval_pack.py`: generate large benchmark packs
- `build_manual_gold15.py`: generate curated 15-case benchmark subset

## Dependencies

Install from repo root:

```bash
pip install -r requirements.txt
```

## Prompt

LLM fallback prompt template:

- `prompts/column_mapper_prompt.txt`

## Gold / Benchmark Inputs

- `gold/example_manifest.json`
- `gold/examples/*.json|*.csv`

## Notes

- `build_manual_gold15.py`, `build_eval_pack.py`, and `eval.py` support `--data-root` so benchmarks can run against datasets stored outside this repository.
- `standardize.py` supports `--iid-dictionary` for `IID-SID-ITEM.csv` lookup (`SID/ItemName -> IID -> co...`), mainly for AC mappings.
