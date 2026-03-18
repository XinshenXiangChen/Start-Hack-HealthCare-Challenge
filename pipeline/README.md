# Pipeline Module

This folder contains the core standardization and evaluation utilities.

## Components

- `standardize.py`: normalize mixed CSV/XLSX/PDF data into SQL target-table CSVs
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
