# Start Hack Healthcare Challenge

This branch contains the data-standardization pipeline for the Start Hack healthcare mapping challenge.

## Technology Alignment

This implementation explicitly covers the challenge technology requirements:

- AI-driven pattern recognition and mapping:
  - local LLM-based column-mapping fallback (Ollama/Llama)
- NLP for free text:
  - free-text extraction from nursing PDF reports
- Autonomous data agents and rule-based/learning mapping logic:
  - deterministic alias/rule engine first
  - model fallback only for unresolved mappings
- SQL database integration (compatible with epaSOLUTIONS target model):
  - SQL Server schema/scripts under `database/sqlserver/`
- On-prem / offline capable deployment:
  - local model runtime support
  - no cloud dependency required for pipeline execution

## Repository Layout

- `pipeline.py`: CLI entrypoint for schema export, standardization, and evaluation
- `pipeline/`: standardization, benchmark, and evaluation scripts
- `database/sqlserver/`: SQL Server target schema scripts

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Generate schema report:

```bash
python pipeline.py schema
```

Run standardization:

```bash
python pipeline.py standardize \
  --input-dir "data/input" \
  --output-dir "pipeline/out/run_001" \
  --model llama3.2:latest \
  --iid-dictionary "/absolute/path/to/IID-SID-ITEM.csv"
```

Supported input formats for `--input-dir`:

- `.csv`
- `.xlsx`
- `.pdf`
- `.sql` (SQL dump files with `INSERT INTO ... VALUES ...`)

IID/SID dictionary support:

- If you provide `IID-SID-ITEM.csv`, the pipeline can resolve `SID` or item-name headers to canonical `IID`.
- For AC files, this enables `SID/ItemName -> IID -> co...` mapping even when source headers do not include direct `E*_I_*`.

Run evaluation:

```bash
python pipeline.py eval \
  --manifest pipeline/gold/example_manifest.json \
  --out-json pipeline/out/eval.json \
  --out-md pipeline/out/eval.md \
  --model llama3.2:latest
```

Run live dashboard:

```bash
python pipeline.py dashboard --host 0.0.0.0 --port 8000
```

Dashboard behavior:

- Watches `runtime/incoming` for new files.
- Queues each file and processes it with worker(s) (FIFO queue).
- Standardizes each file to `runtime/standardized`.
- Moves processed inputs to `runtime/processed`.
- Builds a cross-file entity index (patient/case/encounter linking) under `runtime/linked`.
- Linked rows include provenance and continuity fields (`_eid`, `_source_file`, `_source_job_id`, `_source_table`, `_source_row_index`, `_match_method`).
- Provides upload + status UI at `http://localhost:8000`.

Queue controls (optional env vars):

- `PIPELINE_WORKER_CONCURRENCY` (default `1`): number of concurrent workers.
- `PIPELINE_SCAN_SECONDS` (default `5`): watcher scan interval.
- `PIPELINE_IID_DICTIONARY`: explicit path to `IID-SID-ITEM.csv` (dashboard/runtime).
- `PIPELINE_LINKED_DIR` (default `runtime/linked`): output directory for cross-file linked artifacts.
- `PIPELINE_ENTITY_SAMPLE_LIMIT` (default `40`): max sample rows stored per linked entity for UI/API previews.
- `PIPELINE_AI_LINKING` (default `1`): enable LLM fallback to match disconnected rows when explicit IDs are missing.
- `PIPELINE_AI_LINK_TIMEOUT` (default `15`): timeout (seconds) per AI entity-link call.
- `PIPELINE_AI_LINK_MAX_CALLS_PER_JOB` (default `40`): cap AI linking calls per file/job.
