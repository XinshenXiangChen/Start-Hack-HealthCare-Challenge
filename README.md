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
  --model llama3.2:latest
```

Run evaluation:

```bash
python pipeline.py eval \
  --manifest pipeline/gold/example_manifest.json \
  --out-json pipeline/out/eval.json \
  --out-md pipeline/out/eval.md \
  --model llama3.2:latest
```
