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
  - SQL Server schema/scripts under `database/sqlserver/` and mock database in runtime folder (appears once the code is run)
- On-prem / offline capable deployment:
  - local model runtime support (via Ollama)
  - no cloud dependency required for pipeline execution

## Repository Layout

- `pipeline.py`: CLI entrypoint for schema export, standardization, and evaluation
- `pipeline/`: standardization, benchmark, and evaluation scripts
- `database/sqlserver/`: SQL Server target schema scripts

## Quick Start
First install docker 

Run evaluation:



## Docker (recommended)

Start the full stack (app + Ollama) from your terminal:

```bash
docker compose build
docker compose up
```

After starting, open the dashboard at:
`http://127.0.0.1:8000`

One-click scripts are in `BUILD_SCRIPTS/`:
- Linux/macOS: `BUILD_SCRIPTS/build_docker_linux.sh` / `BUILD_SCRIPTS/build_docker_macos.sh`
- Windows: `BUILD_SCRIPTS/build_docker_windows.bat` (double-click)

### Do you need Ollama installed locally?
No. The Docker stack starts an `ollama` container and pulls the model (default `llama3.2:latest`) automatically into a Docker volume.

If you don’t want any LLM usage, you can run with `PIPELINE_NO_LLM=1` (then the pipeline will rely on the rule-based mapping only).

## Troubleshooting

If anything fails or you want a completely clean rebuild:

```bash
docker compose down --volumes --remove-orphans
docker compose build --no-cache
docker compose up
```

The webpage can also be launched via (but the setup with the local LLM will NOT work): 

```bash
python pipeline.py dashboard --host 0.0.0.0 --port 8000
```
