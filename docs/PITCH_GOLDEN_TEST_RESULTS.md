# Golden Test Results (Pitch Version)

## Scope
- Primary gold set: manually curated 15-case benchmark (`pipeline/gold/manual15`) across CSV/XLSX/PDF with explicit expected mappings and outputs.
- Extended benchmark: 50-case silver pack (`pipeline/gold/pitch50`) for scale and robustness checks.
- SQL support: dedicated SQL `INSERT INTO ... VALUES ...` smoke test.
- Model under test: `llama3.2:latest` (local Ollama, offline-capable).

## Manual Gold (15 cases)
- Baseline executed: `15/15` (failed: `0`)
- LLM runs executed: `15/15` in each of 3 runs

| Metric | Baseline (No LLM) | LLM Avg (3 runs) | LLM Std Dev | Delta (LLM - Baseline) |
|---|---:|---:|---:|---:|
| Table routing accuracy | 100.00% | 100.00% | 0.00% | 0.00% |
| Mapping F1 | 99.39% | 99.43% | 0.04% | 0.03% |
| Cell accuracy | 98.89% | 99.26% | 0.52% | 0.37% |
| Schema type-valid rate | 94.79% | 94.81% | 0.02% | 0.02% |
| Required-fill rate | 100.00% | 100.00% | 0.00% | 0.00% |

## Scale Benchmark (50-case silver pack)
- Note: expected artifacts are auto-generated bootstrap expectations (silver), used for robustness/stability testing at scale.
- Cases executed: `50/50` (failed: `0`)

| Metric | Baseline (No LLM) | LLM (Prompted) | Delta (LLM - Baseline) |
|---|---:|---:|---:|
| Table routing accuracy | 100.00% | 100.00% | 0.00% |
| Mapping F1 | 100.00% | 100.00% | 0.00% |
| Cell accuracy | 100.00% | 100.00% | 0.00% |
| Schema type-valid rate | 92.13% | 92.13% | 0.00% |
| Required-fill rate | 37.50% | 37.50% | 0.00% |

## SQL Input Smoke Test
- Status: `ok`
- Detected target table: `tbImportLabsData`
- Rows in/out: `2/2`
- Mapped columns: `6`
- Source profile: `{'format': 'sql', 'encoding': 'utf-8-sig', 'insert_statements': 1, 'sql_table': 'synth_labs', 'sql_tables_found': ['synth_labs']}`

## Repro Commands
```bash
PY=/Users/vicentegallardo/Desktop/_StartHack/epaCC-START-Hack-2026/.venv/bin/python
cd /Users/vicentegallardo/Desktop/_StartHack/Start-Hack-HealthCare-Challenge

# 15-case manual gold (build once)
$PY pipeline/build_manual_gold15.py --repo-root . --data-root /Users/vicentegallardo/Desktop/_StartHack/epaCC-START-Hack-2026 --output-dir pipeline/gold/manual15 --max-rows 3
$PY pipeline/eval.py --schema-sql database/sqlserver/CreateImportTables.sql --repo-root . --data-root /Users/vicentegallardo/Desktop/_StartHack/epaCC-START-Hack-2026 --manifest pipeline/gold/manual15/manifest.json --out-json pipeline/out/eval_pitch_manual15_no_llm.json --out-md pipeline/out/eval_pitch_manual15_no_llm.md --no-llm
$PY pipeline/eval.py --schema-sql database/sqlserver/CreateImportTables.sql --repo-root . --data-root /Users/vicentegallardo/Desktop/_StartHack/epaCC-START-Hack-2026 --manifest pipeline/gold/manual15/manifest.json --out-json pipeline/out/eval_pitch_manual15_llm_run1.json --out-md pipeline/out/eval_pitch_manual15_llm_run1.md --model llama3.2:latest
$PY pipeline/eval.py --schema-sql database/sqlserver/CreateImportTables.sql --repo-root . --data-root /Users/vicentegallardo/Desktop/_StartHack/epaCC-START-Hack-2026 --manifest pipeline/gold/manual15/manifest.json --out-json pipeline/out/eval_pitch_manual15_llm_run2.json --out-md pipeline/out/eval_pitch_manual15_llm_run2.md --model llama3.2:latest
$PY pipeline/eval.py --schema-sql database/sqlserver/CreateImportTables.sql --repo-root . --data-root /Users/vicentegallardo/Desktop/_StartHack/epaCC-START-Hack-2026 --manifest pipeline/gold/manual15/manifest.json --out-json pipeline/out/eval_pitch_manual15_llm_run3.json --out-md pipeline/out/eval_pitch_manual15_llm_run3.md --model llama3.2:latest

# 50-case scale benchmark
$PY pipeline/build_eval_pack.py --schema-sql database/sqlserver/CreateImportTables.sql --repo-root . --data-root /Users/vicentegallardo/Desktop/_StartHack/epaCC-START-Hack-2026 --size 50 --max-expected-rows 3 --output-dir pipeline/gold/pitch50 --no-llm
$PY pipeline/eval.py --schema-sql database/sqlserver/CreateImportTables.sql --repo-root . --data-root /Users/vicentegallardo/Desktop/_StartHack/epaCC-START-Hack-2026 --manifest pipeline/gold/pitch50/manifest.json --out-json pipeline/out/eval_pitch50_no_llm.json --out-md pipeline/out/eval_pitch50_no_llm.md --no-llm
$PY pipeline/eval.py --schema-sql database/sqlserver/CreateImportTables.sql --repo-root . --data-root /Users/vicentegallardo/Desktop/_StartHack/epaCC-START-Hack-2026 --manifest pipeline/gold/pitch50/manifest.json --out-json pipeline/out/eval_pitch50_llm_prompt_run1.json --out-md pipeline/out/eval_pitch50_llm_prompt_run1.md --model llama3.2:latest --prompt-template pipeline/prompts/column_mapper_prompt.txt
```
