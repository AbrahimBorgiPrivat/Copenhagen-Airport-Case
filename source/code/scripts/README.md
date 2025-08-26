# scripts — Main Simulation & ETL Orchestration Entry Points

This folder contains high-level orchestration scripts used to simulate, ingest, and upsert data for the Circle K loyalty and transactions platform.

Each script serves as an entry point to run a series of modules — such as data simulation (customers, cards, transactions), geographic enrichment (DAR/DAGI), and loading into a PostgreSQL environment.

---

## Script Categories

### `run_simulation_pipeline.py`
- Orchestrates the full data simulation pipeline:
  - Products
  - Campaigns
  - Segments
  - Customers
  - Cards
  - Transactions
- Pulls from `simulations` and `upserts`.

### `run_datafordeler_pipeline.py` (example)
- Loads public address and registry data from DAR and DAGI into PostgreSQL.
- Reads files from `resource/json` and uses logic in `pipelines`.

---

## Script Pattern

Each main script uses the following modular, fault-tolerant pattern:

```python
run_modules = [
    ("Step Description", module_reference),
    ...
]

for name, module in run_modules:
    try:
        print(f"🔄 Running: {name}")
        module.main()
        print(f"✅ Completed: {name}\n")
    except Exception as e:
        print(f"❌ Error in {name}: {e}\n")
```

This allows one failed step to be logged without halting the full pipeline.

---

## Running a Script

Run any orchestrator using Poetry:

```bash
poetry run python scripts/pipelines/cirkleKsimulations.py
```

Replace the filename as needed to run ingestion or enrichment pipelines.

---

## Notes

- Scripts are designed to run standalone or as a sequence.
- You can comment out individual modules in the `run_modules` list for fast dev cycles.
- Logging can be extended to write to log tables or audit trails.
- The modular layout supports dependency tracking and reusability across environments.

---
