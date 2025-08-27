# 📂 Source

The `source/` folder contains **raw and external reference data** used in the CPH Airport case.  
It serves as the **input layer** for simulations, upserts, and reporting.

---

## Contents

- External datasets (e.g., aircraft reference data, airport codes)  
- JSON/CSV files used to seed or enrich the simulation  
- Any static lookup tables required for ETL pipelines  

---

## Notes

- Files here are **read-only inputs** — not modified during pipeline runs.  
- Used mainly by scripts in `code/` for **enrichment** and **data consistency**.  
- Keep large external datasets out of version control when possible; store only required samples or references.