# CPH AIRPORT — Simulation & ETL Orchestration

This repository contains the main orchestration scripts used to **simulate**, **ingest**, and **upsert** data for the **CPH Airport case**.

The goal is to generate realistic flight and passenger data, connect it to airports, and prepare the dataset for analysis in a PostgreSQL data warehouse.

---

## 📂 Script Categories

### `api/`
- Wrappers for external APIs.
- **`airports_api.py`** — fetches airport information from OpenDataSoft and other sources.  
- **`flights_api.py`** — fetches scheduled flights and enriches them with external APIs.

### `pipelines/`
- High-level orchestration pipelines combining simulation and upserts.  
- **`cph_case_pipeline.py`** — entry point to run the full CPH Airport simulation and load it into PostgreSQL.

### `simulations/`
- Contains logic to simulate synthetic datasets.  
- **`flights_tickets.py`** — generates synthetic flights and ticket bookings.  
- **`passport.py`** — generates synthetic passport/customer data.

### `upserts/`
- ETL logic for inserting/updating into the `cph_airport` schema in PostgreSQL.  
- **`upsert_aircraft_models.py`** — loads metadata about aircraft models.  
- **`upsert_airports.py`** — loads airport data.  
- **`upsert_flights.py`** — loads flight records.  
- **`upsert_passport.py`** — loads passenger/passport data.  
- **`upsert_tickets.py`** — loads ticket and check-in/security events.

---

## ⚙️ Script Pattern

Each orchestrator follows a modular pattern:

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

This ensures one failed step does not stop the entire pipeline.

---

## ▶️ Running a Pipeline

Run any orchestrator via Poetry:

```bash
poetry run python scripts/pipelines/cph_case_pipeline.py
```

---

## 📝 Notes

- Pipelines can be run **standalone** or as a full sequence.
- Comment out modules in the `run_modules` list to speed up dev cycles.
- Logging can be extended to tables or audit trails in PostgreSQL.
- Modular layout ensures **reusability** across pipelines and environments.

---

## 📊 Data Model Overview

The simulation produces and loads:

- **Airports** (from APIs and open datasets)  
- **Aircraft models** (with seats, range, engine, ICAO/IATA codes)  
- **Flights** (scheduled + synthetic)  
- **Passports/Customers** (synthetic passenger pool)  
- **Tickets** (linking flights and passengers, with check-in/security events)  

This supports analysis of **passenger flow, flight occupancy, and airport operations**.

---