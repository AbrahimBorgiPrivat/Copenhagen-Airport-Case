# ✈️ CPH AIRPORT — Data Simulation & ETL Framework

This repository contains the full **simulation and ETL pipeline** for the **CPH Airport case**.  
It is designed to simulate flights, passengers, and tickets, fetch real airport data from external APIs, and upsert everything into a PostgreSQL data warehouse.

---

## 📦 Project Structure

code/
├── .venv/                  # Virtual environment (Poetry-managed)
├── classes/                # Core classes and DB engine
│   ├── db_engine.py        # Connection handler for PostgreSQL
│   └── __init__.py
│
├── scripts/                # Main orchestrators & ETL entry points
│   ├── api/                # API connectors
│   │   ├── airports_api.py # Fetches airport metadata
│   │   ├── flights_api.py  # Fetches/enriches flight schedules
│   │   └── __init__.py
│   │
│   ├── pipelines/          # Orchestration pipelines
│   │   ├── cph_case_pipeline.py # Runs the full CPH case ETL
│   │   └── __init__.py
│   │
│   ├── simulations/        # Synthetic data generators
│   │   ├── flights_tickets.py   # Simulates flights & ticket bookings
│   │   ├── passport.py          # Simulates passengers/passports
│   │   └── __init__.py
│   │
│   ├── upserts/            # ETL loaders for PostgreSQL
│   │   ├── upsert_aircraft_models.py
│   │   ├── upsert_airports.py
│   │   ├── upsert_flights.py
│   │   ├── upsert_passport.py
│   │   ├── upsert_tickets.py
│   │   └── __init__.py
│   │
│   └── __init__.py
│
├── utils/                  # Shared utilities & config
│   ├── api_urls.py         # API endpoints config
│   ├── db_types.py         # DB schema/type definitions
│   ├── env.py              # Environment variables
│   ├── orchestrator.py     # Common ETL orchestration logic
│   ├── path_config.py      # File path configuration
│   └── __init__.py
│
├── test_poetry_script.py   # Example script for testing Poetry runs
├── pyproject.toml          # Poetry dependencies & project config
├── poetry.lock             # Locked dependency versions
├── README.md               # Project documentation
└── .env.example.txt        # Example environment variables

---

## ⚙️ How It Works

1. **Classes**  
   Provide core building blocks, e.g. the **database engine**.

2. **Simulations**  
   Generate synthetic data:
   - Passports (customers)
   - Flights
   - Tickets (check-in, security flow)

3. **APIs**  
   Connect to external open datasets (e.g., **OpenDataSoft Airports**) to fetch **real airport metadata** and enrich simulations.

4. **Upserts**  
   Each entity (airports, aircraft models, flights, passengers, tickets) has its own loader that **inserts or updates** data into the `cph_airport` schema in PostgreSQL.

5. **Pipelines**  
   Tie together simulations, APIs, and upserts into a **single orchestrated ETL run**.

6. **Utils**  
   Reusable components (configs, SQLAlchemy types, API URLs, orchestrator logic).

---

## ▶️ Running the Pipeline

1. **Install dependencies** with [Poetry](https://python-poetry.org/):
   ```bash
   poetry install
   ```

2. **Activate the environment**:
   ```bash
   poetry shell
   ```

3. **Run the full pipeline**:
   ```bash
   poetry run python scripts/pipelines/cph_case_pipeline.py
   ```

This will:
- Simulate passengers, flights, and tickets
- Fetch airport data via APIs
- Upsert everything into the `cph_airport` schema

---

## 📝 Development Notes

- Update **`.env`** from `.env.example.txt` with DB credentials and API keys.
- Use the modular `orchestrator.py` pattern:
  ```python
  run_modules = [
      ("Generate Passports", simulations.passport),
      ("Simulate Flights & Tickets", simulations.flights_tickets),
      ("Upsert Flights", upserts.upsert_flights),
      ...
  ]
  ```
- Each step logs progress and errors without halting the entire pipeline.
- Designed for **incremental dev cycles**: comment out modules to run partial flows.

---

## 📊 Data Model

- **Airports** → linked to **Flights**
- **Aircraft models** → define seat capacity, range, engine type
- **Flights** → scheduled and simulated departures
- **Passports (customers)** → synthetic passengers
- **Tickets** → link passengers to flights, with check-in & security timestamps

---

✈️ This framework provides a realistic sandbox for **airport operations analytics** (passenger flow, capacity, scheduling) in PostgreSQL + Power BI.
