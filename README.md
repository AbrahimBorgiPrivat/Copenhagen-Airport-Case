# ✈️ Copenhagen Airport — Simulation, ETL & Reporting

This project demonstrates a full **data pipeline and analytics case** for **Copenhagen Airport**.  
It covers **data simulation, ETL, enrichment, and reporting** to create a "realistic" environment for analyzing passenger flow, flights, and airport operations.

---

## 🎯 Purpose

- Simulate **passenger, passport, and ticket data**  
- Enrich flights and airports with **real reference data**  
- Load everything into a **PostgreSQL data warehouse**  
- Build **Power BI dashboards** for insights on:
  - Passenger check-in behavior (online vs. onsite)  
  - Security flow and bottlenecks  
  - Seat occupancy rates  
  - Aircraft and airport utilization  

---

## 📂 Project Structure (paste-ready)

```text
COPENHAGEN AIRPORT/
├─ docs/                 # Documentation, design notes, supporting material
├─ resource/             # Assets and themes used in reporting
└─ source/               # Root folder for code and rapport
   ├─ code/              # Core Python codebase (simulation + ETL)
   └─ rapport/           # Power BI dashboards & resources
```

---

## ⚙️ How It Works

1. **Simulation** (`source/code/scripts/simulations/`)  
   Generates synthetic data:
   - Passengers & passports  
   - Flights (scheduled & simulated)  
   - Tickets (with check-in and security times)

2. **APIs** (`source/code/scripts/api/`)  
   Fetch **real-world data** from OpenDataSoft and similar sources for airports and flight metadata.

3. **Upserts** (`source/code/scripts/upserts/`)  
   Load everything into PostgreSQL in the `cph_airport` schema.

4. **Pipelines** (`source/code/scripts/pipelines/`)  
   Orchestrate the entire flow — simulation → enrichment → database load.

5. **Reporting** (`source/rapport/`)  
   Power BI dashboards with **visual storytelling** based on the simulated and enriched datasets.

---

## ▶️ Running the Project

1. Install dependencies with Poetry:
   ```bash
   poetry install
   ```

2. Configure environment variables in `.env` (based on `.env.example.txt`).

3. Run the full ETL pipeline:
   ```bash
   poetry run python source/code/scripts/pipelines/cph_case_pipeline.py
   ```

4. Open the Power BI `.pbip` project in **Power BI Desktop** to view dashboards.

---

## 📊 Outputs

- **PostgreSQL schema (`cph_airport`)**  
  - Airports  
  - Aircraft models  
  - Flights  
  - Passports  
  - Tickets (linking passengers & flights)

- **Power BI Dashboards (source/rapport/)**  
  Contain:
  - KPIs for passengers, flights, and seat occupancy  
  - Passenger check-in vs. security timing visuals  
  - Utilization of aircraft by airline, region, and size  
  - Geographic coverage maps of flights and passengers  
  - Themed reports with custom visuals and icons (from `resources/`)  

---

✈️ With this framework, Copenhagen Airport operations can be **simulated, enriched, stored, and analyzed** end-to-end.
