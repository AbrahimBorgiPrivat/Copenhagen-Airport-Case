# 📊 CPH Airport — Passenger & Flight Insights (Reports)

This folder contains the **Power BI demo reports** and supporting **resources** for the CPH Airport case.

The reports are designed to demonstrate insights into **passenger flow, seat occupancy, and airport operations** using simulated and enriched data.

---

## 📂 Structure

rapport/
├── CPH Airport – Passenger & Flight Insights (Power BI Demo).Report
├── CPH Airport – Passenger & Flight Insights (Power BI Demo).SemanticModel
├── CPH Airport – Passenger & Flight Insights (Power BI Demo).pbip
├── resources/
│   ├── images/
│   └── CPH_Airport_theme.json
└── README.md

---

## 🎨 Reports

- **CPH Airport – Passenger & Flight Insights (Power BI Demo).pbip**  
  Power BI Project file (PBIP) representing the full report definition (model + visuals).

- **CPH Airport – Passenger & Flight Insights (Power BI Demo).Report**  
  Report definition component of the PBIP project.

- **CPH Airport – Passenger & Flight Insights (Power BI Demo).SemanticModel**  
  Data model definition component of the PBIP project.

---

## 🖼️ Resources

- **Theme (.json)**  
  `CPH_Airport_theme.json` defines the **color palette, fonts, and visual style** for Power BI, ensuring consistent look-and-feel across dashboards.

- **Images**  
  Contain visual assets (icons, illustrations, backgrounds) referenced by the report.

---

## 🚀 Usage

1. Open the `.pbip` project in **Power BI Desktop (Preview for PBIP)** or import `.Report` and `.SemanticModel` if working with separate components.  
2. Ensure the **resources/** folder is preserved so images and the theme load correctly.  
3. Apply the **theme** via *View → Themes → Browse for themes* if not auto-applied.  
4. Refresh data connections to simulate real insights from the `cph_airport` schema.

---

## 📊 Insights Covered

- Seat capacity vs. seat bought rate  
- Passenger check-in distribution (online vs. onsite)  
- Passenger flow through security (timing & bottlenecks)  
- Aircraft utilization and flight departures  
- KPIs for airports, passengers, and flights  

---

✈️ This section provides the **visual storytelling** layer of the CPH Airport case — bringing the simulated and enriched datasets into a business intelligence context.
