from scripts import upserts

run_modules = [
    ("Upsert Aircraft Models", upserts.upsert_aircraft_models),
    ("Upsert Aircraft Models", upserts.upsert_airports),
    ("Upsert Flights", upserts.upsert_flights),
    ("Simulate Passports", upserts.upsert_passport),
    ("Simulate Tickets", upserts.upsert_tickets),
]

def main():
    for name, module in run_modules:
        try:
            print(f"🔄 Running: {name}")
            module.main()
            print(f"✅ Completed: {name}\n")
        except Exception as e:
            print(f"❌ Error in {name}: {e}\n")

if __name__ == "__main__":
    main()