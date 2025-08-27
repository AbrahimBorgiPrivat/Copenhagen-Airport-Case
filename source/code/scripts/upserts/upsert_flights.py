from utils.orchestrator import update_insert_dw, ensure_table_structure
from utils.db_types import TEXT, TIMESTAMP
from scripts.api import flights_api
from datetime import datetime

def upsert(create_table_if_not_exist=False,
           dt_from= datetime(2025, 7, 1),
           days = 61):
    flights = flights_api.main(dt_from= dt_from,
                              days = days)
    db_name='cph_airport'
    schema_name='cph_airport'
    table_name='flights'
    fields_dict = {
        "transaction_id": {"type": TEXT(), "primary_key": True, "autoincrement": False},
        "flight_number": {"type": TEXT()},
        "scheduled_utc": {"type": TIMESTAMP()}, 
        "airline": {"type": TEXT()},
        "airline_iata": {"type": TEXT()},
        "airline_icao": {"type": TEXT()},
        "destination": {"type": TEXT()},
        "destination_iata": {"type": TEXT()},
        "destination_icao": {"type": TEXT()},
        "scheduled_local": {"type": TIMESTAMP()},
        "revised_utc": {"type": TIMESTAMP()},
        "revised_local": {"type": TIMESTAMP()},
        "runway_utc": {"type": TIMESTAMP()},
        "runway_local": {"type": TIMESTAMP()},
        "status": {"type": TEXT()},
        "terminal": {"type": TEXT()},
        "gate": {"type": TEXT()},
        "aircraft_model": {"type": TEXT()},
        "aircraft_reg": {"type": TEXT()},
    }
    test_table_structure = ensure_table_structure(db_name=db_name,
                                schema_name=schema_name,
                                table_name=table_name,
                                fields_dict=fields_dict,
                                create_table_if_not_exist=create_table_if_not_exist)
    if not test_table_structure:
        raise RuntimeError(f"Table '{schema_name}.{table_name}' does not exist or has incorrect structure.")
    
    pk = [col for col, config in fields_dict.items() if isinstance(config, dict) and config.get("primary_key", False)]
    update_fields = [col for col in fields_dict if col not in pk]
    update_insert_dw(db_name=db_name,
                     schema=schema_name,
                     table=table_name,
                     new_data=flights,
                     pk=pk,
                     update_fields=update_fields,
                     not_included_in_update_fields=[])
    
    return flights

def main(create_table_if_not_exist=False):
    return upsert(create_table_if_not_exist=create_table_if_not_exist)

if __name__ == "__main__":
    main()