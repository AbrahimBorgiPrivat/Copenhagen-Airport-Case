from utils.orchestrator import update_insert_dw, ensure_table_structure
from utils.db_types import TEXT, BIGINT, TIMESTAMP
from scripts.simulations import flights_tickets
from tqdm import tqdm

def upsert(create_table_if_not_exist=False, chunk_size=100000):
    flights = flights_tickets.main()
    db_name='cph_airport'
    schema_name='cph_airport'
    table_name='tickets'
    fields_dict = {
        "unique_id": {"type": TEXT(), "primary_key": True, "autoincrement": False},
        "transaction_id": {"type": TEXT()},
        "seat_number": {"type": BIGINT()},
        "passport_number": {"type": TEXT()},
        "check_in_type": {"type": TEXT()},
        "checkin_time": {"type": TIMESTAMP()},
        "passed_security_time": {"type": TIMESTAMP()},
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
    total = len(flights)
    for start in tqdm(range(0, total, chunk_size)):
        end = start + chunk_size
        batch = flights[start:end]
        update_insert_dw(
            db_name=db_name,
            schema=schema_name,
            table=table_name,
            new_data=batch,
            pk=pk,
            update_fields=update_fields,
            not_included_in_update_fields=[]
        )

    return flights

def main(create_table_if_not_exist=False):
    return upsert(create_table_if_not_exist=create_table_if_not_exist)

if __name__ == "__main__":
    main(True)