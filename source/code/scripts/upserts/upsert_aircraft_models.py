from utils.orchestrator import load_json_file, update_insert_dw, ensure_table_structure
from utils.db_types import TEXT, BOOLEAN, BIGINT, DOUBLE
from utils.path_config import FILES_DIR

def upsert(create_table_if_not_exist=False):
    file_path = FILES_DIR
    file_name = "aircraft_models_structured_full.json"
    products = load_json_file(file_path,file_name)
    db_name='cph_airport'
    schema_name='cph_airport'
    table_name='aircraft_models'
    fields_dict = {
        'aircraft_model': {"type": TEXT(), "primary_key": True, "autoincrement": False},
        'manufacturer': {"type": TEXT()},
        'country': {"type": TEXT()},
        'seats': {"type": BIGINT()},
        'range_km': {"type": BIGINT()},
        'engine_type': {"type": TEXT()},
        'icao_code': {"type": TEXT()},
        'iata_code': {"type": TEXT()},
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
                     new_data=products,
                     pk=pk,
                     update_fields=update_fields,
                     not_included_in_update_fields=[])
    return products

def main(create_table_if_not_exist=False):
    return upsert(create_table_if_not_exist=create_table_if_not_exist)

if __name__ == "__main__":
    main()