import requests
from utils.api_urls import OPENDATASOFT

def safe_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

def fetch_airports(limit=100, total_count: int = None) -> list[dict]:
    """
    Fetch all airports from OpenDataSoft dataset, in pages of size `limit`.
    Returns a list of dicts (flattened records).
    """
    base_url = OPENDATASOFT
    dataset = "airports-code@public"
    all_records: list[dict] = []

    resp = requests.get(base_url, params={"dataset": dataset, "rows": 1})
    resp.raise_for_status()
    if total_count is None:
        total_count = resp.json().get("nhits", 0)
    print(f"Total records reported: {total_count}")

    offset = 0
    while offset < total_count:
        resp = requests.get(
            base_url,
            params={
                "dataset": dataset,
                "rows": limit,
                "start": offset,
                "sort": "column_1"
            },
        )
        resp.raise_for_status()
        data = resp.json()
        records = data.get("records", [])
        for rec in records:
            row = rec.get("fields", {})
            all_records.append({
                "icao": row.get("column_1"),                       # TEXT
                "name": row.get("airport_name"),                   # TEXT
                "city": row.get("city_name"),                      # TEXT
                "country": row.get("country_name"),                # TEXT
                "country_code": row.get("country_code"),           # TEXT
                "latitude": row.get("latitude"),                   # Numeric
                "longitude": row.get("longitude"),                 # Numeric
                "world_area_code": safe_int(row.get("world_area_code")),             # BIGINT
                "city_name_geo_name_id": safe_int(row.get("city_name_geo_name_id")), # BIGINT
                "country_name_geo_name_id": safe_int(row.get("country_name_geo_name_id")), # BIGINT
            })
        offset += limit
        print(f"Fetched {len(all_records)} / {total_count}")

    return all_records

def main():
    return fetch_airports()

if __name__ == "__main__":
    print(main()[:2])