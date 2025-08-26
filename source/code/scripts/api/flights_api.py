import requests
from utils import api_urls, env
import json
import hashlib
from datetime import datetime, timedelta

def generate_transaction_id(flight: dict) -> str:
    movement = flight.get("movement", {})
    scheduled = movement.get("scheduledTime", {})
    airline = flight.get("airline", {})
    key = f"{flight.get('number', '')}_{scheduled.get('utc', '')}_{airline.get('iata', '')}_{movement.get('gate', '')}_{movement.get('terminal', '')}_{flight.get('status', '')}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()

def fetch_flights(
    code: str,
    date_from: str,
    date_to: str,
    codetype: str = "Iata",
    direction: str = "Departure",
    withLeg: bool = False,
    withCancelled: bool = True,
    withCodeshared: bool = True,
    withCargo: bool = True,
    withPrivate: bool = True,
    withLocation: bool = False,
    ):
    """
    Call AeroDataBox API for airport flights.

    :param code: Airport IATA code, e.g. "CPH"
    :param date_from: Start of range, format YYYY-MM-DDTHH:mm
    :param date_to: End of range, format YYYY-MM-DDTHH:mm
    """
    url = f"{api_urls.HISTORICAL_FLIGHTS_BASE_URL}/{codetype}/{code}/{date_from}/{date_to}"
    headers = {
        "accept": "application/json",
        "x-api-market-key": env.API_MARKET_KEY,
    }
    params = {
        "direction": direction,
        "withLeg": str(withLeg).lower(),
        "withCancelled": str(withCancelled).lower(),
        "withCodeshared": str(withCodeshared).lower(),
        "withCargo": str(withCargo).lower(),
        "withPrivate": str(withPrivate).lower(),
        "withLocation": str(withLocation).lower(),
    }
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def clean_departures_extended(raw_data: dict) -> list[dict]:
    """
    Clean and normalize flight data from AeroDataBox response.
    Adds a stable transaction_id field.
    """
    departures = raw_data.get("departures", [])
    cleaned = []

    for flight in departures:
        movement = flight.get("movement", {})
        airport = movement.get("airport", {})
        scheduled = movement.get("scheduledTime", {})
        revised = movement.get("revisedTime", {})
        runway = movement.get("runwayTime", {})
        aircraft = flight.get("aircraft", {})
        airline = flight.get("airline", {})

        cleaned.append({
            "transaction_id": generate_transaction_id(flight),
            "flight_number": flight.get("number"),
            "scheduled_utc": scheduled.get("utc"),
            "airline": airline.get("name"),
            "airline_iata": airline.get("iata"),
            "airline_icao": airline.get("icao"),
            "destination": airport.get("name"),
            "destination_iata": airport.get("iata"),
            "destination_icao": airport.get("icao"),
            "scheduled_local": scheduled.get("local"),
            "revised_utc": revised.get("utc"),
            "revised_local": revised.get("local"),
            "runway_utc": runway.get("utc"),
            "runway_local": runway.get("local"),
            "status": flight.get("status"),
            "terminal": movement.get("terminal"),
            "gate": movement.get("gate"),
            "aircraft_model": aircraft.get("model"),
            "aircraft_reg": aircraft.get("reg")
        })

    # Deduplicate by transaction_id
    deduplicated = {}
    for flight in cleaned:
        deduplicated[flight["transaction_id"]] = flight

    return list(deduplicated.values())


def main(dt_from: datetime,
        days: int = 1,
        code: str = "CPH",
        codetype: str = "Iata",
        direction: str = "Departure",
        withLeg: bool = False,
        withCancelled: bool = True,
        withCodeshared: bool = True,
        withCargo: bool = True,
        withPrivate: bool = True,
        withLocation: bool = False,
    ) -> list[dict]:
        """
        Fetch and clean flight data in 12-hour intervals starting from `dt_from` over `days` days.
        Returns a deduplicated list of cleaned flights.
        """
        all_cleaned = []

        for day_offset in range(days):
            for hour in [0, 12]:
                window_start = dt_from + timedelta(days=day_offset, hours=hour)
                window_end = window_start + timedelta(hours=11, minutes=59)

                date_from = window_start.strftime("%Y-%m-%dT%H:%M")
                date_to = window_end.strftime("%Y-%m-%dT%H:%M")

                print(f"📡 Fetching {date_from} → {date_to}")

                try:
                    flights = fetch_flights(
                        code=code,
                        date_from=date_from,
                        date_to=date_to,
                        codetype=codetype,
                        direction=direction,
                        withLeg=withLeg,
                        withCancelled=withCancelled,
                        withCodeshared=withCodeshared,
                        withCargo=withCargo,
                        withPrivate=withPrivate,
                        withLocation=withLocation,
                    )
                    cleaned = clean_departures_extended(flights)
                    print(f"{len(cleaned)} cleaned flights")
                    all_cleaned.extend(cleaned)

                except Exception as e:
                    print(f"⚠️ Error during fetch: {e}")
                    continue

        # Final deduplication (just in case)
        deduplicated = {f["transaction_id"]: f for f in all_cleaned}
        return list(deduplicated.values())

if __name__ == "__main__":
    data = main()
    print(data[0].keys())