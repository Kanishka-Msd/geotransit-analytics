"""
PUBLISHER — fetch_data.py
-------------------------
DATA SOURCE: 511 SF Bay Transit API
AGENCY: AC Transit (Fremont/Bay Area buses)

This fetches real-time bus positions from AC Transit —
the buses that actually run through Fremont where you live!

511 API returns GTFS-RT format (General Transit Feed Specification
Realtime) — this is the INDUSTRY STANDARD format used by every
major transit agency in the world. TriMet, CTA, MTA all use it.
So learning this = transferable skill everywhere.
"""

import requests
import redis
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("TRANSIT_511_KEY")

# Connect to Redis (our local Pub/Sub)
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# 511 SF Bay API endpoint
# agency=AC means AC Transit (serves Fremont, Oakland, Berkeley)
BASE_URL = "https://api.511.org/transit/vehiclepositions"

def fetch_buses():
    """
    Calls 511 API and returns list of active AC Transit buses.

    ACTUAL DATA STRUCTURE FROM 511 API:
    {
        "Entities": [
            {
                "Id": "1",
                "Vehicle": {
                    "Trip": {
                        "TripId": "8553020",
                        "RouteId": "NL",
                        "DirectionId": 1
                    },
                    "Vehicle": {
                        "Id": "6111"
                    },
                    "Position": {
                        "Latitude": 37.81741,
                        "Longitude": -122.290657,
                        "Bearing": 123,
                        "Speed": 12.5171194
                    },
                    "Timestamp": 1776980513
                }
            }
        ]
    }
    """
    params = {
        'api_key': API_KEY,
        'agency':  'AC',
        'format':  'json'
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=15)

        print(f"[DEBUG] Status code: {response.status_code}")

        if response.status_code != 200:
            print(f"[ERROR] API returned: {response.status_code}")
            print(f"[ERROR] Response: {response.text[:300]}")
            return []

        # 511 API returns UTF-8 with BOM — decode manually to handle it
        data     = json.loads(response.content.decode('utf-8-sig'))
        entities = data.get('Entities', [])

        print(f"[DEBUG] Raw entity count: {len(entities)}")

        vehicles = []
        skipped  = 0

        for entity in entities:
            try:
                # Get the Vehicle block
                v = entity.get('Vehicle')
                if not v:
                    skipped += 1
                    continue

                # Each sub-section — all default to {} if null
                position = v.get('Position') or {}
                trip     = v.get('Trip')     or {}
                vehicle  = v.get('Vehicle')  or {}

                # Skip if no position data at all
                if not position:
                    skipped += 1
                    continue

                # Skip if lat/lon missing
                if position.get('Latitude') is None or position.get('Longitude') is None:
                    skipped += 1
                    continue

                # vehicle_id from Vehicle.Id
                vehicle_id = str(vehicle.get('Id', ''))

                # trip_id from Trip.TripId, fall back to vehicle_id
                # Some buses have null Trip (seen in your API sample)
                trip_id = str(trip.get('TripId', '') or vehicle_id)

                record = {
                    'trip_id':    trip_id,
                    'vehicle_id': vehicle_id,
                    'route_id':   str(trip.get('RouteId',    '') or ''),
                    'direction':  str(trip.get('DirectionId','') or ''),
                    'latitude':   float(position.get('Latitude',  0)),
                    'longitude':  float(position.get('Longitude', 0)),
                    'speed':      float(position.get('Speed',     0) or 0),
                    'heading':    float(position.get('Bearing',   0) or 0),
                    'timestamp':  datetime.now().isoformat(),
                    'raw_tstamp': str(v.get('Timestamp', ''))
                }

                vehicles.append(record)

            except Exception as e:
                skipped += 1
                print(f"[SKIP] Bad entity {entity.get('Id', '?')}: {e}")
                continue

        print(f"[DEBUG] Parsed: {len(vehicles)} valid | Skipped: {skipped}")
        return vehicles

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] API call failed: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"[ERROR] Could not parse JSON: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return []


def publish_to_queue(vehicles):
    """
    Pushes each vehicle record into Redis queue.
    Skips records with missing or zero GPS coordinates.

    CLOUD EQUIVALENT:
        GCP  → publisher.publish(topic_path, data)
        AWS  → sqs.send_message(QueueUrl=..., MessageBody=...)
        We use Redis LIST:
            RPUSH = add to right end (publisher puts message in)
            BLPOP = subscriber takes from left end (first in first out)
    """
    count   = 0
    skipped = 0

    for vehicle in vehicles:
        if not vehicle.get('latitude') or not vehicle.get('longitude'):
            skipped += 1
            continue

        if vehicle['latitude'] == 0 or vehicle['longitude'] == 0:
            skipped += 1
            continue

        r.rpush('breadcrumbs', json.dumps(vehicle))
        count += 1

    if skipped:
        print(f"[SKIP] {skipped} vehicles had no GPS fix")

    return count


def main():
    print("=" * 55)
    print("GeoTransit Publisher — 511 SF Bay / AC Transit")
    print("Fetching Fremont/Bay Area bus data every 5 seconds")
    print("Press Ctrl+C to stop")
    print("=" * 55)

    total_published = 0

    while True:
        cycle_start = time.time()

        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fetching buses...")
        vehicles = fetch_buses()

        if vehicles:
            count = publish_to_queue(vehicles)
            total_published += count
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"Fetched: {len(vehicles)} | "
                  f"Published: {count} | "
                  f"Total: {total_published}")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"No vehicles returned — check API key or agency code")

        # 5 second interval — same as TriMet breadcrumb frequency
        elapsed    = time.time() - cycle_start
        sleep_time = max(0, 5 - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()