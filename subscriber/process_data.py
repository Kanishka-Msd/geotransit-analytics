"""
SUBSCRIBER — process_data.py
-----------------------------
WHAT THIS DOES:
    Continuously listens to the Redis queue for new bus records.
    For each record it:
        1. Validates the data (rejects bad records)
        2. Transforms/enhances the data (cleans timestamps etc.)
        3. Inserts into PostgreSQL (trip table + breadcrumb table)

CLOUD EQUIVALENT:
    In GCP, this would be a Pub/Sub subscriber running on a VM
    with systemd keeping it alive 24/7.
    In AWS, this would be an SQS consumer on EC2 or Lambda.

WHY SEPARATE FROM PUBLISHER?
    - Publisher only cares about fetching and queuing fast
    - Subscriber only cares about processing and storing
    - If DB is slow, publisher keeps running — no data lost
    - This is called the 'separation of concerns' principle
"""

import redis
import json
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─── Connect to Redis ────────────────────────────────────────────────
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# ─── Connect to PostgreSQL ───────────────────────────────────────────
def get_db_connection():
    """
    Connects via Unix socket — bypasses password auth on Mac.
    This is how local development connections work on macOS
    with Postgres.app installed.
    """
    try:
        # Try Unix socket first (no password needed on Mac)
        return psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
        )
    except Exception:
        # Fall back to TCP with password
        return psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD', ''),
            host=os.getenv('DB_HOST', '127.0.0.1'),
            port=os.getenv('DB_PORT', '5432')
        )

# ─── Validation ──────────────────────────────────────────────────────
def validate(record):
    """
    Checks if a record is good enough to store.
    Returns (True, None) if valid, (False, reason) if not.

    WHY VALIDATION MATTERS:
        Bad data corrupts analytics downstream.
        In production, bad records go to a 'dead letter queue'
        for later inspection — not silently dropped.
    """
    # Must have a trip ID
    if not record.get('trip_id'):
        return False, "missing trip_id"

    # GPS must be in valid global range
    lat = record.get('latitude', 0)
    lon = record.get('longitude', 0)

    if not (-90 <= lat <= 90):
        return False, f"invalid latitude: {lat}"

    if not (-180 <= lon <= 180):
        return False, f"invalid longitude: {lon}"

    # Must be within SF Bay Area bounding box
    # Catches GPS glitches that put buses in the ocean
    if not (37.2 <= lat <= 38.2):
        return False, f"latitude outside Bay Area: {lat}"

    if not (-122.6 <= lon <= -121.5):
        return False, f"longitude outside Bay Area: {lon}"

    # Speed must be realistic for a city bus (0–100 mph)
    speed = record.get('speed', 0)
    if speed < 0 or speed > 100:
        return False, f"unrealistic speed: {speed}"

    return True, None

# ─── Transform ───────────────────────────────────────────────────────
def transform(record):
    """
    Cleans and enhances the record before storing.
    This is the 'T' in ETL (Extract, Transform, Load).
    """
    # Parse timestamp — use raw_tstamp if available (Unix epoch from 511)
    # otherwise fall back to our fetch timestamp
    raw_ts = record.get('raw_tstamp', '')
    try:
        # 511 API gives Unix timestamp (seconds since 1970)
        tstamp = datetime.fromtimestamp(int(raw_ts))
    except (ValueError, TypeError):
        try:
            tstamp = datetime.fromisoformat(record.get('timestamp'))
        except Exception:
            tstamp = datetime.now()

    # Speed: 511 API gives m/s already — no conversion needed
    # But we round to 4 decimal places for clean storage
    speed = round(float(record.get('speed', 0)), 4)

    return {
        'trip_id':    str(record['trip_id']),
        'vehicle_id': str(record.get('vehicle_id', '')),
        'route_id':   str(record.get('route_id', '')),
        'direction':  str(record.get('direction', '')),
        'latitude':   float(record['latitude']),
        'longitude':  float(record['longitude']),
        'heading':    float(record.get('heading', 0)),
        'speed':      speed,
        'tstamp':     tstamp
    }

# ─── Database Insert ─────────────────────────────────────────────────
def upsert_trip(conn, record):
    """
    Inserts a trip record if it doesn't already exist.
    ON CONFLICT DO NOTHING = safe to call repeatedly for same trip.

    WHY UPSERT?
        Same bus runs all day. We only want ONE row per vehicle
        in the trip table, not one per GPS ping.
    """
    sql = """
        INSERT INTO trip (trip_id, route_id, vehicle_id, direction)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (trip_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            record['trip_id'],
            record['route_id'],
            record['vehicle_id'],
            record['direction']
        ))

def insert_breadcrumb(conn, record):
    """
    Inserts one GPS ping into the breadcrumb table.
    Every 5-second location update = one breadcrumb row.
    This is identical in concept to TriMet's breadcrumb data.
    """
    sql = """
        INSERT INTO breadcrumb
            (trip_id, latitude, longitude, speed, heading, tstamp)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            record['trip_id'],
            record['latitude'],
            record['longitude'],
            record['speed'],
            record['heading'],
            record['tstamp']
        ))

# ─── Main Loop ───────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("GeoTransit Subscriber — 511 SF Bay / AC Transit")
    print("Listening to Redis queue 'breadcrumbs'...")
    print("Press Ctrl+C to stop")
    print("=" * 55)

    # Counters for monitoring
    processed = 0
    rejected  = 0
    errors    = 0

    # Connect to PostgreSQL once — reuse connection for performance
    try:
        conn = get_db_connection()
        print("[DB] Connected to PostgreSQL successfully\n")
    except Exception as e:
        print(f"[DB ERROR] Could not connect: {e}")
        return

    while True:
        try:
            # BLPOP waits up to 5 seconds for a message
            # Efficient — no busy waiting, no wasted CPU
            # CLOUD EQUIVALENT:
            #   GCP Pub/Sub  → streaming pull subscription
            #   AWS SQS      → receive_message with WaitTimeSeconds=5
            result = r.blpop('breadcrumbs', timeout=5)

            if result is None:
                queue_size = r.llen('breadcrumbs')
                print(f"[WAIT] Queue empty (size={queue_size}), "
                      f"waiting for publisher...")
                continue

            # result = ('breadcrumbs', 'json string')
            _, raw   = result
            record   = json.loads(raw)

            # Step 1: Validate
            valid, reason = validate(record)
            if not valid:
                rejected += 1
                print(f"[REJECT] trip={record.get('trip_id','?')} "
                      f"reason={reason}")
                continue

            # Step 2: Transform
            clean = transform(record)

            # Step 3: Load into PostgreSQL
            upsert_trip(conn, clean)
            insert_breadcrumb(conn, clean)
            conn.commit()

            processed += 1

            # Print progress every 100 records
            if processed % 100 == 0:
                queue_size = r.llen('breadcrumbs')
                print(f"[OK] Processed: {processed} | "
                      f"Rejected: {rejected} | "
                      f"Errors: {errors} | "
                      f"Queue remaining: {queue_size}")

        except psycopg2.Error as e:
            errors += 1
            print(f"[DB ERROR] {e}")
            conn.rollback()
            # Reconnect if connection dropped
            try:
                conn = get_db_connection()
                print("[DB] Reconnected successfully")
            except Exception:
                pass

        except KeyboardInterrupt:
            print(f"\n[STOP] Shutting down subscriber gracefully")
            print(f"Final — Processed: {processed} | "
                  f"Rejected: {rejected} | "
                  f"Errors: {errors}")
            break

        except Exception as e:
            errors += 1
            print(f"[ERROR] Unexpected: {e}")

    conn.close()
    print("[DB] Connection closed")

if __name__ == "__main__":
    main()