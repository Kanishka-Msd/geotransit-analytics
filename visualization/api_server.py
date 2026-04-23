"""
API SERVER — api_server.py
---------------------------
A simple HTTP server that queries PostgreSQL and
serves bus data as JSON to our MapboxGL map.

WHY A SEPARATE API SERVER?
    MapboxGL runs in the browser (JavaScript).
    Browsers can't connect directly to PostgreSQL.
    So we need a Python server in the middle that:
        1. Receives HTTP requests from the browser
        2. Queries PostgreSQL
        3. Returns JSON data

CLOUD EQUIVALENT:
    GCP  → Cloud Run or App Engine
    AWS  → API Gateway + Lambda
    Local → This simple Python HTTP server
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
    )

class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        # CORS headers — allows browser to call this server
        if self.path == '/buses':
            self.serve_buses()
        elif self.path == '/routes':
            self.serve_routes()
        else:
            self.send_response(404)
            self.end_headers()

    def send_json(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

    def serve_buses(self):
        """
        Returns the most recent GPS ping for every active bus.
        This is what gets plotted as colored dots on the map.
        """
        conn = get_conn()
        cur  = conn.cursor()

        # Get latest position for each trip
        # DISTINCT ON = PostgreSQL-specific — gets one row per trip_id
        # ordered by most recent timestamp
        cur.execute("""
            SELECT DISTINCT ON (b.trip_id)
                b.trip_id,
                t.vehicle_id,
                t.route_id,
                b.latitude,
                b.longitude,
                b.speed,
                b.heading,
                b.tstamp
            FROM breadcrumb b
            JOIN trip t ON b.trip_id = t.trip_id
            ORDER BY b.trip_id, b.tstamp DESC;
        """)

        rows = cur.fetchall()

        # Stats for header
        cur.execute("SELECT COUNT(*) FROM trip;")
        total_buses = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM breadcrumb;")
        total_pings = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT route_id) FROM trip;")
        total_routes = cur.fetchone()[0]

        cur.execute("SELECT ROUND(AVG(speed)::numeric, 2) FROM breadcrumb;")
        avg_speed = float(cur.fetchone()[0] or 0)

        conn.close()

        buses = [{
            'trip_id':    r[0],
            'vehicle_id': r[1],
            'route_id':   r[2],
            'latitude':   r[3],
            'longitude':  r[4],
            'speed':      float(r[5] or 0),
            'heading':    float(r[6] or 0),
            'tstamp':     r[7].isoformat()
        } for r in rows]

        self.send_json({
            'buses': buses,
            'stats': {
                'total_buses':  total_buses,
                'total_pings':  total_pings,
                'total_routes': total_routes,
                'avg_speed':    avg_speed
            }
        })

    def serve_routes(self):
        """
        Returns average speed per route for sidebar analytics.
        Same query as your TriMet analytics!
        """
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT
                t.route_id,
                ROUND(AVG(b.speed)::numeric, 2) AS avg_speed,
                COUNT(*) AS total_pings
            FROM breadcrumb b
            JOIN trip t ON b.trip_id = t.trip_id
            GROUP BY t.route_id
            ORDER BY avg_speed DESC
            LIMIT 20;
        """)

        rows = cur.fetchall()
        conn.close()

        self.send_json({
            'routes': [{
                'route_id':   r[0],
                'avg_speed':  float(r[1] or 0),
                'total_pings': r[2]
            } for r in rows]
        })

    def log_message(self, format, *args):
        # Custom log format
        print(f"[API] {self.address_string()} → {args[0]}")

if __name__ == '__main__':
    server = HTTPServer(('localhost', 8000), Handler)
    print("=" * 45)
    print("GeoTransit API Server running!")
    print("URL: http://localhost:8000")
    print("Endpoints:")
    print("  /buses  → latest bus positions")
    print("  /routes → route speed analytics")
    print("Press Ctrl+C to stop")
    print("=" * 45)
    server.serve_forever()