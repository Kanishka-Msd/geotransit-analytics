# GeoTransit Analytics

Real-time bus tracking and analytics pipeline for SF Bay Area (AC Transit).

![Python](https://img.shields.io/badge/Python-3.12-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue)
![Redis](https://img.shields.io/badge/Redis-Queue-red)
![MapboxGL](https://img.shields.io/badge/MapboxGL-Visualization-green)

## Live Map Preview

![GeoTransit Map](screenshot.png)

## What It Does

- 500+ buses tracked simultaneously across 75 routes
- 109000+ GPS pings stored per session in PostgreSQL
- Real-time speed analytics per route
- Color-coded map: red=slow, yellow=moderate, green=fast

## Architecture

| Step | Component | Details |
|------|-----------|---------|
| 1 | 511 SF Bay API | GTFS-RT real-time feed |
| 2 | Python Publisher | Fetches buses every 5 seconds |
| 3 | Redis Queue | GCP Pub/Sub or AWS SQS equivalent |
| 4 | Python Subscriber | Validate, transform, load |
| 5 | PostgreSQL | Stores breadcrumb and trip tables |
| 6 | Python REST API | Serves data to browser |
| 7 | MapboxGL Map | Interactive speed visualization |

## Cloud Mapping

| Local | GCP | AWS | Azure |
|-------|-----|-----|-------|
| Python loop | Compute Engine | EC2 | AKS |
| Redis | Cloud Pub/Sub | SQS | Service Bus |
| Python consumer | Cloud Functions | Lambda | Azure Functions |
| PostgreSQL | Cloud SQL | RDS | Azure SQL |
| Python API | Cloud Run | API Gateway | App Service |

## Setup

```bash
git clone https://github.com/Kanishka-Msd/geotransit-analytics
cd geotransit-analytics
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create .env file:
TRANSIT_511_KEY=your_key_here
DB_NAME=geotransit
DB_USER=your_username
Run the pipeline:

```bash
python3 publisher/fetch_data.py
python3 subscriber/process_data.py
python3 visualization/api_server.py
open visualization/map.html
```

## What I Learned

- Decoupled pub/sub pipeline architecture
- GTFS-RT industry standard transit data format
- PostgreSQL upsert patterns for streaming data
- Real-time geospatial visualization with MapboxGL
- Data validation to prevent pipeline failures
- Mapping local architecture to GCP, AWS, and Azure
