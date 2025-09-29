Workshop ETL – PostgreSQL & Google Sheets
📌 Overview

This project implements a full ETL pipeline for a mechanical workshop.
Data originates in Google Sheets, is ingested into PostgreSQL (via Docker), cleaned and transformed into structured layers, and finally exported back to Google Sheets for reporting and visualization in Looker Studio.

🏗️ Architecture

Source → Google Sheets with raw service records (sales, purchases, labor).

ETL (Python) → Scripts handle:

Extracting from Google Sheets.

Transforming and cleaning records.

Loading into PostgreSQL.

PostgreSQL (Dockerized) → organized in three layers:

raw → raw ingested data.

core → cleaned and normalized data.

mart → business-ready views (servicios_summary).

Export → Results from mart.servicios_summary are written to a destination Google Sheet.

Visualization → Looker Studio dashboards built on top of the summary sheet.

📂 Project structure
taller/
├─ .github/workflows/       # CI/CD pipelines
├─ sql/                     # SQL scripts (schemas, transformations, views)
│   ├─ 00_init_schemas.sql
│   ├─ 01_raw_servicios.sql
│   ├─ 02_core_servicios.sql
│   ├─ 05_mart_servicios_summary.sql
├─ src/                     # Python ETL scripts
│   ├─ load_sheet.py            # ingest Google Sheets → raw.servicios
│   ├─ export_core_to_gsheet.py # export mart.servicios_summary → Google Sheets
├─ secrets/                 # credentials (ignored by git)
├─ .env.example             # environment variable template
├─ requirements.txt         # Python dependencies
├─ docker-compose.yml       # Postgres service definition
└─ README.md                # this file

⚙️ Stack

Python 3.13

gspread (Google Sheets API)

psycopg[binary] (PostgreSQL driver)

python-dotenv (env var management)

PostgreSQL 16 (Dockerized)

Google Sheets API (Service Account)

Docker & docker-compose

Looker Studio for BI dashboards

GitHub Actions for CI/CD

🚀 Workflow

Clone repository

git clone git@github.com:ArmandoCarrillo91/taller.git
cd taller


Setup environment

Create .env from .env.example.

Place service account JSON in secrets/gsheets_sa.json.

Start Postgres with Docker

docker compose up -d db


Initialize schemas and tables

docker exec -i taller_db psql -U admin -d taller < sql/00_init_schemas.sql
docker exec -i taller_db psql -U admin -d taller < sql/01_raw_servicios.sql
docker exec -i taller_db psql -U admin -d taller < sql/02_core_servicios.sql
docker exec -i taller_db psql -U admin -d taller < sql/05_mart_servicios_summary.sql


Load data from Google Sheets → RAW

python src/load_sheet.py


Transform into CORE
Data is cleaned, normalized, and enriched with business rules.

Generate MART (summary view)
mart.servicios_summary aggregates per service order (folio).

Export MART to Google Sheets

python src/export_core_to_gsheet.py


Build dashboards
Connect the destination Google Sheet to Looker Studio.

📊 Key columns in mart.servicios_summary

fecha_inicio / fecha_fin → service order start and end dates.

venta_refacciones / costo_refacciones / utilidad_refacciones → parts revenue, cost, and margin.

venta_mano_obra / pago_mecanico / utilidad_mano_obra → labor revenue, mechanic payout (30%), and workshop margin.

total_venta → total billed to the customer (parts + labor).

total_costos → workshop costs (parts + mechanic payouts).

utilidad_total / %utilidad → final margin in absolute and percentage terms.

piezas_ref_sin_compra / piezas_ref_sin_venta → data quality checks (parts sold without purchase, or purchased without sale).

✅ Current status

Repository and Dockerized Postgres environment set up.

ETL pipeline established (Google Sheets → RAW → CORE → MART).

Export process working (MART → Google Sheets).

Dashboard integration ready (Looker Studio).