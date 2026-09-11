# YouTube ELT Pipeline & Automated Data Quality Platform

[![CI/CD Pipeline](https://github.com/Tranhoainam2kar4/Youtube_ELT/actions/workflows/CI_CD_yt_elt.yaml/badge.svg)](https://github.com/Tranhoainam2kar4/Youtube_ELT/actions/workflows/CI_CD_yt_elt.yaml)
![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Airflow 2.9.3](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL 13](https://img.shields.io/badge/PostgreSQL-13-4169E1?logo=postgresql&logoColor=white)
![Soda Core 3.3.14](https://img.shields.io/badge/Soda%20Core-3.3.14-FF6B6B)

A production-grade, containerized ELT data pipeline orchestrating daily YouTube channel metric ingestion, multi-layer PostgreSQL warehouse loading (staging to core), and automated Soda Core data quality assertions via Apache Airflow.

---

## Architecture & Data Flow

```text
+------------------------+
| YouTube Data API v3    |
+-----------+------------+
            |
            v  [produce_json @ 14:00 Europe/Malta]
+------------------------+
| Raw JSON Snapshot      | --> ./data/YT_data_{date}.json
+-----------+------------+
            |
            v  [update_db : staging_table]
+------------------------+
| Postgres (Staging)     | --> staging.yt_api (Differential Upsert + Hard Delete)
+-----------+------------+
            |
            v  [update_db : core_table]
+------------------------+
| Postgres (Core)        | --> core.yt_api (ISO 8601 Duration Parsing & Video_Type)
+-----------+------------+
            |
            v  [data_quality]
+------------------------+
| Soda Core Quality Gate | --> Automated Schema, Uniqueness & Metric Sanity Scans
+------------------------+
```

### Pipeline DAGs Breakdown

- **`produce_json` (Scheduled: `0 14 * * *`)**: Queries YouTube Data API v3 with pagination (`maxResults=50`) to resolve the channel's uploads playlist, extracts video metadata, metrics, and content details, writes an immutable snapshot to `./data/YT_data_{date}.json`, and triggers `update_db`.
- **`update_db` (Event-Driven: `TriggerDagRunOperator`)**: Synchronizes the daily snapshot into `staging.yt_api` using differential upsert and stale-record deletion; transforms staging records (parsing ISO 8601 duration into `TIME` format and classifying `Video_Type` as `Shorts` if duration $\le$ 60s else `Normal`) into `core.yt_api`, then triggers `data_quality`.
- **`data_quality` (Event-Driven: `TriggerDagRunOperator`)**: Executes blocking Soda Core scans sequentially across `staging` and `core` schemas using `soda-core-postgres` to enforce data contract integrity before downstream consumption.

---

## Tech Stack

| Domain               | Technology     | Version       | Purpose                                                                                        |
| :------------------- | :------------- | :------------ | :--------------------------------------------------------------------------------------------- |
| **Orchestration**    | Apache Airflow | `2.9.3`       | Multi-DAG workflow scheduling, CeleryExecutor task distribution, and pipeline chaining         |
| **Warehouse / DB**   | PostgreSQL     | `13`          | Multi-tenant instance hosting metadata, Celery backend, and ELT warehouse (`staging` & `core`) |
| **Data Quality**     | Soda Core      | `3.3.14`      | Declarative schema validation, entity uniqueness, and business metric integrity assertions     |
| **Testing**          | pytest         | `8.3.3`       | Automated unit testing, DAG structural integrity checks, and live integration tests            |
| **CI/CD**            | GitHub Actions | Ubuntu Latest | Automated image build, Docker Hub deployment, test execution, and end-to-end DAG runs          |
| **Containerization** | Docker Compose | Compose v2    | Multi-container isolation for Airflow services, Redis broker, and PostgreSQL                   |

---

## Data Quality & Idempotency Rules

### Soda Core Quality Gate (`include/soda/checks.yml`)

Automated data contract checks executed against both `staging.yt_api` and `core.yt_api`:

- **Null Value Checks**: `missing_count("Video_ID") = 0` enforces complete primary key population.
- **Entity Uniqueness**: `duplicate_count("Video_ID") = 0` eliminates duplicate video records.
- **View Count Integrity**: Custom SQL assertions verify engagement counts never exceed total views:
  - `likes_count_greater_than_vid_views = 0`: Validates `Likes_Count <= Video_Views`.
  - `comments_count_greater_than_vid_views = 0`: Validates `Comments_Count <= Video_Views`.

### Data Engineering Design Rules

- **Differential Snapshot Upsert**: Ingestion compares incoming JSON snapshots against existing warehouse IDs—inserting novel records and updating mutable metrics (`Video_Title`, `Video_Views`, `Likes_Count`, `Comments_Count`).

* **Stale ID Reconciliation:** Computes set difference (`Warehouse_IDs` - `Snapshot_IDs`) to automatically remove deleted or privated YouTube videos, preventing ghost records.

- **Idempotent Execution**: DAG runs are deterministic and safe to replay; table initialization uses `IF NOT EXISTS` DDL, and state updates guarantee consistency regardless of execution frequency.

### Automated Testing Strategy

- **Unit & DAG Integrity (`tests/unit_test.py`)**: Asserts environment variable fallback, connection parsing, zero DAG import errors (`dagbag.import_errors == {}`), expected DAG IDs, and exact task counts per DAG (`produce_json`: 5, `update_db`: 3, `data_quality`: 2).
- **Integration Tests (`tests/integration_test.py`)**: Tests live YouTube Data API v3 connectivity and PostgreSQL socket readiness (`SELECT 1;`).

---

## Quick Start

### Prerequisites

- Docker Engine 20.10+
- Docker Compose v2+

### Setup & Execution

1. Clone the repository:

   ```bash
   git clone https://github.com/Tranhoainam2kar4/Youtube_ELT.git
   cd Youtube_ELT
   ```

2. Configure environment variables:

   > Copy `.env.example` to `.env` and configure your credentials.

   ```bash
   cp .env.example .env
   ```

3. Build and launch the cluster:

   ```bash
   docker compose up -d --build
   ```

4. Execute test suite inside the container:
   ```bash
   docker exec -it airflow-worker pytest tests/ -v
   ```

---

## Project Structure

```text
Youtube_ELT/
├── .github/workflows/
│   └── CI_CD_yt_elt.yaml          # CI/CD: build/push image, compose setup, pytest, E2E DAG test
├── dags/
│   ├── api/
│   │   └── video_stats.py          # YouTube Data API v3 client, pagination & JSON snapshot writer
│   ├── dataquality/
│   │   └── soda.py                 # Soda Core BashOperator task definition
│   ├── datawarehouse/
│   │   ├── data_loading.py         # Snapshot loader for raw JSON payloads
│   │   ├── data_modification.py    # SQL upsert and stale-record deletion logic
│   │   ├── data_transformation.py  # ISO 8601 duration parsing & Shorts classification
│   │   ├── data_utils.py           # PostgresHook wrapper, schema/table DDL creation
│   │   └── dwh.py                  # Staging and core warehouse loading tasks
│   └── main.py                     # Primary DAG definitions (produce_json, update_db, data_quality)
├── docker/
│   └── postgres/
│       └── init-multiple-databases.sh # Entrypoint script provisioning 3 isolated databases
├── include/
│   └── soda/
│       ├── checks.yml              # Quality checks (nullability, uniqueness, metric consistency)
│       └── configuration.yml       # Soda data source connection configuration
├── tests/
│   ├── conftest.py                 # Pytest fixtures (mock Airflow vars, connections, DB session)
│   ├── integration_test.py         # Live YouTube API and PostgreSQL connectivity tests
│   └── unit_test.py                # DAG integrity, task count, and configuration unit tests
├── .env.example                    # Template environment variables (committed)
├── docker-compose.yaml             # Multi-service stack (Webserver, Scheduler, Worker, Postgres, Redis)
├── dockerfile                      # Airflow 2.9.3 + Python 3.11 + Soda Core base image
└── requirements.txt                # Python dependencies (soda-core-postgres, pytest)
```
