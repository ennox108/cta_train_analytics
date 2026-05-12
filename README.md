# CTA Train Analytics Pipeline

Real-time CTA 'L' train data pipeline that loads live train positions and arrival predictions into Snowflake and transforms them into analytics-ready tables using dbt.

---

## Architecture

```
CTA Train Tracker API
        │
        ▼
   dlt (pipeline.py)
   GitHub Actions — runs every 15 min
        │
        ▼
 Snowflake: CTA_DB.CTA_RAW
   ├── arrivals
   └── train_locations
        │
        ▼
   dbt Cloud (Production job)
        │
        ▼
 Snowflake: CTA_DB.CTA_MARTS
   ├── stg_arrivals
   ├── stg_train_locations
   ├── fct_arrivals
   ├── fct_delay_events
   ├── agg_delay_by_route
   └── agg_delay_by_station
```

---

## Stack

| Layer | Tool |
|---|---|
| Extract & Load | [dlt](https://dlthub.com) |
| Data Warehouse | [Snowflake](https://snowflake.com) |
| Transformation | [dbt Cloud](https://cloud.getdbt.com) |
| Orchestration (EL) | GitHub Actions |
| Orchestration (T) | dbt Cloud Jobs |

---

## Data Sources

- **CTA Train Tracker API** — real-time train positions and arrival predictions
  - `ttpositions.aspx` — live train locations for all 8 'L' routes
  - `ttarrivals.aspx` — arrival predictions for major stations

---

## Project Structure

```
cta_train/
  pipeline.py                  # dlt pipeline: CTA API → Snowflake
  requirements.txt             # Python dependencies
  .dlt/
    config.toml                # Non-secret dlt configuration
    secrets.toml.example       # Template for credentials
  .github/
    workflows/
      cta_pipeline.yml         # GitHub Actions workflow (runs every 15 min)
  dbt_cta/
    dbt_project.yml            # dbt project configuration
    models/
      staging/
        _sources.yml           # Declares cta_raw source tables
        stg_arrivals.sql       # Cleaned arrivals predictions
        stg_train_locations.sql # Cleaned train positions
      marts/
        fct_arrivals.sql       # Core fact table
        fct_delay_events.sql   # Filtered delay events
        agg_delay_by_route.sql # Delay rate % by route per hour
        agg_delay_by_station.sql # Delay rate % by station
```

---

## dbt Lineage


<img width="2492" height="1313" alt="Screenshot 2026-05-12 at 3 17 19 PM" src="https://github.com/user-attachments/assets/5b274511-4bc1-4e25-b2d0-604aece1d682" />




## Setup

### 1. Clone the repo

```bash
git clone https://github.com/ennox108/cta_train_analytics.git
cd cta_train_analytics
pip install -r requirements.txt
```

### 2. Configure credentials

Copy `.dlt/secrets.toml.example` to `.dlt/secrets.toml` and fill in:
- CTA Train Tracker API key — [apply here](https://www.transitchicago.com/developers/traintrackerapply/)
- Snowflake credentials

### 3. Run dlt locally (optional)

```bash
python pipeline.py
```

### 4. GitHub Actions (automated)

Add the following secrets to your GitHub repo under **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `CTA_API_KEY` | CTA Train Tracker API key |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_ROLE` | Snowflake role |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name |
| `SNOWFLAKE_DATABASE` | `CTA_DB` |

### 5. dbt Cloud

- Connect dbt Cloud to this repo with project subdirectory set to `dbt_cta`
- Configure Snowflake connection
- Create a Production environment and deploy job

---

## Models

| Model | Layer | Description |
|---|---|---|
| `stg_arrivals` | Staging | Cleaned arrival predictions |
| `stg_train_locations` | Staging | Cleaned live train positions |
| `fct_arrivals` | Mart | Core fact table with minutes until arrival |
| `fct_delay_events` | Mart | All delay flag events |
| `agg_delay_by_route` | Mart | Hourly delay rate % per route |
| `agg_delay_by_station` | Mart | Delay rate % ranked by station |
