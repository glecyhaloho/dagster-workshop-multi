# pipeline_reporting — cross-pipeline EUR value + high-value order report

Combines data already produced by three independent pipelines
(`pipeline_products`, `pipeline_fx`, `pipeline_ml`) into one warehouse table,
without any pipeline depending on another directly — just a shared
destination database. I picked this track because it best demonstrates the
core idea of the workshop: independent containers landing in one place, and
downstream consumers reading that place instead of calling each other.

Built on top of [dagster-workshop-multi](https://github.com/DanielAdif/dagster-workshop-multi),
a multi-container Dagster workshop — see below for the base architecture
(`pipeline_products`, `pipeline_fx`, `pipeline_ml`).

## What I built

- **Track:** B — cross-pipeline analytics
- **Data source:** the shared `warehouse_postgresql` tables written by the
  three existing pipelines — `orders`/`products` (`pipeline_products`),
  `exchange_rates` (`pipeline_fx`), `order_value_predictions` (`pipeline_ml`)
- **Key assets:**
  - `high_value_orders_eur_report` — joins `orders` + `products` per
    `order_id`, converts totals to EUR using the latest USD→EUR rate, and
    merges in each order's high-value prediction and average model
    probability from `pipeline_ml`
  - `high_value_orders_eur_report_table` — writes the result to the
    warehouse as `high_value_orders_eur_report`
- **Quality gate:** `report_has_no_duplicate_orders` — an `@asset_check`
  that fails if the report contains more than one row for the same
  `order_id`, since the report is meant to be one row per order

## Architecture

```
                     dagster_webserver (:3000)  <-- workspace.yaml -->  dagster_daemon
                              |                                              |
                              +---------------------+-----------------------+
                                                     |
                             dagster_postgresql  (Dagster's own run/schedule/event storage)

  pipeline_products (:4000)     pipeline_fx (:4001)      pipeline_ml (:4002)      pipeline_reporting (:4003)
  fakestoreapi.com ->           api.frankfurter.app ->    trains a classifier      reads orders+products+
  raw_products/raw_orders       raw_exchange_rates        on products+orders,      exchange_rates+predictions,
        |                             |                   writes predictions       writes high_value_orders_
        v                             v                   back                     eur_report
  products, orders  ---------> warehouse_postgresql <-------------------------------------+
  tables                       (also: exchange_rates,                                     |
                                order_value_predictions,                                   |
                                high_value_orders_eur_report) <-----------------------------+
```

## Running it

```bash
docker compose up --build
```

Open http://localhost:3000, find `pipeline_reporting` under Deployment >
Code Locations, and materialize its assets — run `pipeline_products`,
`pipeline_fx`, and `pipeline_ml` at least once first, since
`pipeline_reporting` reads their output tables.

Verified locally: all four pipelines materialize successfully end to end via
`dagster job launch`, `report_has_no_duplicate_orders` passes
(`num_duplicate_orders: 0`), and `high_value_orders_eur_report` lands in the
warehouse with one row per order (7 rows from 14 order lines in the sample
data).

## Demo

*(Add a screenshot of the Dagster UI here — open http://localhost:3000,
select all assets, click "Materialize all", and screenshot the asset graph
once `pipeline_reporting` shows green.)*

## What I'd do differently in production

This report truncates and reloads on every run (`if_exists="replace"`), so
there's no history of what the report looked like yesterday — in production
I'd append with a run/partition date so past reports stay queryable. The
quality gate only checks for duplicate orders; a real version should also
check for orders present in `orders` but missing from the report (meaning
the join silently dropped something) and alert on that, not just on
duplicates.

---

# dagster-workshop-multi

A multi-container introduction to [Dagster](https://dagster.io) using the
real production pattern: one Docker container per pipeline, each running its
own Dagster gRPC code server, registered with a central webserver/daemon via
`workspace.yaml`.

## Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)
- Internet access (`pipeline_products` and `pipeline_fx` call free public APIs)

## Quickstart

```bash
docker compose up --build
```

Then open http://localhost:3000. Under Deployment > Code Locations you should
see `pipeline_products`, `pipeline_fx`, and `pipeline_ml`, each its own
container. Select all assets and click "Materialize all" to run all three
pipelines end to end — `pipeline_ml` trains on the data the other two just
loaded, so it needs to run after them at least once.

## Verifying a run

- **In the UI:** every asset in the graph should turn green. A red asset
  means its run failed — click it and open the run logs for the error.
  `model_quality_check` (under `pipeline_ml`) should show a passing check;
  a failing check means the trained model's accuracy dropped below the 0.6
  threshold — click it in the Asset Checks panel to see the reported
  accuracy.
- **In the warehouse:** connect to the shared Postgres directly and confirm
  data actually landed:
  ```bash
  docker compose exec warehouse_postgresql psql -U warehouse_user -d warehouse -c "\dt"
  docker compose exec warehouse_postgresql psql -U warehouse_user -d warehouse -c "SELECT COUNT(*) FROM order_value_predictions;"
  ```
  You should see `products`, `orders`, `exchange_rates`, and
  `order_value_predictions` tables, each with rows.

## What just happened

```
                     dagster_webserver (:3000)  <-- workspace.yaml -->  dagster_daemon
                              |                                              |
                              +---------------------+-----------------------+
                                                     |
                             dagster_postgresql  (Dagster's own run/schedule/event storage)

  pipeline_products (:4000)          pipeline_fx (:4001)          pipeline_ml (:4002)
  fakestoreapi.com ->                api.frankfurter.app ->       trains a classifier on
  raw_products/raw_orders            raw_exchange_rates           products+orders, writes
        |                                  |                      predictions back
        v                                  v                            |
  products, orders  ------------->  warehouse_postgresql  <-------------+
  tables                            (also: exchange_rates,
                                      order_value_predictions)
```

Each pipeline is a fully independent container: its own `Dockerfile`, its own
`requirements.txt`, its own source/db modules. They only share the
`warehouse_postgresql` database as a landing zone — exactly like production's
21 pipeline containers, each pulling from its own source system into one
destination database. `pipeline_ml` is the odd one out: instead of pulling
from an external API, it reads `pipeline_products`' tables straight out of
the warehouse, trains a classifier, and writes predictions back — see
[docs/mlops.md](docs/mlops.md) for why Dagster's asset/asset-check model
fits that pattern too.

All three pipelines write with a simple truncate-and-load (`if_exists="replace"`)
— a simplified stand-in for production's shift-based "check-then-insert"
pattern.

## Running the tests locally

Each pipeline has its own test suite, independent of Docker — tests mock
the external API calls and the warehouse connection, so no running database
or containers are needed:

```bash
cd pipeline_products && pip install -r requirements.txt && python -m pytest -v
cd pipeline_fx && pip install -r requirements.txt && python -m pytest -v
cd pipeline_ml && pip install -r requirements.txt && python -m pytest -v
```

## Exercises

See [docs/exercises.md](docs/exercises.md) for three hands-on TODOs, in
increasing difficulty. Each one has a `# TODO(exercise-N)` comment marking
where to add your code.

## Capstone

Once you've finished the three exercises, see
[docs/capstone.md](docs/capstone.md) for a bigger, open-ended assignment:
build and wire in your own pipeline, in your own fork, and turn it into a
portfolio piece.

## How this maps to the production pipeline

This is adapted from a real Dagster + Docker production system with 21
pipeline containers pulling manufacturing data (OEE, downtime, QC) from
internal MSSQL/AS400 systems into a central SQL Server database. This
workshop keeps the core architecture — one container per pipeline, gRPC code
servers, `workspace.yaml` registration, a shared destination database — but
swaps the internal systems for free public APIs, and drops production's
`DockerRunLauncher` (which spawns a fresh container per run via a mounted
`docker.sock`) in favor of Dagster's default run launcher, where runs execute
in-process within each pipeline's own gRPC container. See
`dagster-workshop-basic` for a single-container introduction to the core
Dagster concepts before diving into this multi-container version.
