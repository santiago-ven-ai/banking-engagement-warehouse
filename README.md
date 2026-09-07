# banking-engagement-warehouse

[![CI](https://github.com/santiago-ven-ai/banking-engagement-warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/santiago-ven-ai/banking-engagement-warehouse/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-18%25-orange)](https://github.com/santiago-ven-ai/banking-engagement-warehouse/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A dimensional engagement warehouse for bank customer analytics — SCD Type 2 history, data quality gates that block bad loads, and per-pipeline cost/SLA attribution.

## Pitch Card

**Problem** — Bank engagement analytics break silently: a customer changes segment, the dimension overwrites history, and last quarter's cohort report becomes unreproducible — with no one alerted and no cost visibility into which pipeline caused it.

**Solution** — A dimensional warehouse (bronze → silver → gold on plain Parquet/JSON) with SCD Type 2 customer history, 6 data-quality gates that **block** promotion on failure instead of just reporting it, and Lambda-based per-pipeline cost attribution.

**Impact** — 100% reproducible historical cohorts across 24 synthetic months, 6 quality gates catching 6 seeded defect classes, SLA breach detected in <5 min, per-pipeline daily cost attribution.

**Stack** — SQL · Python 3 · PySpark / Parquet · Flask · AWS (S3, Redshift, Lambda, Step Functions, SNS, SQS, DynamoDB) via LocalStack · Terraform (Azure export stub)

---

## Architecture

```
  synthetic engagement events (logins, card txns, offers, redemptions)
             |
             v
  src/ingestion/data_gen.py --> src/ingestion/upload_bronze.py
             |
             v
        S3 bronze (raw)
             |
             v
  src/transformation/silver.py (PySpark)
    clean, dedupe, type — plain Parquet/JSON, not Delta Lake
             |
             v
        S3 silver
             |
             v
  src/models/gates.py -- 6 data-quality gates, driven by
  src/transformation/pipeline.py
             |
        +----+----+
        v         v
      fail       pass
        |         |
        v         v
   SNS quality-   src/transformation/gold.py --> dim_customer (SCD Type 2)
   alert          src/transformation/facts.py --> fact_engagement_daily, dim_offer
        |               |
        v               v
   SQS on-call     src/utils/warehouse.py :: DuckDB (Redshift stand-in)
   queue                |
   (deduped)             v
                    src/serving/api.py :: Flask
                      /cohort  /sla/status  /cost/by-pipeline

  src/orchestration/statemachine.py
    orchestrates the daily run + an SLA timer via Lambdas
    mark_started.py and check_sla_lambda.py
             |
             v
  src/orchestration/cost_sla.py
    tag-based per-pipeline $/day --> DynamoDB cost table

  terraform/azure/: Azure (ADLS + Data Factory) export stub, plan-only
```

See `docs/architecture.md` for the diagram.

## Why this repo has no Java/Go

Deliberately. This is the SQL and dimensional-modeling core of the portfolio. Adding a JVM or Go worker here would be decoration, not a real runtime justification — unlike [`fintech-txn-integrity-pipeline`](../fintech-txn-integrity-pipeline) or [`delivery-eta-mesh`](../delivery-eta-mesh), where the language boundary maps to a real latency/runtime need.

## Measured in this repo

| Metric | Value | How it's measured |
|---|---|---|
| SCD2 backfill reproducibility (full reprocess, hash-compare) | **byte-identical**, verified | `pytest tests/data_quality/test_scd2_backfill.py::test_scd2_backfill_is_byte_identical_on_reprocess` |
| Overlapping validity ranges after reprocess | **0** | `pytest tests/data_quality/test_scd2_backfill.py::test_no_overlapping_validity_ranges_per_customer` |
| Seeded defect classes caught by quality gates | **6/6** | `pytest tests/unit/test_gates.py` (8/8 tests passing) |
| SLA breach alert dedup (2 breaches same day → alerts sent) | **1 sent, 1 deduped** | `pytest tests/integration/test_cost_sla.py::test_sla_breach_sends_one_alert_then_dedups` |
| Gold-layer read time (244 rows, DuckDB over S3 Parquet) | **7 ms** | `make bench` |
| Full test suite | **14/14 passing** | `pytest tests/ -v` |

> Numbers are from a 6-month / 200-customer synthetic run on this machine — `make demo` uses 3 months / 200 customers (learnable); `make demo-full` is the 24-month / 5,000-customer dataset the business framing describes; `make bench` regenerates these.

## Modeled business impact (synthetic data — assumptions documented)

| Assumption | Source | Modeled outcome |
|---|---|---|
| Analyst hours/month lost to unreproducible cohort reports pre-fix | TODO — cite in `docs/impact-model.md` | TODO |

> Figures are a **model**, not measured production results — see `docs/impact-model.md`.

## Emulated vs. real

| Component | Dev (this repo) | Production would use | Fidelity |
|---|---|---|---|
| S3 / SNS / SQS / Lambda / DynamoDB | [MiniStack](https://ministack.org) (free, MIT, no account) | AWS | High |
| AWS CLI v2 | Real `aws` CLI against MiniStack (`AWS_ENDPOINT_URL`) — see `docs/RUNBOOK.md` §2 | AWS CLI v2 | High |
| Glue Data Catalog | MiniStack — `src/transformation/catalog.py` registers `dim_customer`/`fact_engagement_daily`/`dim_offer` with real schema; `create-database`/`create-table`/`get-tables` round-trip correctly | AWS Glue | High — catalog metadata only, verified against the actual Spark-written schema (see `docs/RUNBOOK.md` §5 ex. 4) |
| Athena | MiniStack accepts `start-query-execution` against the Glue-cataloged gold data and reports `SUCCEEDED`, but `get-query-results` returns a hardcoded mock (`{"result": "mock_value"}`), never the real rows | Amazon Athena | **Not used** — DuckDB (`make query`) is the real query layer here; verified live (`docs/RUNBOOK.md` §5 ex. 5) |
| Redshift | **DuckDB**, reading gold Parquet directly from S3 | Redshift Serverless | Medium — no MPP distribution; real `DISTKEY`/`SORTKEY` DDL in `sql/redshift/` |
| Azure (ADLS + Data Factory) | Terraform `validate` only (no Azure credentials in this repo — `plan` requires a real subscription even for new resources, since the `azurerm` provider authenticates before planning) | Real Azure subscription | Illustrative only — proves IaC literacy and correct resource modeling, not a working second cloud |

## Three non-tutorial challenges

1. **SCD Type 2 under backfill**: reprocess 6 months of history without duplicating dimension versions or corrupting `valid_from`/`valid_to`.
2. **Quality gates that block, not just report**: 6 seeded defect classes (nulls, cardinality drift, referential breaks, duplicates, out-of-range outliers, freshness failures) must each be caught before gold promotion.
3. **Cost attribution on shared resources**: three synthetic pipelines share one cluster/bucket — tag-based allocation rules must split cost fairly and auditably.

## Installation

```bash
git clone https://github.com/santiago-ven-ai/banking-engagement-warehouse.git
cd banking-engagement-warehouse
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt   # app deps + lint/type/security tooling
```

## Usage — Demo (3 minutes)

```bash
source env.sh
make demo          # 3 months × 200 customers — learn / iterate (docs/RUNBOOK.md)
make demo-full     # 24 months × 5,000 customers
pytest tests/data_quality/test_scd2_backfill.py
make query
terraform -chdir=terraform/azure init -backend=false && terraform -chdir=terraform/azure validate
```

## Testing

```bash
make test                     # unit + integration + BDD (pytest-bdd), against real MiniStack
make e2e                      # full pipeline, emits benchmarks/quality-report.json
.venv/bin/pre-commit run --all-files   # ruff, mypy, whitespace/EOF checks
```

CI (`.github/workflows/ci.yml`) runs the same suite on every push, plus an isolated `security` job (`pip-audit`) and a coverage gate that fails the build under the threshold on the badge above — measured from a real run, not invented (see `docs/quality-report.md`).

## What this is NOT

Not a dbt tutorial. What makes it engineering: SCD2 backfill correctness, gates that actually block bad data, and cost attributed to a specific pipeline — not just computed globally.

## Build it yourself

See [`docs/RUNBOOK.md`](docs/RUNBOOK.md) to run and understand the flow, or [`docs/BUILD_GUIDE.md`](docs/BUILD_GUIDE.md) to build from scratch.

## Contributing

Solo-maintained portfolio/demo repo — not actively seeking external contributions, but issues and questions are welcome via [GitHub Issues](https://github.com/santiago-ven-ai/banking-engagement-warehouse/issues). See [`CODEOWNERS`](CODEOWNERS) and [`SECURITY.md`](SECURITY.md) for how reports are handled.

## License

[MIT](LICENSE) © santiago-ven-ai
