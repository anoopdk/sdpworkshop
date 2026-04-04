# ecommerce

An end-to-end e-commerce data pipeline built on **Databricks Asset Bundles (DAB)** and **Spark Declarative Pipelines (SDP)**. The project implements a **Medallion Architecture** (Bronze → Silver → Gold) with centralised data-quality expectations.

---

## Project Structure

```
ecommerce/
├── config/                          # Centralised pipeline configuration
│   └── expect/
│       └── silver_expect.py         # Data-quality expectation rules for Silver tables (dictionary-based)
│
├── src/                             # Pipeline source code
│   ├── bronze/                      # Bronze layer – raw ingestion
│   ├── silver/                      # Silver layer – cleansing & enrichment
│   │   └── silver_transformation.py # Silver materialized views & CDC flows
│   └── gold/                        # Gold layer – business aggregations
│
├── resources/                       # Databricks resource definitions
│   ├── jobs/                        # Job YAML configurations
│   └── pipelines/                   # Pipeline YAML configurations
│
├── fixtures/                        # Test/seed data
│   ├── dataSource/
│   ├── setup/
│   └── variables/
│
├── tests/                           # Unit tests (pytest)
├── databricks.yml                   # Bundle manifest
└── pyproject.toml                   # Python project metadata & dependencies
```

---

## Medallion Architecture

| Layer  | Schema  | Description |
|--------|---------|-------------|
| Bronze | `bronze` | Raw files ingested as-is from the volume (`/Volumes/<catalog>/bronze/raw_files`) |
| Silver | `silver` | Cleansed, validated, and enriched tables; invalid rows are dropped or flagged |
| Gold   | `gold`   | Business-level aggregations and reporting views |

---

## Data-Quality Expectations

All **Silver** expectation rules are defined as Python dictionaries in:

```
config/expect/silver_expect.py
```

Each table has its own dictionary with three keys:

| Key | SDP Behaviour |
|-----|---------------|
| `expect` | Record the violation as a warning; keep the row |
| `expect_or_drop` | Drop rows that violate the rule |
| `expect_or_fail` | Fail the entire pipeline run on any violation |

### Example – `SILVER_ORDERS_EXPECTATIONS`

```python
SILVER_ORDERS_EXPECTATIONS = {
    "expect": {
        "valid_order_id": "order_id IS NOT NULL",
        "valid_date":     "order_date IS NOT NULL",
    },
    "expect_or_drop": {
        "positive_quantity": "quantity > 0",
        "positive_price":    "unit_price > 0",
    },
    "expect_or_fail": {},
}
```

`silver_transformation.py` imports these dictionaries and applies them at runtime via the `_apply_expectations` helper, keeping transformation logic free of hard-coded rule strings.

---

## Getting Started

Choose how you want to work on this project:

**(a)** Directly in your Databricks workspace →
    https://docs.databricks.com/dev-tools/bundles/workspace

**(b)** Locally with VS Code or Cursor →
    https://docs.databricks.com/dev-tools/vscode-ext.html

**(c)** With the Databricks CLI →
    https://docs.databricks.com/dev-tools/cli/databricks-cli.html

### Local development setup

Make sure you have the **uv** package manager installed:
https://docs.astral.sh/uv/getting-started/installation/

```bash
uv sync --dev
```

---

## Deploying with the CLI

1. **Authenticate** to your Databricks workspace:
    ```bash
    databricks configure
    ```

2. **Deploy to development** (default target):
    ```bash
    databricks bundle deploy --target dev
    ```
    This deploys all pipelines and jobs defined in `resources/`.  
    Find the deployed pipeline under **Jobs & Pipelines** in your workspace.

3. **Deploy to production**:
    ```bash
    databricks bundle deploy --target prod
    ```
    > The production job runs the pipeline on a daily schedule (see `resources/jobs/`).  
    > The schedule is automatically paused in development mode.

4. **Run a job or pipeline**:
    ```bash
    databricks bundle run
    ```

5. **Run tests locally**:
    ```bash
    uv run pytest
    ```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog_env` | `dev` | Controls which Unity Catalog is targeted (`ecommerce_dev`, `ecommerce_prod`, etc.) |
