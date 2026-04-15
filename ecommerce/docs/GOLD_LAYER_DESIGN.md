# Gold Layer Design - Ecommerce (Production Reference)

## Purpose

This document defines the production gold-layer strategy for ecommerce analytics built on top of the silver FAB-5 + SCD2 model.

Primary goals:
- Provide business-ready marts with stable semantics.
- Preserve historical truth for dimensions that change over time.
- Support accurate point-in-time analytics using SCD2 temporal joins.
- Keep gold queries maintainable, auditable, and performant.

---

## Gold Assets Overview

Current gold tables in [src/notebook/gold/gold_aggregation.py](src/notebook/gold/gold_aggregation.py):

- `gold_daily_revenue`
- `gold_customer_360`
- `gold_top_products`
- `gold_executive_kpis`
- `gold_order_customer_product_pit_fact`

Each table is a business-facing mart, not a raw processing layer.

---

## Why Point-In-Time (PIT) Joins Matter

### Problem with key-only joins

If you join only on natural keys (for example `customer_id`, `product_id`) while dimensions are SCD2:
- You can match multiple historical versions for the same key.
- You may accidentally match the current version instead of the version valid at event time.
- Historical reports drift over time (restatements become wrong).

This causes analytical errors such as:
- Orders from last year being attributed to the customer's current tier instead of the tier at purchase time.
- Product revenue grouped by the latest category/brand instead of category/brand active when the order happened.

### PIT join principle

A fact event must join to the dimension version that was valid at the fact event timestamp.

For any dimension row:
- valid_from = `__START_AT`
- valid_to = `__END_AT` (null means current/open-ended)

For any event timestamp `t`, the valid version satisfies:

$$
\text{valid_from} \le t \;\;\text{and}\;\;(\text{valid_to is null} \;\text{or}\; \text{valid_to} > t)
$$

This is the core of temporal correctness.

---

## SCD2 PIT Join Pattern Used

Implemented in `gold_order_customer_product_pit_fact` inside [src/notebook/gold/gold_aggregation.py](src/notebook/gold/gold_aggregation.py).

### Fact-side event anchor

Orders are filtered to current order records and relevant statuses:
- source: `silver_orders`
- filter: `__END_AT is null`
- status: completed/shipped
- event anchor: `o._event_ts`

### Customer temporal join condition

- key condition: `o.customer_id = c.customer_id`
- temporal condition:
  - `c.__START_AT <= o._event_ts`
  - `c.__END_AT is null OR c.__END_AT > o._event_ts`

### Product temporal join condition

- key condition: `o.product_id = p.product_id`
- temporal condition:
  - `p.__START_AT <= o._event_ts`
  - `p.__END_AT is null OR p.__END_AT > o._event_ts`

This ensures the selected customer/product row is the version valid at order time.

---

## Why This Approach Is Production-Grade

### 1. Historical accuracy by construction

PIT joins remove ambiguity and preserve event-time truth.

### 2. Reproducibility

Backfills and reruns yield consistent outputs because temporal logic is explicit and deterministic.

### 3. Explainability and auditability

You can explain exactly why a dimension attribute was selected for any given order.

### 4. Safer downstream modeling

Gold marts can be consumed by BI and ML without repeatedly re-implementing temporal predicates.

### 5. Future-proofing

As customer/product attributes evolve, historical metrics remain correct.

---

## Role of Surrogate Keys in Gold

Surrogate keys (`order_sk`, `customer_sk`, `product_sk`) are carried into gold facts.

Benefits:
- Stable identity for specific SCD2 versions.
- Simplifies lineage and reconciliation.
- Enables durable references in downstream marts/feature sets.
- Reduces dependency on repeated temporal joins in every consumer query.

Without surrogate keys, consumers must repeatedly join on natural key + effective ranges, increasing complexity and error risk.

---

## Gold Table Intent and Grain

### `gold_daily_revenue`

- Grain: day x shipping_city x payment_method
- Purpose: dashboard-ready revenue trend mart
- Source: `silver_order_customer_fact`
- Metric safety: `countDistinct(order_id)` to avoid duplicate order inflation

### `gold_customer_360`

- Grain: customer
- Purpose: customer profile + purchase behavior + segmentation
- Current customer profile from `silver_customers` where `__END_AT is null`
- Behavioral metrics from `silver_order_customer_fact`

### `gold_top_products`

- Grain: product
- Purpose: ranking by revenue, quantity, buyers, price signals, stock coverage
- Source: `silver_order_customer_product_fact`

### `gold_executive_kpis`

- Grain: single KPI snapshot row per refresh
- Purpose: executive rollup indicators for business monitoring

### `gold_order_customer_product_pit_fact`

- Grain: order line
- Purpose: canonical temporal fact with customer/product versions valid at order event time
- Core use cases:
  - historical segmentation analysis
  - pricing variance analysis
  - stock-risk impact analysis

---

## Temporal Edge Cases and Handling

### Late-arriving dimension updates

If a dimension change arrives after the order was ingested but has an earlier effective start:
- PIT join still maps based on event time window.
- Historical attribution remains correct after recompute.

### Open-ended current rows

`__END_AT is null` rows represent active versions.
- Temporal predicate safely includes these rows for events after `__START_AT`.

### Missing dimension at event time

Left joins preserve order facts even if no matching dimension version exists.
- Expectations in gold can flag missing dim mappings.

### Multiple overlapping SCD2 intervals (data quality issue)

If overlaps exist for same natural key:
- PIT join may produce duplicates.
- This should be prevented in silver CDC design and validated by DQ checks.

---

## Performance Guidance

### Prefer pre-curated silver facts when PIT is not required

For standard dashboards:
- Use `silver_order_customer_fact` and `silver_order_customer_product_fact`.

For strict historical attribution:
- Use `gold_order_customer_product_pit_fact`.

### Optimize temporal joins

- Ensure silver SCD2 tables are clustered by natural keys.
- Keep event timestamp columns typed as timestamp (no string comparisons).
- Push status/date filters early before joins.

### Distinct counting

Use `countDistinct(order_id)` on aggregated marts to avoid overcount due to line-level records.

---

## Data Quality Expectations in Gold

Current expectations include:
- revenue existence checks
- valid date checks
- PIT fact mapping checks (`has_customer_dim`, `has_product_dim`)

Recommended additions:
- non-negative quantity and price checks in product/order marts
- threshold alerts for missing-dimension ratios in PIT fact
- freshness checks on `_gold_load_ts`

---

## Common Mistakes to Avoid

- Joining SCD2 dims by key only (no time predicate).
- Using current dimension snapshot for historical metrics.
- Comparing timestamps as strings.
- Ignoring null `__END_AT` logic.
- Building many one-off temporal join variants in each report.

---

## Validation Queries (Reference)

### 1. Check PIT join uniqueness at order line level

```sql
SELECT order_id, customer_id, product_id, COUNT(*) AS cnt
FROM ecommerce_dev.gold.gold_order_customer_product_pit_fact
GROUP BY order_id, customer_id, product_id
HAVING COUNT(*) > 1;
```

Expected: 0 rows (unless true multi-line duplicates in source grain).

### 2. Check missing dimension mappings

```sql
SELECT
  SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) AS missing_customer_dim,
  SUM(CASE WHEN product_sk IS NULL THEN 1 ELSE 0 END) AS missing_product_dim,
  COUNT(*) AS total_rows
FROM ecommerce_dev.gold.gold_order_customer_product_pit_fact;
```

Expected: low or zero null mapping ratio.

### 3. Validate historical consistency sample

```sql
SELECT order_id, order_event_ts, customer_id, customer_sk, tier, product_id, product_sk, category
FROM ecommerce_dev.gold.gold_order_customer_product_pit_fact
WHERE order_id IN ('ORD-000101', 'ORD-000102');
```

Expected: tier/category reflect values valid at order event time, not necessarily current values.

---

## Operational Runbook Notes

- Recompute PIT fact after major SCD2 backfills in silver.
- Monitor row counts and missing-dimension ratios each run.
- Keep gold marts business-facing; avoid adding ingestion artifacts unless needed for auditing.
- Document any new derived KPI formulas with business owner sign-off.

---

## Summary

This gold design uses SCD2 point-in-time joins to guarantee temporal correctness and stable historical analytics. The approach is intentionally stricter than key-only joins because it protects business metrics from drift as dimensions evolve over time.

For future work, treat `gold_order_customer_product_pit_fact` as the canonical temporal semantic layer and build additional marts on top of it where historical attribution accuracy is required.
