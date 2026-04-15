# Silver Layer Design - Ecommerce (FAB-5 Implementation)

## Architecture Overview

The silver layer is organized into 3 logical layers:

1. Stage Tables (from bronze):
- silver_orders_stage
- silver_customers_stage
- silver_products_stage

2. SCD2 Core Tables (history + current record semantics):
- silver_orders
- silver_customers
- silver_products

3. Derived Silver Facts (built on current views from SCD2):
- silver_order_customer_fact
- silver_order_customer_product_fact

---

## FAB-5 Definition

FAB-5 is the mandatory transformation contract for each core entity (order, customer, product):

- 1. Clean and Standardize
- 2. Deduplicate
- 3. Handle late-arrival updates
- 4. Keep current + history (SCD2)
- 5. Add audit metadata and surrogate key

---

## How FAB-5 Is Implemented

### 1. Clean and Standardize

Goal: remove obvious source noise before downstream logic.

Implementation examples:
- trim and case normalization:
  - order_status, payment_method -> lower(trim(...))
  - customer_name, city, product_name, category -> initcap(trim(...))
  - email -> lower(trim(...))
  - brand -> upper(trim(...))
- normalize operation in product CDC:
  - operation -> upper(trim(operation))
- date/timestamp normalization:
  - order_date -> date
  - signup_date -> date
  - cdc_timestamp -> timestamp
- numeric casts and business-ready columns:
  - quantity -> int
  - unit_price -> double
  - stock_quantity -> int
  - total_amount = quantity * unit_price
  - order_year/order_month from order_date

Why it matters:
- Reduces false change detection in SCD2 caused by only casing/whitespace differences.

### 2. Deduplicate

Goal: keep one canonical record for the same business key at the same event instant.

Implementation pattern:
- row_number over partition by natural key + event timestamp
- keep row_number = 1

Examples:
- orders: partition by (order_id, _event_ts), order by _ingest_ts desc, _order_index desc
- customers: partition by (customer_id, _event_ts), order by _ingest_ts desc, _cust_index desc
- products: partition by (product_id, _event_ts), order by _ingest_ts desc, _prod_index desc

Why it matters:
- Avoids duplicate SCD2 versions from replayed files or duplicate feed rows.

### 3. Late-Arrival Handling

Goal: process out-of-order records correctly.

Implementation:
- explicit event time column _event_ts per entity:
  - orders: _event_ts from order_date
  - customers: _event_ts from signup_date (fallback to _ingest_ts)
  - products: _event_ts from cdc_timestamp
- SCD2 sequencing uses struct(_event_ts, _ingest_ts, optional _*_index)

Why it matters:
- If a record arrives late, it is sequenced by business event time first, then ingest time for tie-break.

### 4. Keep Current + History (SCD2)

Goal: keep one current record and preserve all historical versions.

Implementation:
- `create_auto_cdc_flow(..., stored_as_scd_type=2)` for orders, customers, and products
- natural keys are used for identity:
  - order_id
  - customer_id
  - product_id
- event-time-first sequencing with ingest-time tie-break
- product CDC delete semantics:
  - apply_as_deletes = operation = DELETE

Why it matters:
- Supports point-in-time analysis without losing current-state usability.

### 5. Audit Metadata and Surrogate Key

Goal: enable lineage/debugging and uniquely identify each versioned row.

Audit columns used include:
- _ingest_ts: ingestion timestamp from bronze
- _file_mod_time: source file modification time
- _order_index / _cust_index / _prod_index: source row index where available
- _updated_at: update timestamp used for processing semantics
- _inserted_at: silver insertion timestamp
- _record_number: dedup window rank used to keep canonical record

Surrogate key implementation:
- deterministic hash key at stage level using business key + temporal attributes:
  - order_sk
  - customer_sk
  - product_sk
- built with sha2(concat_ws(...), 256)

Why it matters:
- Natural keys (order_id/customer_id/product_id) identify a business entity, not a specific version.
- SCD2 creates multiple versions for one natural key; each version needs stable row identity.
- Surrogate keys support robust fact-to-dimension joins across time.

---

## SCD2 Strategy (Current + History)

Each entity has an SCD2 target table populated by auto CDC:

- silver_orders from silver_orders_stage
- silver_customers from silver_customers_stage
- silver_products from silver_products_stage

CDC semantics:
- keys = natural business key (order_id, customer_id, product_id)
- sequence_by = event-time-first ordering for late-arrival correctness
- stored_as_scd_type = 2
- product CDC additionally applies deletes:
  - apply_as_deletes = operation = DELETE

Current snapshots are read inline in append flows using:
- __END_AT is null

This gives both:
- full history (SCD2 table)
- current snapshot (current view)

---

## Surrogate Key Deep Dive

### How surrogate keys are used in silver derived facts

- silver_order_customer_fact includes:
  - order_sk, customer_sk
- silver_order_customer_product_fact includes:
  - order_sk, customer_sk, product_sk

These keys allow derived facts to reference exact SCD2 row versions rather than only natural keys.

### Why this matters for gold layer

In gold marts, analysts commonly need:
- point-in-time customer tier at order time
- point-in-time product category/brand/price at order time
- slowly changing hierarchy rollups

Surrogate keys make this straightforward because each fact row can carry stable references to precise dimensional versions.

### What becomes difficult without surrogate keys

Without surrogate keys, downstream models face several problems:

1. Ambiguous joins:
- joining only on natural key may return multiple SCD2 versions.

2. Complex point-in-time logic everywhere:
- every gold query must repeatedly implement effective-date range joins.

3. Historical inconsistency risk:
- easy to accidentally join to current dimension state and lose historical truth.

4. Slower and harder-to-maintain SQL:
- repeated temporal predicates increase query complexity and maintenance burden.

5. Greater reconciliation issues:
- late-arriving updates can produce mismatched historical aggregates if joins are not carefully constrained.

Surrogate keys reduce these issues by making dimensional references explicit and stable.

---

## Derived Facts in Silver

### silver_order_customer_fact

Inputs:
- silver_orders (__END_AT is null)
- silver_customers (__END_AT is null)

Adds:
- order_amount_band
- customer_tenure_days
- is_high_value_order
- _fact_load_id

### silver_order_customer_product_fact

Inputs:
- silver_orders (__END_AT is null)
- silver_customers (__END_AT is null)
- silver_products (__END_AT is null)

Adds:
- line_amount
- product_stock_status
- is_price_mismatch
- _fact_load_id

These tables remain silver-derived facts and can be promoted to gold marts or used as gold inputs.

---

## Operational Notes

1. Bronze remains ingestion-focused:
- source read + metadata capture
- no SCD2 business logic

2. Silver owns stateful semantics:
- dedup
- late-arrival ordering
- SCD2 history
- surrogate key assignment

3. This separation keeps ingestion simple and moves business/version logic to the analytical contract layer.

---

## Next Recommended Step

Add product expectation rules in the silver expectations config and apply them to silver_products_stage to complete parity with order/customer quality controls.
