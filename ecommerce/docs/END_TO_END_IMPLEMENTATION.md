# Ecommerce Lakehouse - End-to-End Technical Implementation

## 1. Objective

This document explains the full technical implementation of the ecommerce lakehouse pipeline from source generation to bronze, silver, and gold outputs.

It is intended to give a complete architectural picture for engineers onboarding to the project.

For deeper layer-specific details, refer to:
- [BRONZE_LAYER_DESIGN.md](BRONZE_LAYER_DESIGN.md)
- [SILVER_LAYER_DESIGN.md](SILVER_LAYER_DESIGN.md)
- [GOLD_LAYER_DESIGN.md](GOLD_LAYER_DESIGN.md)

---

## 2. High-Level Architecture

The pipeline follows a classic medallion-style design with Databricks Lakeflow Declarative Pipelines:

1. Source data generation
2. Bronze ingestion
3. Silver curation (FAB-5 + SCD2)
4. Gold business marts

Logical flow:

```
Raw Files (orders, customers, cdc_event)
            |
            v
Bronze (streaming ingestion + metadata)
            |
            v
Silver Stage (FAB-5 transformations)
            |
            v
Silver Core (SCD2 stateful tables)
            |
            v
Silver Facts (append_flow)
            |
            v
Gold Marts + PIT Fact
```

---

## 3. Source Data Layer

Source dataset generator notebook:
- [fixtures/dataSource/dataSourceGenerator.ipynb](fixtures/dataSource/dataSourceGenerator.ipynb)

Generated datasets:
1. `orders/`
2. `customers/`
3. `cdc_event/` (product CDC events)

Important data modeling decision:
- IDs are related across datasets:
  - `orders.customer_id` maps to `customers.customer_id`
  - `orders.product_id` maps to `cdc_event.product_id`

This guarantees referential consistency for downstream joins.

---

## 4. Bronze Layer Implementation

Bronze orchestration code:
- [src/notebook/bronze/bronze_orchestration.py](src/notebook/bronze/bronze_orchestration.py)

Bronze detailed design reference:
- [BRONZE_LAYER_DESIGN.md](BRONZE_LAYER_DESIGN.md)

### 4.1 Bronze responsibilities

Bronze is ingestion-focused and intentionally light:
- Read source files continuously with Auto Loader (`cloudFiles`)
- Preserve source structure with minimal transformation
- Add ingestion metadata columns for traceability

### 4.2 Bronze tables

1. `bronze_order`
- Source: `/raw_files/orders/`
- Metadata captured:
  - `_ingest_ts`
  - `_order_index`
  - `_file_mod_time`

2. `bronze_customer`
- Source: `/raw_files/customers/`
- Metadata captured:
  - `_ingest_ts`
  - `_cust_index`
  - `_file_mod_time`

3. `bronze_product`
- Source: `/raw_files/cdc_event/`
- Product CDC feed with operation and event timestamp
- Metadata captured:
  - `_ingest_ts`
  - `_prod_index`
  - `_file_mod_time`

### 4.3 Technical implementation notes

- Auto Loader options include:
  - `cloudFiles.format = csv`
  - `header = true`
  - `cloudFiles.inferColumnTypes = true`
  - explicit `cloudFiles.schemaLocation`
- Correct ingestion order is used:
  - configure read options
  - call `.load(...)`
  - then append metadata with `.withColumn(...)`

This keeps bronze deterministic and replay-friendly.

---

## 5. Silver Layer Implementation

Silver transformation code:
- [src/notebook/silver/silver_transformation.py](src/notebook/silver/silver_transformation.py)

Design reference:
- [SILVER_LAYER_DESIGN.md](SILVER_LAYER_DESIGN.md)

### 5.1 Silver responsibilities

Silver is the semantic contract layer and owns:
- Data cleaning and standardization
- Deduplication
- Late-arrival correctness
- SCD2 state management
- Surrogate key generation
- Derived silver facts

### 5.2 FAB-5 model used

FAB-5 in this project is applied consistently to core entities (orders, customers, products):

1. Clean and Standardize
2. Deduplicate
3. Handle late-arrival updates
4. Keep current + history (SCD2)
5. Add audit metadata and surrogate key

### 5.3 Silver stage tables (FAB-5)

1. `silver_orders_stage`
2. `silver_customers_stage`
3. `silver_products_stage`

These stage tables perform:
- Type normalization
- Text normalization (trim/case)
- Event timestamp derivation (`_event_ts`)
- Window dedup using `row_number`
- Surrogate key computation with `sha2(...)`
- Audit fields (`_ingest_ts`, `_updated_at`, `_inserted_at`, etc.)

### 5.4 Silver SCD2 core tables

Stateful SCD2 targets built with `create_streaming_table` + `create_auto_cdc_flow`:

1. `silver_orders`
2. `silver_customers`
3. `silver_products`

Key SCD2 mechanics:
- Natural keys (`order_id`, `customer_id`, `product_id`)
- Event-time-first sequencing with ingest tie-breaks
- Product CDC delete handling via `apply_as_deletes`

### 5.5 Silver derived facts

Append-only silver facts (immutable analytical records):

1. `silver_order_customer_fact`
2. `silver_order_customer_product_fact`

Built using:
- `create_streaming_table(...)`
- `@sdp.append_flow(...)`

Why append flow for facts:
- Facts represent event timelines, not mutable dimensions
- Reduces complexity and accidental in-place updates
- Aligns with downstream aggregation patterns

### 5.6 Key silver design considerations

During silver design, the most important technical considerations were:

1. Late-arrival correctness
- Must sequence by business event time before ingest time.

2. SCD2 history integrity
- Must avoid false versions caused by dirty raw strings.

3. Surrogate key strategy
- Needed for stable version-level references in silver/gold facts.

4. Compute optimization
- Avoid unnecessary helper MVs where inline current filters are enough.

5. Contract clarity
- Stage tables isolate transformation logic; core tables isolate state logic.

---

## 6. Gold Layer Implementation

Gold aggregation code:
- [src/notebook/gold/gold_aggregation.py](src/notebook/gold/gold_aggregation.py)

Design reference:
- [GOLD_LAYER_DESIGN.md](GOLD_LAYER_DESIGN.md)

### 6.1 Gold responsibilities

Gold exposes business-facing marts for BI and reporting:
- Revenue trends
- Customer behavior and segmentation
- Product performance
- Executive KPI snapshots
- Temporal point-in-time analytical fact

### 6.2 Gold marts

1. `gold_daily_revenue`
- Daily revenue by city and payment method

2. `gold_customer_360`
- Current customer profile + behavioral metrics

3. `gold_top_products`
- Product ranking by demand/revenue

4. `gold_executive_kpis`
- Snapshot KPI set for leadership reporting

5. `gold_order_customer_product_pit_fact`
- Order-line fact with temporal SCD2 joins to customer and product

### 6.3 Point-in-time (PIT) logic in gold

The PIT fact uses SCD2 validity-window joins, not only key joins:

For each dimension:
- key match (e.g., `customer_id`)
- validity condition:
  - `dim.__START_AT <= order_event_ts`
  - `dim.__END_AT IS NULL OR dim.__END_AT > order_event_ts`

This ensures historical attribution is correct for the event timestamp.

---

## 7. End-to-End Data Contracts

### 7.1 Contract boundaries

- Bronze contract: ingestion + metadata
- Silver stage contract: cleaned/deduped/event-ready rows
- Silver core contract: SCD2 versioned entities
- Silver fact contract: append-only analytical events
- Gold contract: business marts and KPI-ready outputs

### 7.2 Why this separation works

- Better debugging: each layer has clear ownership
- Safer changes: business logic changes do not require raw-ingest rewrites
- Reusability: gold can compose from stable silver contracts
- Historical correctness: SCD2 + PIT preserve temporal truth

---

## 8. Operational Considerations

### 8.1 Performance

- Clustered silver SCD2 tables by natural keys
- Use curated silver facts for most gold aggregations
- Use PIT joins only where temporal attribution is required

### 8.2 Data quality

- Silver expectations enforce validity before stateful merges
- Gold expectations guard business-level assumptions

### 8.3 Replay and backfill

- Bronze metadata + silver SCD2 design allow deterministic reprocessing
- PIT gold fact can be recomputed consistently after backfills

---

## 9. Common Failure Modes and Prevention

1. Joining SCD2 tables only by natural key
- Prevention: always include validity-window predicates for temporal models.

2. Mixing current snapshot with historical reporting
- Prevention: use PIT fact for historical attribution use cases.

3. False SCD2 churn due to dirty strings
- Prevention: clean/standardize before CDC merge.

4. Duplicate fact inflation
- Prevention: deduplicate in stage and use distinct counting where needed.

---

## 10. Summary

This project is implemented as a production-oriented, state-aware Databricks pipeline:
- Bronze keeps ingestion simple and traceable.
- Silver applies FAB-5 and SCD2 rigor for correctness.
- Gold delivers business marts, including a temporal PIT fact for accurate historical analytics.

Use this document as the top-level architecture reference, and rely on the layer-specific documents for implementation depth.
