# Ecommerce Spark Declarative Pipelines

> Zynx Technologies | 3-slide delivery overview

---

## Slide 1: Context & problem

- Zynx x Retail / Ecommerce Client
- Engagement: Implementation Delivery
- The Retail / Ecommerce Client required an end-to-end Databricks pipeline for ecommerce entities with centralised data-quality expectations, using Spark Declarative Pipelines and Asset Bundles for repeatable deployment.
- Engagement levels: Discovery & Source Profiling -> Medallion Design -> SDP Implementation -> DQ Expectations & Handover

---

## Slide 2: Solution architecture & stack

- Zynx implemented a Bronze -> Silver -> Gold ecommerce SDP solution under `ecommerce/`, including config-driven expectations, jobs/pipelines YAML, fixtures, design docs, and test hooks.
- Stack: Databricks Asset Bundles, Spark Declarative Pipelines, Unity Catalog, Python expectations, Medallion
- Architecture documented in docs/03-architecture.md with Mermaid diagrams

---

## Slide 3: Implementation highlights & inspection

- Key evidence: ecommerce/docs/, ecommerce/src/, ecommerce/resources/, ecommerce/databricks.yml
- Open docs/audit/PROJECT-FACTSHEET.md for the audit index
- Repository standardised to Zynx industrial portfolio structure
- Portfolio tag target: 1.0.0-portfolio
